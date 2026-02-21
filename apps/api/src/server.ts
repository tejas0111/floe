// src/server.ts

import Fastify from "fastify";
import multipart from "@fastify/multipart";
import cors from "@fastify/cors";
import fs from "fs/promises";
import os from "os";
import path from "path";

import uploadRoutes from "./routes/uploads.routes.js";
import healthRoute from "./routes/health.js";
import { filesRoutes } from "./routes/files.routes.js";
import { initRedis } from "./state/client.js";
import { startUploadGc, stopUploadGc } from "./state/gc/upload.gc.scheduler.js";
import { reconcileOrphanUploads } from "./state/gc/upload.gc.reconcile.js";
import { ChunkConfig, UploadConfig } from "./config/uploads.config.js";
import {
  createDefaultAuthProvider,
  type AuthProvider,
} from "./services/auth/auth.provider.js";
import {
  AuthOwnerPolicyConfig,
  AuthRateLimitConfig,
  AuthUploadPolicyConfig,
} from "./config/auth.config.js";

process.on("unhandledRejection", (reason) => {
  console.error("Unhandled promise rejection:", reason);
});

process.on("uncaughtException", (err) => {
  console.error("Uncaught exception:", err);
  process.exit(1);
});

function createFastifyApp() {
  return Fastify({
    logger: {
      level: process.env.NODE_ENV === "production" ? "info" : "debug",
      redact: {
        paths: ["req.headers.authorization", "req.headers.x-api-key"],
        remove: true,
      },
    },
    // Requests should be chunk-sized (multipart) or small JSON.
    bodyLimit: ChunkConfig.maxBytes + 1024 * 1024,
  });
}

function parseCorsOrigins(): string[] {
  const raw = process.env.FLOE_CORS_ORIGINS;
  if (!raw) return [];
  return raw
    .split(",")
    .map((v) => v.trim())
    .filter(Boolean);
}

async function validateUploadTmpDir() {
  const dir = UploadConfig.tmpDir;
  const home = os.homedir();

  if (!path.isAbsolute(dir)) {
    throw new Error("UPLOAD_TMP_DIR must be an absolute path");
  }
  if (dir === "/" || dir === "/home" || dir === home) {
    throw new Error(`UPLOAD_TMP_DIR is unsafe: ${dir}`);
  }

  await fs.mkdir(dir, { recursive: true });

  // Verify we can write to the directory. This prevents starting with a
  // misconfigured path that will later fail during uploads/GC/cancel.
  const probe = path.join(dir, `.floe_write_test_${process.pid}_${Date.now()}`);
  await fs.writeFile(probe, "ok");
  await fs.unlink(probe);
}

export async function createApiServer(params?: { authProvider?: AuthProvider }) {
  const app = createFastifyApp();
  const corsOrigins = parseCorsOrigins();

  app.decorate("authProvider", params?.authProvider ?? createDefaultAuthProvider());
  app.addHook("onRequest", async (req, reply) => {
    reply.header("x-request-id", req.id);
  });

  await app.register(cors, {
    origin:
      corsOrigins.length === 0
        ? false
        : (origin, cb) => {
            if (!origin) {
              cb(null, true);
              return;
            }
            cb(null, corsOrigins.includes(origin));
          },
    methods: ["GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS"],
    allowedHeaders: [
      "content-type",
      "authorization",
      "x-api-key",
      "x-wallet-address",
      "x-owner-address",
      "x-auth-user",
      "x-chunk-sha256",
    ],
    exposedHeaders: ["x-request-id", "x-ratelimit-limit", "x-ratelimit-remaining", "x-ratelimit-window", "retry-after"],
    maxAge: 600,
  });

  await app.register(multipart, {
    attachFieldsToBody: false,
    throwFileSizeLimit: false,
    limits: {
      // One chunk per request.
      fileSize: ChunkConfig.maxBytes,
      files: 1,
    },
  });

  try {
    await initRedis();
    await validateUploadTmpDir();
    app.log.info(
      {
        limits: {
          uploadControl: AuthRateLimitConfig.limits.upload_control,
          uploadChunk: AuthRateLimitConfig.limits.upload_chunk,
          fileRead: AuthRateLimitConfig.limits.file_read,
          uploadMaxFileSizeBytes: UploadConfig.maxFileSizeBytes,
          publicMaxFileSizeBytes: AuthUploadPolicyConfig.maxFileSizeBytes.public,
          authMaxFileSizeBytes: AuthUploadPolicyConfig.maxFileSizeBytes.authenticated,
          enforceUploadOwner: AuthOwnerPolicyConfig.enforceUploadOwner,
        },
      },
      "Redis initialized and config loaded"
    );
  } catch (err) {
    app.log.error(err, "Failed to initialize Redis");
    throw err;
  }

  await reconcileOrphanUploads(app.log);
  startUploadGc(app.log);

  await app.register(uploadRoutes);
  await app.register(filesRoutes);
  await app.register(healthRoute);

  app.setErrorHandler((err, req, reply) => {
    const statusCode =
      (err as any)?.statusCode && Number.isInteger((err as any).statusCode)
        ? (err as any).statusCode
        : 500;
    const knownCodeByMessage: Record<string, string> = {
      FILE_BLOB_UNAVAILABLE: "FILE_BLOB_UNAVAILABLE",
      FILE_CONTENT_NOT_FOUND: "FILE_BLOB_UNAVAILABLE",
    };
    const knownCode = err instanceof Error ? knownCodeByMessage[err.message] : undefined;

    req.log.error(
      { err, url: req.url, method: req.method, requestId: req.id },
      "Request error"
    );

    return reply.code(statusCode).send({
      error: {
        code: knownCode ?? (statusCode < 500 ? "REQUEST_ERROR" : "INTERNAL_ERROR"),
        message:
          statusCode < 500 && err instanceof Error
            ? err.message
            : "Unexpected server error",

        retryable: false,
      },
    });
  });

  return app;
}

async function start() {
  const app = await createApiServer();
  const PORT = Number(process.env.PORT ?? 3000);

  try {
    await app.listen({
      port: PORT,
      host: "0.0.0.0",
    });

    app.log.info(
      { port: PORT, env: process.env.NODE_ENV ?? "development" },
      "API server started"
    );
  } catch (err) {
    app.log.error(err, "Failed to start server");
    process.exit(1);
  }

  async function shutdown(signal: string) {
    app.log.info({ signal }, "Shutting down server");

    try {
      await stopUploadGc();
      await app.close();
      process.exit(0);
    } catch (err) {
      app.log.error(err, "Shutdown failed");
      process.exit(1);
    }
  }

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
}

await start();
