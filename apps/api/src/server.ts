// src/server.ts

import Fastify from "fastify";
import multipart from "@fastify/multipart";

import uploadRoutes from "./routes/uploads.routes.js";
import healthRoute from "./routes/health.js";
import { filesRoutes } from "./routes/files.routes.js";
import { initRedis } from "./state/client.js";
import { startUploadGc, stopUploadGc } from "./state/gc/upload.gc.scheduler.js";
import { reconcileOrphanUploads } from "./state/gc/upload.gc.reconcile.js";
import { ChunkConfig } from "./config/uploads.config.js";

process.on("unhandledRejection", (reason) => {
  console.error("Unhandled promise rejection:", reason);
});

process.on("uncaughtException", (err) => {
  console.error("Uncaught exception:", err);
  process.exit(1);
});

const app = Fastify({
  logger: {
    level: process.env.NODE_ENV === "production" ? "info" : "debug",
    redact: {
      paths: ["req.headers.authorization"],
      remove: true,
    },
  },
  // Requests should be chunk-sized (multipart) or small JSON.
  bodyLimit: ChunkConfig.maxBytes + 1024 * 1024,
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
  app.log.info("Redis initialized");
} catch (err) {
  app.log.error(err, "Failed to initialize Redis");
  process.exit(1);
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

  req.log.error(
    { err, url: req.url, method: req.method, requestId: req.id },
    "Request error"
  );

  return reply.code(statusCode).send({
    error: {
      code: statusCode < 500 ? "REQUEST_ERROR" : "INTERNAL_ERROR",
      message:
        statusCode < 500 && err instanceof Error
          ? err.message
          : "Unexpected server error",

      retryable: false,
    },
  });
});

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
