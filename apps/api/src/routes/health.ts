import { FastifyInstance } from "fastify";
import crypto from "node:crypto";

import { syncFinalizeQueueMetrics } from "../services/uploads/finalize.queue.js";
import { renderPrometheusMetrics } from "../services/metrics/runtime.metrics.js";
import { sendApiError } from "../utils/apiError.js";
import {
  getDependencyHealthReport,
  logDependencyHealthTransitions,
} from "../services/health/dependencies.js";
import { getStartupRecoveryState } from "../services/startup/recovery.js";

function parseBoolEnv(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  if (raw === "1" || raw.toLowerCase() === "true") return true;
  if (raw === "0" || raw.toLowerCase() === "false") return false;
  return fallback;
}

const METRICS_ENABLED = parseBoolEnv("FLOE_ENABLE_METRICS", true);
const METRICS_TOKEN = (process.env.FLOE_METRICS_TOKEN ?? "").trim();

function bearerTokenFromAuthHeader(raw: unknown): string | undefined {
  if (typeof raw === "string") {
    const trimmed = raw.trim();
    const prefix = "bearer ";
    if (trimmed.toLowerCase().startsWith(prefix)) {
      return trimmed.slice(prefix.length).trim() || undefined;
    }
  }
  return undefined;
}

function secureEqual(a: string, b: string): boolean {
  const aBuf = Buffer.from(a);
  const bBuf = Buffer.from(b);
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

async function loadHealthState(app: FastifyInstance) {
  const timestamp = new Date().toISOString();
  const dependency = await getDependencyHealthReport();
  const recovery = getStartupRecoveryState();
  logDependencyHealthTransitions(app.log, dependency);

  const reasons = [...dependency.reasons];
  if (recovery.completed === false) {
    reasons.push(
      recovery.error === null ? "startup_recovery_incomplete" : "startup_recovery_failed"
    );
  }

  const serviceStatus =
    dependency.status === "unavailable"
      ? "DOWN"
      : reasons.length > 0
        ? "DEGRADED"
        : "UP";

  const ready = dependency.ready && recovery.completed;
  const healthStatusCode = dependency.status === "unavailable" ? 503 : 200;
  const readyStatusCode = ready ? 200 : 503;

  return {
    timestamp,
    dependency,
    recovery,
    reasons,
    serviceStatus,
    ready,
    healthStatusCode,
    readyStatusCode,
  };
}

export default async function healthRoute(app: FastifyInstance) {
  app.get("/metrics", async (req, reply) => {
    if (METRICS_ENABLED === false) {
      return sendApiError(reply, 404, "FILE_NOT_FOUND", "Not Found");
    }

    if (METRICS_TOKEN === "") {
      req.log.error("FLOE_METRICS_TOKEN is missing while /metrics is enabled");
      return sendApiError(
        reply,
        503,
        "INTERNAL_ERROR",
        "Metrics auth is not configured",
        { retryable: true }
      );
    }

    const supplied =
      (typeof req.headers["x-metrics-token"] === "string"
        ? req.headers["x-metrics-token"].trim()
        : "") || bearerTokenFromAuthHeader(req.headers.authorization) || "";

    if (supplied === "" || secureEqual(supplied, METRICS_TOKEN) === false) {
      return sendApiError(reply, 401, "UNAUTHORIZED", "Unauthorized");
    }

    await syncFinalizeQueueMetrics().catch((err) => {
      req.log.error({ err }, "Failed to sync finalize queue metrics");
    });

    return reply
      .code(200)
      .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
      .send(renderPrometheusMetrics());
  });

  app.get("/live", async (_req, reply) => {
    return reply.code(200).send({
      status: "UP",
      service: "floe-api-v1",
      live: true,
      timestamp: new Date().toISOString(),
    });
  });

  app.get("/ready", async (_req, reply) => {
    const state = await loadHealthState(app);
    return reply.code(state.readyStatusCode).send({
      status: state.ready ? "READY" : "NOT_READY",
      service: "floe-api-v1",
      ready: state.ready,
      timestamp: state.timestamp,
      reasons: state.reasons,
      startupRecovery: state.recovery,
      checks: {
        redis: state.dependency.checks.redis,
        postgres: state.dependency.checks.postgres,
      },
    });
  });

  app.get("/health", async (_req, reply) => {
    const state = await loadHealthState(app);

    return reply.code(state.healthStatusCode).send({
      status: state.serviceStatus,
      service: "floe-api-v1",
      ready: state.ready,
      timestamp: state.timestamp,
      reasons: state.reasons,
      startupRecovery: state.recovery,
      checks: state.dependency.checks,
    });
  });
}
