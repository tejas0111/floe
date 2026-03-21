import type { FastifyBaseLogger } from "fastify";

import { checkPostgresHealth } from "../../state/postgres.js";
import { checkRedisHealth } from "../../state/redis.js";
import { getUploadFinalizeQueueStats } from "../uploads/finalize.queue.js";

export type DependencyStatus = "healthy" | "degraded" | "unavailable";

export type DependencyHealthReport = {
  status: DependencyStatus;
  ready: boolean;
  reasons: string[];
  checks: {
    redis: Awaited<ReturnType<typeof checkRedisHealth>>;
    postgres: Awaited<ReturnType<typeof checkPostgresHealth>>;
    finalizeQueue: {
      depth: number | null;
      pendingUnique: number | null;
      activeLocal: number | null;
      concurrency: number | null;
    };
  };
};

let lastLoggedSummary: string | null = null;

export async function getDependencyHealthReport(): Promise<DependencyHealthReport> {
  const [redis, postgres] = await Promise.all([
    checkRedisHealth(),
    checkPostgresHealth(),
  ]);

  let finalizeQueue: {
    depth: number | null;
    pendingUnique: number | null;
    activeLocal: number | null;
    concurrency: number | null;
  } = {
    depth: null,
    pendingUnique: null,
    activeLocal: null,
    concurrency: null,
  };

  if (redis.state === "healthy") {
    finalizeQueue = await getUploadFinalizeQueueStats().catch(() => finalizeQueue);
  }

  const reasons: string[] = [];
  if (redis.state === "unavailable") {
    reasons.push("redis_unavailable");
  }
  if (postgres.configured === true && postgres.state === "degraded") {
    reasons.push("postgres_degraded");
  }
  if (postgres.configured === true && postgres.enabled === false) {
    reasons.push("postgres_unavailable");
  }

  const status: DependencyStatus =
    redis.state === "unavailable"
      ? "unavailable"
      : reasons.length > 0
        ? "degraded"
        : "healthy";

  const ready = redis.state === "healthy";

  return {
    status,
    ready,
    reasons,
    checks: {
      redis,
      postgres,
      finalizeQueue,
    },
  };
}

export function logDependencyHealthTransitions(
  log: FastifyBaseLogger,
  report: DependencyHealthReport
) {
  const summary = JSON.stringify({
    status: report.status,
    reasons: report.reasons,
    redis: report.checks.redis.state,
    postgres: report.checks.postgres.state,
  });

  if (summary === lastLoggedSummary) return;
  lastLoggedSummary = summary;

  const level = report.status === "healthy" ? "info" : "warn";
  log[level](
    {
      status: report.status,
      reasons: report.reasons,
      redis: report.checks.redis,
      postgres: report.checks.postgres,
    },
    "Dependency health state changed"
  );
}
