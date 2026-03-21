import { Redis } from "@upstash/redis";

let redis: Redis | null = null;
let lastRedisError: string | null = null;
let lastRedisCheckedAt: number | null = null;

export type RedisHealthSnapshot = {
  configured: boolean;
  state: "healthy" | "unavailable";
  latencyMs: number | null;
  checkedAt: number | null;
  error: string | null;
};

function markRedisHealthy(checkedAt: number) {
  lastRedisCheckedAt = checkedAt;
  lastRedisError = null;
}

function markRedisUnavailable(error: unknown, checkedAt: number) {
  lastRedisCheckedAt = checkedAt;
  lastRedisError = String((error as Error)?.message ?? error ?? "unknown");
}

export async function initRedis(): Promise<Redis> {
  if (redis) return redis;

  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;

  if (Boolean(url) === false || Boolean(token) === false) {
    throw new Error(
      "Upstash Redis env vars missing (UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN)"
    );
  }

  const client = new Redis({
    url,
    token,
    retry: {
      retries: 3,
      backoff: (attempt) => Math.min(100 * 2 ** attempt, 1000),
    },
  });

  const checkedAt = Date.now();
  try {
    await client.ping();
    markRedisHealthy(checkedAt);
  } catch (err) {
    markRedisUnavailable(err, checkedAt);
    throw err;
  }

  redis = client;
  return redis;
}

export function getRedis(): Redis {
  if (redis === null) {
    throw new Error(
      "Redis not initialized. initRedis() must be awaited during startup."
    );
  }
  return redis;
}

export async function checkRedisHealth(): Promise<RedisHealthSnapshot> {
  const checkedAt = Date.now();
  if (redis === null) {
    return {
      configured: true,
      state: "unavailable",
      latencyMs: null,
      checkedAt,
      error: "Redis not initialized",
    };
  }

  const startedAt = Date.now();
  try {
    await redis.ping();
    markRedisHealthy(checkedAt);
    return {
      configured: true,
      state: "healthy",
      latencyMs: Date.now() - startedAt,
      checkedAt,
      error: null,
    };
  } catch (err) {
    markRedisUnavailable(err, checkedAt);
    return {
      configured: true,
      state: "unavailable",
      latencyMs: Date.now() - startedAt,
      checkedAt,
      error: lastRedisError,
    };
  }
}

export function getRedisHealthSnapshot(): RedisHealthSnapshot {
  return {
    configured: true,
    state:
      redis === null
        ? "unavailable"
        : lastRedisError === null
          ? "healthy"
          : "unavailable",
    latencyMs: null,
    checkedAt: lastRedisCheckedAt,
    error: lastRedisError,
  };
}

export function isRedisUnavailableError(err: unknown): boolean {
  const message = String((err as Error)?.message ?? err ?? "").toUpperCase();
  return (
    message.includes("REDIS NOT INITIALIZED") ||
    message.includes("UPSTASH") ||
    message.includes("REDIS_TRANSACTION_FAILED") ||
    message.includes("NETWORK") ||
    message.includes("REQUEST FAILED") ||
    message.includes("ECONN") ||
    message.includes("ETIMEDOUT") ||
    message.includes("ENOTFOUND")
  );
}
