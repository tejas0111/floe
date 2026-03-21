import type { FastifyBaseLogger } from "fastify";

type PgPool = {
  query: (sql: string, values?: unknown[]) => Promise<{ rows: any[] }>;
  end: () => Promise<void>;
};

export type PostgresHealthSnapshot = {
  configured: boolean;
  enabled: boolean;
  state: "healthy" | "degraded" | "unavailable";
  latencyMs: number | null;
  checkedAt: number | null;
  error: string | null;
};

let pool: PgPool | null = null;
let enabled = false;
let lastPostgresError: string | null = null;
let lastPostgresCheckedAt: number | null = null;

function parseBoolEnv(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  if (raw === "1" || raw.toLowerCase() === "true") return true;
  if (raw === "0" || raw.toLowerCase() === "false") return false;
  throw new Error(name + " must be one of: 1, 0, true, false");
}

function parsePositiveIntEnv(name: string, fallback: number, min = 1): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  const n = Number(raw);
  if (Number.isInteger(n) === false || n < min) {
    throw new Error(name + " must be an integer >= " + min);
  }
  return n;
}

function databaseUrl(): string {
  return (process.env.DATABASE_URL ?? "").trim();
}

function markPostgresHealthy(checkedAt: number) {
  lastPostgresCheckedAt = checkedAt;
  lastPostgresError = null;
}

export function notePostgresFailure(error: unknown) {
  lastPostgresCheckedAt = Date.now();
  lastPostgresError = String((error as Error)?.message ?? error ?? "unknown");
}

export function isPostgresConfigured(): boolean {
  return databaseUrl().length > 0;
}

export function isPostgresEnabled(): boolean {
  return enabled && pool !== null;
}

function isPostgresRequired(): boolean {
  return parseBoolEnv("FLOE_POSTGRES_REQUIRED", false);
}

async function loadPgPool(connectionString: string): Promise<PgPool> {
  const dynamicImport = new Function("s", "return import(s)") as (
    specifier: string
  ) => Promise<any>;
  const pg = await dynamicImport("pg").catch(() => null);
  if (pg === null) {
    throw new Error("Postgres driver not found. Run: npm install pg --workspace=apps/api");
  }

  const PoolCtor = pg.Pool ?? pg.default?.Pool;
  if (PoolCtor === undefined || PoolCtor === null) {
    throw new Error("Invalid pg module: Pool constructor not found");
  }

  const max = parsePositiveIntEnv("FLOE_POSTGRES_POOL_MAX", 10);
  const idleTimeoutMillis = parsePositiveIntEnv("FLOE_POSTGRES_IDLE_TIMEOUT_MS", 30000, 1000);
  const connectionTimeoutMillis = parsePositiveIntEnv(
    "FLOE_POSTGRES_CONNECT_TIMEOUT_MS",
    10000,
    1000
  );

  return new PoolCtor({
    connectionString,
    max,
    idleTimeoutMillis,
    connectionTimeoutMillis,
  }) as PgPool;
}

export async function initPostgres(log?: FastifyBaseLogger): Promise<PgPool | null> {
  if (pool && enabled) return pool;
  const url = databaseUrl();
  if (url.length === 0) {
    enabled = false;
    pool = null;
    lastPostgresCheckedAt = Date.now();
    lastPostgresError = null;
    return null;
  }

  try {
    const client = await loadPgPool(url);
    await client.query("select 1");
    pool = client;
    enabled = true;
    markPostgresHealthy(Date.now());
    log?.info("Postgres initialized");
    return pool;
  } catch (err) {
    pool = null;
    enabled = false;
    notePostgresFailure(err);
    if (isPostgresRequired()) {
      throw err;
    }
    log?.warn({ err }, "Postgres unavailable; continuing without read-model index");
    return null;
  }
}

export function getPostgres(): PgPool | null {
  if (enabled === false || pool === null) return null;
  return pool;
}

export async function closePostgres(): Promise<void> {
  if (pool === null) return;
  await pool.end().catch(() => {});
  pool = null;
  enabled = false;
}

export async function checkPostgresHealth(): Promise<PostgresHealthSnapshot> {
  if (isPostgresConfigured() === false) {
    return {
      configured: false,
      enabled: false,
      state: "unavailable",
      latencyMs: null,
      checkedAt: lastPostgresCheckedAt,
      error: null,
    };
  }

  if (enabled === false || pool === null) {
    return {
      configured: true,
      enabled: false,
      state: "unavailable",
      latencyMs: null,
      checkedAt: lastPostgresCheckedAt,
      error: lastPostgresError,
    };
  }

  const start = Date.now();
  try {
    await pool.query("select 1");
    markPostgresHealthy(Date.now());
    return {
      configured: true,
      enabled: true,
      state: "healthy",
      latencyMs: Date.now() - start,
      checkedAt: lastPostgresCheckedAt,
      error: null,
    };
  } catch (err) {
    notePostgresFailure(err);
    return {
      configured: true,
      enabled: true,
      state: "degraded",
      latencyMs: Date.now() - start,
      checkedAt: lastPostgresCheckedAt,
      error: lastPostgresError,
    };
  }
}

export function isPostgresUnavailableError(err: unknown): boolean {
  const message = String((err as Error)?.message ?? err ?? "").toUpperCase();
  return (
    message.includes("POSTGRES") ||
    message.includes("ECONN") ||
    message.includes("ETIMEDOUT") ||
    message.includes("ENOTFOUND") ||
    message.includes("CONNECTION TERMINATED") ||
    message.includes("CONNECTION REFUSED")
  );
}
