// apps/api/src/config/walrus.config.ts

if (!process.env.WALRUS_PUBLISHER_URL) {
  throw new Error("Missing required env: WALRUS_PUBLISHER_URL");
}

if (!process.env.WALRUS_AGGREGATOR_URL) {
  throw new Error("Missing required env: WALRUS_AGGREGATOR_URL");
}

export const WalrusEnv = {
  publisherUrl: process.env.WALRUS_PUBLISHER_URL,
  aggregatorUrl: process.env.WALRUS_AGGREGATOR_URL,
};

export const WalrusReadLimits = {
  timeoutMs: Number(process.env.WALRUS_READ_TIMEOUT_MS ?? 60_000),
  maxRangeBytes: Number(process.env.FLOE_STREAM_MAX_RANGE_BYTES ?? 16 * 1024 * 1024),
};

export const WalrusEpochLimits = {
  min: 1,
  max: 90,
  default: 3,
} as const;

export const WalrusUploadLimits = {
  maxRetries: 3,
  baseRetryDelayMs: 2000,
};

export const WalrusQueueLimits = {
  /**
   * Max concurrent Walrus publish requests.
   */
  concurrency: 3,

  /**
   * Max jobs per interval window.
   */
  intervalCap: 1,

  /**
   * Interval window in ms.
   */
  intervalMs: 1500,
};
