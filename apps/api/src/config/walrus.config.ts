// apps/api/src/config/walrus.config.ts

function parseUrlList(raw: string | undefined): string[] {
  if (!raw) return [];
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function assertHttpUrl(name: string, url: string) {
  if (!/^https?:\/\//.test(url)) {
    throw new Error(`${name} must start with http:// or https://`);
  }
}

function parsePositiveIntEnv(name: string, fallback: number, min = 1): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < min) {
    throw new Error(`${name} must be an integer >= ${min}`);
  }
  return n;
}

if (!process.env.WALRUS_PUBLISHER_URL) {
  throw new Error("Missing required env: WALRUS_PUBLISHER_URL");
}

if (!process.env.WALRUS_AGGREGATOR_URL) {
  throw new Error("Missing required env: WALRUS_AGGREGATOR_URL");
}

const publisherUrl = process.env.WALRUS_PUBLISHER_URL;
const primaryAggregator = process.env.WALRUS_AGGREGATOR_URL;
const fallbackAggregators = parseUrlList(
  process.env.WALRUS_AGGREGATOR_FALLBACK_URLS
);

assertHttpUrl("WALRUS_PUBLISHER_URL", publisherUrl);
assertHttpUrl("WALRUS_AGGREGATOR_URL", primaryAggregator);
for (const u of fallbackAggregators) assertHttpUrl("WALRUS_AGGREGATOR_FALLBACK_URLS", u);

export const WalrusEnv = {
  publisherUrl,

  // Ordered list. Reader will try primary first, then fallbacks.
  aggregatorUrls: [primaryAggregator, ...fallbackAggregators],
};

export const WalrusReadLimits = {
  timeoutMs: parsePositiveIntEnv("WALRUS_READ_TIMEOUT_MS", 10 * 60_000),

  // Max size for a single upstream Walrus Range request. Used to stitch large
  // reads into bounded segments so public aggregators can serve big files.
  maxRangeBytes: parsePositiveIntEnv("FLOE_STREAM_MAX_RANGE_BYTES", 64 * 1024 * 1024),

  // Retry budget per stitched segment (network errors, 5xx, 429).
  maxSegmentRetries: parsePositiveIntEnv("WALRUS_READ_MAX_RETRIES", 2),
  baseRetryDelayMs: parsePositiveIntEnv("WALRUS_READ_RETRY_DELAY_MS", 250),
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
