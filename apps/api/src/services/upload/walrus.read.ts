// src/services/upload/walrus.read.ts

import { WalrusEnv } from "../../config/walrus.config.js";

const FETCH_TIMEOUT_MS = 60_000;

function normalizeBaseUrl(url: string): string {
  return url.replace(/\/$/, "");
}

export async function fetchWalrusBlob(params: {
  blobId: string;
  rangeHeader?: string;
  signal?: AbortSignal;
}): Promise<Response> {
  const aggregator = normalizeBaseUrl(WalrusEnv.aggregatorUrl);
  const url = `${aggregator}/v1/blobs/${encodeURIComponent(params.blobId)}`;

  const headers: Record<string, string> = {};
  if (params.rangeHeader) headers["Range"] = params.rangeHeader;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

  const onAbort = () => controller.abort();
  if (params.signal) {
    if (params.signal.aborted) {
      controller.abort();
    } else {
      params.signal.addEventListener("abort", onAbort, { once: true });
    }
  }

  try {
    const res = await fetch(url, {
      method: "GET",
      headers,
      signal: controller.signal,
    });

    return res;
  } finally {
    clearTimeout(timeout);
    if (params.signal) {
      try {
        params.signal.removeEventListener("abort", onAbort);
      } catch {}
    }
  }
}
