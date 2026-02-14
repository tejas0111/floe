// src/services/upload/walrus.read.ts

import { WalrusEnv } from "../../config/walrus.config.js";

const FETCH_TIMEOUT_MS = 60_000;

function normalizeBaseUrl(url: string): string {
  return url.replace(/\/$/, "");
}

export async function fetchWalrusBlob(params: {
  blobId: string;
  rangeHeader?: string;
}): Promise<Response> {
  const aggregator = normalizeBaseUrl(WalrusEnv.aggregatorUrl);
  const url = `${aggregator}/v1/blobs/${encodeURIComponent(params.blobId)}`;

  const headers: Record<string, string> = {};
  if (params.rangeHeader) headers["Range"] = params.rangeHeader;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

  try {
    const res = await fetch(url, {
      method: "GET",
      headers,
      signal: controller.signal,
    });

    return res;
  } finally {
    clearTimeout(timeout);
  }
}
