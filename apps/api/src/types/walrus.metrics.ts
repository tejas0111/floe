// src/types/walrus.metrics.ts

export type WalrusUploadOutcome =
  | "success"
  | "auth_failed"
  | "insufficient_balance"
  | "network_error"
  | "server_error"
  | "invalid_response"
  | "queue_timeout"
  | "unknown_error";

export interface WalrusUploadMetric {
  uploadId: string;
  sizeBytes: number;
  epochs: number;
  attempt: number;
  durationMs: number;
  outcome: WalrusUploadOutcome;
  blobId?: string;
  error?: string;
  network: "mainnet" | "testnet";
  timestamp: number;
}

export function recordWalrusUploadMetric(
  metric: WalrusUploadMetric
) {
  console.info("[walrus.upload.metric]", metric);
}

export function classifyWalrusError(
  err: Error
): WalrusUploadOutcome {
  const msg = err.message.toUpperCase();

  if (msg.includes("AUTH")) return "auth_failed";
  if (msg.includes("INSUFFICIENT")) return "insufficient_balance";
  if (msg.includes("ECONN") || msg.includes("NETWORK") || msg.includes("FETCH"))
    return "network_error";
  if (msg.includes("SERVER")) return "server_error";
  if (msg.includes("INVALID")) return "invalid_response";

  return "unknown_error";
}

