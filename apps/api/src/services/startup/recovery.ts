import type { FastifyBaseLogger } from "fastify";

import { reconcileOrphanUploads } from "../../state/gc/upload.gc.reconcile.js";
import { startUploadFinalizeWorker } from "../uploads/finalize.queue.js";

let startupRecoveryStartedAt: number | null = null;
let startupRecoveryCompletedAt: number | null = null;
let startupRecoveryError: string | null = null;
let startupRecoveryDurationMs: number | null = null;

export async function runStartupRecovery(log: FastifyBaseLogger): Promise<void> {
  startupRecoveryStartedAt = Date.now();
  startupRecoveryCompletedAt = null;
  startupRecoveryError = null;
  startupRecoveryDurationMs = null;

  try {
    await reconcileOrphanUploads(log);
    await startUploadFinalizeWorker(log);
    startupRecoveryCompletedAt = Date.now();
    startupRecoveryDurationMs = startupRecoveryCompletedAt - startupRecoveryStartedAt;
    log.info(
      {
        startedAt: startupRecoveryStartedAt,
        completedAt: startupRecoveryCompletedAt,
        durationMs: startupRecoveryDurationMs,
      },
      "Startup recovery completed"
    );
  } catch (err) {
    startupRecoveryError = String((err as Error)?.message ?? err ?? "unknown");
    startupRecoveryCompletedAt = Date.now();
    startupRecoveryDurationMs = startupRecoveryCompletedAt - startupRecoveryStartedAt;
    log.error(
      {
        err,
        startedAt: startupRecoveryStartedAt,
        completedAt: startupRecoveryCompletedAt,
        durationMs: startupRecoveryDurationMs,
      },
      "Startup recovery failed"
    );
    throw err;
  }
}

export function getStartupRecoveryState() {
  return {
    startedAt: startupRecoveryStartedAt,
    completedAt: startupRecoveryCompletedAt,
    durationMs: startupRecoveryDurationMs,
    error: startupRecoveryError,
    completed:
      startupRecoveryCompletedAt === null
        ? false
        : startupRecoveryError === null,
  };
}
