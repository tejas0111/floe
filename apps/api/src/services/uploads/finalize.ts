import fs from "fs/promises";
import path from "path";
import { Readable } from "stream";
import crypto from "crypto";
import type { FastifyBaseLogger } from "fastify";

import { UploadConfig } from "../../config/uploads.config.js";
import { InternalSession } from "./session.js";
import { getRedis } from "../../state/redis.js";
import { uploadKeys } from "../../state/keys.js";
import { chunkStore } from "../../store/index.js";
import { uploadToWalrusWithMetrics } from "../walrus/metrics.js";
import { finalizeFileMetadata } from "../../sui/file.metadata.js";
import {
  observeFinalizeStage,
  observeSuiFinalize,
} from "../metrics/runtime.metrics.js";
import { upsertIndexedFile } from "../../db/files.repository.js";

const finalFilePath = (uploadId: string) =>
  path.join(UploadConfig.tmpDir, `${uploadId}.bin`);

const FINALIZE_LOCK_TTL_SECONDS = 15 * 60;
const FINALIZE_LOCK_REFRESH_INTERVAL_MS = 60_000;

type FinalizeStage =
  | "verify_chunks"
  | "walrus_publish"
  | "sui_finalize"
  | "redis_commit"
  | "cleanup";

type FinalizeStageDurations = Record<FinalizeStage, number>;

type FinalizeContext = {
  log?: FastifyBaseLogger;
  attempt?: number;
  queueWaitMs?: number;
};

type FinalizeFailureCode =
  | "lock_in_progress"
  | "lock_lost"
  | "upload_not_found"
  | "incomplete_chunks"
  | "missing_chunks"
  | "walrus_upload_failed"
  | "walrus_unavailable"
  | "walrus_unknown"
  | "sui_file_create_failed"
  | "sui_unavailable"
  | "redis_failure"
  | "corrupt_completed_upload"
  | "finalize_failed";

function normalizeFinalizeFailure(err: unknown): {
  reasonCode: FinalizeFailureCode;
  retryable: boolean;
} {
  const message = String((err as Error)?.message ?? "UPLOAD_FINALIZE_FAILED").toUpperCase();

  if (message === "UPLOAD_FINALIZATION_IN_PROGRESS") {
    return { reasonCode: "lock_in_progress", retryable: true };
  }
  if (message === "UPLOAD_FINALIZATION_LOCK_LOST") {
    return { reasonCode: "lock_lost", retryable: true };
  }
  if (message === "UPLOAD_NOT_FOUND") {
    return { reasonCode: "upload_not_found", retryable: false };
  }
  if (message === "INCOMPLETE_CHUNKS") {
    return { reasonCode: "incomplete_chunks", retryable: false };
  }
  if (message === "MISSING_CHUNKS") {
    return { reasonCode: "missing_chunks", retryable: false };
  }
  if (message.includes("WALRUS_UPLOAD_FAILED")) {
    return { reasonCode: "walrus_upload_failed", retryable: true };
  }
  if (message.includes("WALRUS")) {
    return { reasonCode: "walrus_unavailable", retryable: true };
  }
  if (message === "SUI_FILE_CREATE_FAILED") {
    return { reasonCode: "sui_file_create_failed", retryable: false };
  }
  if (message.includes("SUI")) {
    return { reasonCode: "sui_unavailable", retryable: true };
  }
  if (message.includes("REDIS")) {
    return { reasonCode: "redis_failure", retryable: true };
  }
  if (message === "CORRUPT_COMPLETED_UPLOAD") {
    return { reasonCode: "corrupt_completed_upload", retryable: false };
  }

  return { reasonCode: "finalize_failed", retryable: false };
}

function attachFinalizeStage(err: unknown, stage: FinalizeStage): Error {
  const wrapped = err instanceof Error ? err : new Error(String(err));
  (wrapped as Error & { finalizeStage?: FinalizeStage }).finalizeStage = stage;
  return wrapped;
}

async function refreshFinalizeLockAtomic(params: {
  lockKey: string;
  lockToken: string;
  ttlSeconds: number;
}): Promise<boolean> {
  const redis = getRedis();
  const script = `
    if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("EXPIRE", KEYS[1], tonumber(ARGV[2]))
    end
    return 0
  `;
  const res = await redis.eval(
    script,
    [params.lockKey],
    [params.lockToken, String(params.ttlSeconds)]
  );
  return Number(res) === 1;
}

async function releaseFinalizeLockAtomic(params: {
  lockKey: string;
  lockToken: string;
}): Promise<boolean> {
  const redis = getRedis();
  const script = `
    if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("DEL", KEYS[1])
    end
    return 0
  `;
  const res = await redis.eval(script, [params.lockKey], [params.lockToken]);
  return Number(res) === 1;
}

function createChunkAssemblyStream(params: {
  uploadId: string;
  totalChunks: number;
}): Readable {
  const iter = async function* () {
    for (let i = 0; i < params.totalChunks; i++) {
      const rs = chunkStore.openChunk(params.uploadId, i);
      for await (const buf of rs) {
        yield buf as Uint8Array;
      }
    }
  };
  return Readable.from(iter());
}

export async function finalizeUpload(
  session: InternalSession,
  context: FinalizeContext = {}
): Promise<{
  fileId: string;
  blobId: string;
  sizeBytes: number;
  status: "ready";
  walrusEndEpoch?: number;
  finalize: {
    totalMs: number;
    stageDurationsMs: FinalizeStageDurations;
  };
}> {
  const redis = getRedis();
  const uploadId = session.uploadId;
  const metaKey = uploadKeys.meta(uploadId);
  const startedAt = Date.now();
  const stageDurationsMs: FinalizeStageDurations = {
    verify_chunks: 0,
    walrus_publish: 0,
    sui_finalize: 0,
    redis_commit: 0,
    cleanup: 0,
  };
  let currentStage: FinalizeStage | null = null;
  const setFinalizeStage = async (stage: FinalizeStage) => {
    currentStage = stage;
    await redis.hset(metaKey, {
      finalizeStage: stage,
      finalizeStageStartedAt: String(Date.now()),
      finalizeLastProgressAt: String(Date.now()),
    });
  };

  const runStage = async <T>(stage: FinalizeStage, fn: () => Promise<T>): Promise<T> => {
    await setFinalizeStage(stage);
    const stageStartedAt = Date.now();

    try {
      const result = await fn();
      const durationMs = Date.now() - stageStartedAt;
      stageDurationsMs[stage] = durationMs;
      observeFinalizeStage({ stage, outcome: "success", durationMs });
      await redis.hset(metaKey, {
        finalizeLastSuccessfulStage: stage,
        finalizeLastProgressAt: String(Date.now()),
      });
      return result;
    } catch (err) {
      const durationMs = Date.now() - stageStartedAt;
      stageDurationsMs[stage] = durationMs;
      observeFinalizeStage({ stage, outcome: "failure", durationMs });
      throw attachFinalizeStage(err, stage);
    }
  };

  // Fast-path idempotency.
  const pre = await redis.hgetall<Record<string, string>>(metaKey);
  if (pre?.status === "completed") {
    if (!pre.fileId || !pre.blobId) {
      throw new Error("CORRUPT_COMPLETED_UPLOAD");
    }

    return {
      fileId: pre.fileId,
      blobId: pre.blobId,
      sizeBytes: Number(pre.sizeBytes ?? session.sizeBytes),
      status: "ready",
      ...(pre.walrusEndEpoch ? { walrusEndEpoch: Number(pre.walrusEndEpoch) } : {}),
      finalize: {
        totalMs: Number(pre.finalizeTotalMs ?? 0),
        stageDurationsMs: {
          verify_chunks: Number(pre.finalizeVerifyMs ?? 0),
          walrus_publish: Number(pre.finalizeWalrusMs ?? 0),
          sui_finalize: Number(pre.finalizeSuiMs ?? 0),
          redis_commit: Number(pre.finalizeRedisMs ?? 0),
          cleanup: Number(pre.finalizeCleanupMs ?? 0),
        },
      },
    };
  }

  const lockKey = `${metaKey}:lock`;
  const lockToken = crypto.randomUUID();
  const locked = await redis.set(lockKey, lockToken, {
    nx: true,
    ex: FINALIZE_LOCK_TTL_SECONDS,
  });

  if (!locked) {
    throw new Error("UPLOAD_FINALIZATION_IN_PROGRESS");
  }

  const refreshFinalizeLock = async () => {
    const refreshed = await refreshFinalizeLockAtomic({
      lockKey,
      lockToken,
      ttlSeconds: FINALIZE_LOCK_TTL_SECONDS,
    });
    if (!refreshed) {
      throw new Error("UPLOAD_FINALIZATION_LOCK_LOST");
    }
  };

  let lockError: Error | null = null;
  const lockRefreshTimer = setInterval(() => {
    void refreshFinalizeLock().catch((err) => {
      lockError = err instanceof Error ? err : new Error("UPLOAD_FINALIZATION_LOCK_LOST");
    });
  }, FINALIZE_LOCK_REFRESH_INTERVAL_MS);
  lockRefreshTimer.unref();

  const assertFinalizeLockHealthy = () => {
    if (lockError) throw lockError;
  };

  try {
    // Re-check inside the lock (race-safe).
    const meta = await redis.hgetall<Record<string, string>>(metaKey);
    if (meta?.status === "completed") {
      if (!meta.fileId || !meta.blobId) {
        throw new Error("CORRUPT_COMPLETED_UPLOAD");
      }

      return {
        fileId: meta.fileId,
        blobId: meta.blobId,
        sizeBytes: Number(meta.sizeBytes ?? session.sizeBytes),
        status: "ready",
        ...(meta.walrusEndEpoch ? { walrusEndEpoch: Number(meta.walrusEndEpoch) } : {}),
        finalize: {
          totalMs: Number(meta.finalizeTotalMs ?? 0),
          stageDurationsMs: {
            verify_chunks: Number(meta.finalizeVerifyMs ?? 0),
            walrus_publish: Number(meta.finalizeWalrusMs ?? 0),
            sui_finalize: Number(meta.finalizeSuiMs ?? 0),
            redis_commit: Number(meta.finalizeRedisMs ?? 0),
            cleanup: Number(meta.finalizeCleanupMs ?? 0),
          },
        },
      };
    }

    await redis.hset(metaKey, {
      status: "finalizing",
      finalizingAt: String(Date.now()),
      finalizeAttemptState: "running",
      finalizeLastProgressAt: String(Date.now()),
    });

    await runStage("verify_chunks", async () => {
      const redisCount = await redis.scard(uploadKeys.chunks(uploadId));
      if (redisCount !== session.totalChunks) {
        throw new Error("INCOMPLETE_CHUNKS");
      }

      const chunks = await chunkStore.listChunks(uploadId);
      const set = new Set(chunks);
      for (let i = 0; i < session.totalChunks; i++) {
        if (!set.has(i)) {
          throw new Error("MISSING_CHUNKS");
        }
      }
    });

    let blobId: string | null = meta?.blobId ?? null;
    let fileId: string | null = meta?.fileId ?? null;
    let walrusEndEpoch: number | undefined =
      meta?.walrusEndEpoch !== undefined ? Number(meta.walrusEndEpoch) : undefined;

    if (!blobId) {
      assertFinalizeLockHealthy();
      await refreshFinalizeLock();
      const result = await runStage("walrus_publish", async () =>
        uploadToWalrusWithMetrics({
          uploadId,
          sizeBytes: session.sizeBytes,
          epochs: session.resolvedEpochs,
          streamFactory: () =>
            createChunkAssemblyStream({
              uploadId,
              totalChunks: session.totalChunks,
            }),
        })
      );

      blobId = result.blobId;
      walrusEndEpoch = result.endEpoch;

      if (!blobId) {
        throw new Error("WALRUS_UPLOAD_FAILED");
      }

      await redis.hset(metaKey, {
        blobId,
        walrusUploadedAt: String(Date.now()),
        ...(walrusEndEpoch !== undefined ? { walrusEndEpoch: String(walrusEndEpoch) } : {}),
      });
    }

    if (!fileId) {
      assertFinalizeLockHealthy();
      await refreshFinalizeLock();
      const minted = await runStage("sui_finalize", async () => {
        const suiStartedAt = Date.now();
        try {
          const result = await finalizeFileMetadata({
            blobId,
            sizeBytes: session.sizeBytes,
            mimeType: session.contentType ?? "application/octet-stream",
            owner: session.owner,
            walrusEndEpoch,
          });
          observeSuiFinalize({
            durationMs: Date.now() - suiStartedAt,
            outcome: "success",
          });
          return result;
        } catch (err) {
          observeSuiFinalize({
            durationMs: Date.now() - suiStartedAt,
            outcome: "failure",
          });
          throw err;
        }
      });

      fileId = minted.fileId;

      await redis.hset(metaKey, {
        fileId,
        metadataFinalizedAt: String(Date.now()),
      });
    }

    assertFinalizeLockHealthy();
    await refreshFinalizeLock();
    const tx = await runStage("redis_commit", async () =>
      redis
        .multi()
        .hset(metaKey, {
          status: "completed",
          fileId,
          blobId,
          sizeBytes: String(session.sizeBytes),
          completedAt: String(Date.now()),
          finalizeStage: "completed",
          finalizeAttemptState: "completed",
          finalizeLastProgressAt: String(Date.now()),
          finalizeVerifyMs: String(stageDurationsMs.verify_chunks),
          finalizeWalrusMs: String(stageDurationsMs.walrus_publish),
          finalizeSuiMs: String(stageDurationsMs.sui_finalize),
          ...(walrusEndEpoch !== undefined ? { walrusEndEpoch: String(walrusEndEpoch) } : {}),
        })
        .del(uploadKeys.session(uploadId))
        .del(uploadKeys.chunks(uploadId))
        .srem(uploadKeys.gcIndex(), uploadId)
        .exec()
    );

    if (!tx) {
      throw new Error("REDIS_FINALIZE_TRANSACTION_FAILED");
    }

    await upsertIndexedFile({
      fileId,
      blobId,
      ownerAddress: session.owner ?? null,
      sizeBytes: session.sizeBytes,
      mimeType: session.contentType ?? "application/octet-stream",
      walrusEndEpoch: walrusEndEpoch ?? null,
      createdAtMs: Date.now(),
    }).catch(() => {});

    await runStage("cleanup", async () => {
      await Promise.all([
        chunkStore.cleanup(uploadId).catch(() => {}),
        fs.unlink(finalFilePath(uploadId)).catch(() => {}),
      ]);
    });

    const finalizeTotalMs = Date.now() - startedAt;
    await redis.hset(metaKey, {
      finalizeStage: "completed",
      finalizeCleanupMs: String(stageDurationsMs.cleanup),
      finalizeRedisMs: String(stageDurationsMs.redis_commit),
      finalizeTotalMs: String(finalizeTotalMs),
      finalizeLastProgressAt: String(Date.now()),
      finalizeAttemptState: "completed",
    });

    context.log?.info(
      {
        uploadId,
        attempt: context.attempt ?? 1,
        queueWaitMs: context.queueWaitMs ?? 0,
        totalMs: finalizeTotalMs,
        stageDurationsMs,
      },
      "Upload finalize completed"
    );

    return {
      fileId,
      blobId,
      sizeBytes: session.sizeBytes,
      status: "ready",
      ...(walrusEndEpoch !== undefined ? { walrusEndEpoch } : {}),
      finalize: {
        totalMs: finalizeTotalMs,
        stageDurationsMs,
      },
    };
  } catch (err) {
    const wrapped = err as Error & { finalizeStage?: FinalizeStage };
    const message = wrapped.message;
    const isLockOwnershipError =
      message === "UPLOAD_FINALIZATION_LOCK_LOST" ||
      message === "UPLOAD_FINALIZATION_IN_PROGRESS";
    const failure = normalizeFinalizeFailure(err);

    // If we no longer own the lock, avoid forcing terminal "failed" state.
    if (!isLockOwnershipError) {
      await redis.hset(metaKey, {
        status: "failed",
        error: message,
        failedAt: String(Date.now()),
        failedStage: wrapped.finalizeStage ?? currentStage ?? "unknown",
        failedReasonCode: failure.reasonCode,
        failedRetryable: failure.retryable ? "1" : "0",
        finalizeAttemptState: failure.retryable ? "retryable_failure" : "terminal_failure",
        finalizeLastProgressAt: String(Date.now()),
      });
    }

    context.log?.error(
      {
        uploadId,
        attempt: context.attempt ?? 1,
        queueWaitMs: context.queueWaitMs ?? 0,
        failedStage: wrapped.finalizeStage ?? currentStage ?? "unknown",
        reasonCode: failure.reasonCode,
        retryable: failure.retryable,
        stageDurationsMs,
        err,
      },
      "Upload finalize failed"
    );

    throw err;
  } finally {
    clearInterval(lockRefreshTimer);
    await releaseFinalizeLockAtomic({ lockKey, lockToken }).catch(() => {});
  }
}
