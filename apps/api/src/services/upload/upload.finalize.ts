// src/services/upload/upload.finalize.ts

import fs from "fs/promises";
import path from "path";
import { createWriteStream, createReadStream } from "fs";
import { once } from "events";
import crypto from "crypto";

import { UploadConfig } from "../../config/uploads.config.js";
import { InternalSession } from "./upload.session.js";
import { getRedis } from "../../state/client.js";
import { uploadKeys, fileKeys } from "../../state/keys.js";
import { chunkStore } from "../../store/index.js";
import { uploadToWalrusWithMetrics } from "./walrus.metrics.js";
import { finalizeFileMetadata } from "../../sui/file.metadata.js";

const finalFilePath = (uploadId: string) =>
  path.join(UploadConfig.tmpDir, `${uploadId}.bin`);

const FINALIZE_LOCK_TTL_SECONDS = 15 * 60;
const FINALIZE_LOCK_REFRESH_INTERVAL_MS = 60_000;

async function assembleChunksToFile(params: {
  uploadId: string;
  totalChunks: number;
  outPath: string;
}) {
  const ws = createWriteStream(params.outPath, { flags: "w" });
  const finished = once(ws, "finish");
  const failed = once(ws, "error").then(([err]) => {
    throw err;
  });

  try {
    for (let i = 0; i < params.totalChunks; i++) {
      const rs = chunkStore.openChunk(params.uploadId, i);
      for await (const buf of rs) {
        if (!ws.write(buf)) {
          await once(ws, "drain");
        }
      }
    }

    ws.end();
    await Promise.race([finished, failed]);
  } catch (err) {
    ws.destroy();
    throw err;
  }
}

export async function finalizeUpload(session: InternalSession): Promise<{
  fileId: string;
  blobId: string;
  sizeBytes: number;
  status: "ready";
}> {
  const redis = getRedis();
  const uploadId = session.uploadId;
  const metaKey = uploadKeys.meta(uploadId);

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
    const current = await redis.get<string>(lockKey);
    if (current !== lockToken) {
      throw new Error("UPLOAD_FINALIZATION_LOCK_LOST");
    }
    await redis.expire(lockKey, FINALIZE_LOCK_TTL_SECONDS);
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
      };
    }

    await redis.hset(metaKey, {
      status: "finalizing",
      finalizingAt: String(Date.now()),
    });

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

    const outPath = finalFilePath(uploadId);

    // Checkpointed fields (best-effort).
    let blobId: string | null = meta?.blobId ?? null;
    let fileId: string | null = meta?.fileId ?? null;

    // Only assemble/upload if we don't already have a durable blobId checkpoint.
    if (!blobId) {
      assertFinalizeLockHealthy();
      await refreshFinalizeLock();
      await assembleChunksToFile({
        uploadId,
        totalChunks: session.totalChunks,
        outPath,
      });

      assertFinalizeLockHealthy();
      await refreshFinalizeLock();
      const result = await uploadToWalrusWithMetrics({
        uploadId,
        sizeBytes: session.sizeBytes,
        epochs: session.resolvedEpochs,
        streamFactory: () => createReadStream(outPath),
      });

      blobId = result.blobId;

      if (!blobId) {
        throw new Error("WALRUS_UPLOAD_FAILED");
      }

      // Checkpoint immediately so retries don't re-upload and re-pay.
      await redis.hset(metaKey, {
        blobId,
        walrusUploadedAt: String(Date.now()),
      });
    }

    if (!fileId) {
      assertFinalizeLockHealthy();
      await refreshFinalizeLock();
      const minted = await finalizeFileMetadata({
        blobId,
        sizeBytes: session.sizeBytes,
        mimeType: session.contentType ?? "application/octet-stream",
        owner: undefined,
      });

      fileId = minted.fileId;

      // Cache fields immediately so streaming does not depend on Sui RPC availability.
      await redis
        .set(
          fileKeys.fields(fileId),
          JSON.stringify({
            blob_id: blobId,
            size_bytes: session.sizeBytes,
            mime: session.contentType ?? "application/octet-stream",
            created_at: Date.now(),
            owner: null,
          }),
          {
            px: Number(process.env.FLOE_FILE_FIELDS_CACHE_TTL_MS ?? 24 * 60 * 60_000),
          }
        )
        .catch(() => {});

      // Checkpoint early to reduce duplicate mint risk on retry.
      await redis.hset(metaKey, {
        fileId,
        metadataFinalizedAt: String(Date.now()),
      });
    }

    await chunkStore.cleanup(uploadId);
    await fs.unlink(outPath).catch(() => {});

    const tx = await redis
      .multi()
      .hset(metaKey, {
        status: "completed",
        fileId,
        blobId,
        sizeBytes: String(session.sizeBytes),
        completedAt: String(Date.now()),
      })
      .del(uploadKeys.session(uploadId))
      .del(uploadKeys.chunks(uploadId))
      .srem(uploadKeys.gcIndex(), uploadId)
      .exec();

    if (!tx) {
      throw new Error("REDIS_FINALIZE_TRANSACTION_FAILED");
    }

    return {
      fileId,
      blobId,
      sizeBytes: session.sizeBytes,
      status: "ready",
    };
  } catch (err) {
    const message = (err as Error).message;
    const isLockOwnershipError =
      message === "UPLOAD_FINALIZATION_LOCK_LOST" ||
      message === "UPLOAD_FINALIZATION_IN_PROGRESS";

    // If we no longer own the lock, avoid forcing terminal "failed" state.
    if (!isLockOwnershipError) {
      await redis.hset(metaKey, {
        status: "failed",
        error: message,
        failedAt: String(Date.now()),
      });
    }

    throw err;
  } finally {
    clearInterval(lockRefreshTimer);
    const current = await redis.get<string>(lockKey).catch(() => null);
    if (current === lockToken) {
      await redis.del(lockKey).catch(() => {});
    }
  }
}
