// src/services/upload/upload.finalize.ts

import crypto from "crypto";
import fs from "fs/promises";
import { createWriteStream, createReadStream } from "fs";
import { once } from "events";

import { UploadConfig } from "../../config/uploads.config.js";
import { InternalSession } from "./upload.session.js";
import { getRedis } from "../../state/client.js";
import { uploadKeys } from "../../state/keys.js";
import { chunkStore } from "../../store/index.js";
import { uploadToWalrusWithMetrics } from "./walrus.metrics.js";

const WALRUS_MAX_RETRIES = 3;
const WALRUS_RETRY_DELAY_MS = 2000;

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

const finalFilePath = (uploadId: string) =>
  `${UploadConfig.tmpDir}/${uploadId}.bin`;

export async function finalizeUpload(session: InternalSession) {
  const redis = getRedis();
  const uploadId = session.uploadId;
  const metaKey = uploadKeys.meta(uploadId);

  const existing = await redis.hgetall(metaKey);
  if (existing?.status === "completed") {
    if (
      !existing.blobId ||
      !existing.videoId ||
      !existing.sizeBytes ||
      !existing.epochs
    ) {
      throw new Error("CORRUPT_COMPLETED_UPLOAD");
    }

    return {
      videoId: existing.videoId,
      blobId: existing.blobId,
      epochs: Number(existing.epochs),
      sizeBytes: Number(existing.sizeBytes),
      status: "ready" as const,
    };
  }

  const lockKey = `${metaKey}:lock`;
  const locked = await redis.set(lockKey, "1", {
    nx: true,
    px: 15 * 60 * 1000,
  });
  if (!locked) throw new Error("UPLOAD_FINALIZATION_IN_PROGRESS");

  try {
    await redis.hset(metaKey, {
      status: "finalizing",
      finalizingAt: String(Date.now()),
    });

    const redisCount = await redis.scard(
      uploadKeys.chunks(uploadId)
    );
    if (redisCount !== session.totalChunks) {
      throw new Error("INCOMPLETE_CHUNKS");
    }

    const chunks = await chunkStore.listChunks(uploadId);
    const set = new Set(chunks);
    for (let i = 0; i < session.totalChunks; i++) {
      if (!set.has(i)) throw new Error("MISSING_CHUNKS");
    }

    const outPath = finalFilePath(uploadId);
    const ws = createWriteStream(outPath, { flags: "w" });

    ws.on("error", err => {
      throw err;
    });

    for (let i = 0; i < session.totalChunks; i++) {
      const rs = chunkStore.openChunk(uploadId, i);
      rs.on("error", err => {
        throw err;
      });

      for await (const buf of rs) {
        if (!ws.write(buf)) {
          await once(ws, "drain");
        }
      }
    }

    ws.end();
    await once(ws, "finish");
    await redis.pexpire(lockKey, 15 * 60 * 1000);

    let blobId: string | null = null;

    for (let attempt = 1; attempt <= WALRUS_MAX_RETRIES; attempt++) {
      try {
        const result = await uploadToWalrusWithMetrics({
          uploadId,
          sizeBytes: session.sizeBytes,
          epochs: session.resolvedEpochs,
          streamFactory: () => createReadStream(outPath),
        });
        blobId = result!.blobId;
        break;
      } catch (err) {
        if (attempt === WALRUS_MAX_RETRIES) throw err;
        await sleep(WALRUS_RETRY_DELAY_MS * attempt);
      }
    }

    if (!blobId) throw new Error("WALRUS_UPLOAD_FAILED");

    await chunkStore.cleanup(uploadId);
    await fs.unlink(outPath).catch(() => {});

    const videoId = crypto.randomUUID();

    const tx = await redis.multi()
      .hset(metaKey, {
        status: "completed",
        blobId,
        videoId,
        epochs: String(session.resolvedEpochs),
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
      videoId,
      blobId,
      epochs: session.resolvedEpochs,
      sizeBytes: session.sizeBytes,
      status: "ready" as const,
    };

  } catch (err) {
    await redis.hset(metaKey, {
      status: "failed",
      error: (err as Error).message,
      failedAt: String(Date.now()),
    });
    throw err;

  } finally {
    await redis.del(lockKey);
  }
}

