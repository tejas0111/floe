import fs from "fs/promises";
import path from "path";
import type { FastifyBaseLogger } from "fastify";

import { getRedis } from "../client.js";
import { uploadKeys } from "../keys.js";
import { UploadConfig } from "../../config/uploads.config.js";

/**
 * Reconcile orphan uploads on disk that are not registered in Redis.
 */
export async function reconcileOrphanUploads(
  log: FastifyBaseLogger
) {
  const redis = getRedis();
  const baseDir = UploadConfig.tmpDir;

  let entries: string[];
  try {
    entries = await fs.readdir(baseDir);
  } catch {
    return;
  }

  for (const entry of entries) {
    const uploadId = entry.endsWith(".bin")
      ? entry.slice(0, -4)
      : entry;

    if (!uploadId || uploadId.includes(".")) continue;

    const isTracked = await redis.sismember(
      uploadKeys.gcIndex(),
      uploadId
    );

    if (isTracked) continue;

    log.warn(
      { uploadId },
      "Recovered orphan upload; registering for GC"
    );

    await redis.multi()
      .hset(uploadKeys.meta(uploadId), {
        status: "expired",
        recoveredAt: String(Date.now()),
      })
      .sadd(uploadKeys.gcIndex(), uploadId)
      .exec();
  }
}

