// src/routes/uploads.routes.ts

import { FastifyInstance } from "fastify";
import crypto from "crypto";
import fs from "fs/promises";
import path from "path";

import { sendApiError } from "../utils/apiError.js";
import { ChunkConfig, UploadConfig } from "../config/uploads.config.js";
import { WalrusEpochLimits } from "../config/walrus.config.js";

import { createSession, getSession } from "../services/upload/upload.session.js";
import { finalizeUpload } from "../services/upload/upload.finalize.js";

import { chunkStore } from "../store/index.js";
import { getRedis } from "../state/client.js";
import { uploadKeys } from "../state/keys.js";

function isUuid(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
      value
    )
  );
}

function finalBinPath(uploadId: string) {
  return path.join(UploadConfig.tmpDir, `${uploadId}.bin`);
}

export default async function uploadRoutes(app: FastifyInstance) {
  app.post("/v1/uploads/create", async (req, reply) => {
    const log = req.log;
    const body = req.body as any;

    if (!body || typeof body !== "object") {
      return sendApiError(
        reply,
        400,
        "INVALID_REQUEST_BODY",
        "Request body must be JSON"
      );
    }

    const { filename, contentType, sizeBytes, chunkSize, epochs } = body;

    if (!filename || !contentType || !sizeBytes) {
      return sendApiError(
        reply,
        400,
        "INVALID_CREATE_UPLOAD_REQUEST",
        "Missing required fields"
      );
    }

    if (typeof filename !== "string" || filename.length > 512) {
      return sendApiError(
        reply,
        400,
        "INVALID_FILENAME",
        "filename must be <= 512 chars"
      );
    }

    if (typeof contentType !== "string" || contentType.length > 128) {
      return sendApiError(
        reply,
        400,
        "INVALID_CONTENT_TYPE",
        "contentType must be <= 128 chars"
      );
    }

    const fileSizeNum = Number(sizeBytes);
    if (!Number.isFinite(fileSizeNum) || fileSizeNum <= 0) {
      return sendApiError(reply, 400, "INVALID_FILE_SIZE", "sizeBytes must be positive");
    }

    if (fileSizeNum > UploadConfig.maxFileSizeBytes) {
      return sendApiError(
        reply,
        413,
        "FILE_TOO_LARGE",
        `File exceeds maxFileSizeBytes (${UploadConfig.maxFileSizeBytes})`
      );
    }

    let chunkSizeNum: number | undefined;
    if (chunkSize !== undefined) {
      chunkSizeNum = Number(chunkSize);
      if (!Number.isFinite(chunkSizeNum) || chunkSizeNum <= 0) {
        return sendApiError(
          reply,
          400,
          "INVALID_CHUNK_SIZE",
          "chunkSize must be a positive number"
        );
      }
    }

    let epochsNum: number | undefined;
    if (epochs !== undefined) {
      epochsNum = Number(epochs);
      if (!Number.isFinite(epochsNum) || epochsNum <= 0) {
        return sendApiError(reply, 400, "INVALID_EPOCHS", "epochs must be a positive number");
      }
    }

    const resolvedChunkSize = Math.min(
      ChunkConfig.maxBytes,
      Math.max(ChunkConfig.minBytes, chunkSizeNum ?? ChunkConfig.defaultBytes)
    );

    const resolvedEpochs = Math.min(
      WalrusEpochLimits.max,
      Math.max(WalrusEpochLimits.min, epochsNum ?? WalrusEpochLimits.default)
    );

    const totalChunks = Math.ceil(fileSizeNum / resolvedChunkSize);

    if (!Number.isFinite(totalChunks) || totalChunks <= 0) {
      return sendApiError(reply, 400, "INVALID_TOTAL_CHUNKS", "Invalid totalChunks derived from inputs");
    }

    if (totalChunks > UploadConfig.maxTotalChunks) {
      return sendApiError(
        reply,
        413,
        "TOO_MANY_CHUNKS",
        `totalChunks exceeds maxTotalChunks (${UploadConfig.maxTotalChunks})`
      );
    }

    const redis = getRedis();
    const activeUploads = await redis.scard(uploadKeys.gcIndex());
    if (activeUploads >= UploadConfig.maxActiveUploads) {
      return sendApiError(reply, 429, "UPLOAD_CAPACITY_REACHED", "Too many active uploads", {
        retryable: true,
      });
    }

    const uploadId = crypto.randomUUID();

    try {
      const session = await createSession({
        uploadId,
        filename,
        contentType,
        sizeBytes: fileSizeNum,
        chunkSize: resolvedChunkSize,
        totalChunks,
        epochs: resolvedEpochs,
      });

      log.info({ uploadId, totalChunks }, "Upload session created");

      return reply.code(201).send({
        uploadId: session.uploadId,
        chunkSize: session.chunkSize,
        totalChunks: session.totalChunks,
        epochs: session.resolvedEpochs,
        expiresAt: session.expiresAt,
      });
    } catch (err) {
      log.error({ err }, "Session creation failed");
      return sendApiError(
        reply,
        500,
        "SESSION_CREATE_FAILED",
        "Failed to create upload session",
        {
          retryable: true,
        }
      );
    }
  });

  app.put("/v1/uploads/:uploadId/chunk/:index", async (req, reply) => {
    const log = req.log;
    const { uploadId, index } = req.params as any;

    if (!isUuid(uploadId)) {
      return sendApiError(reply, 400, "INVALID_UPLOAD_ID", "uploadId must be a UUID");
    }

    const session = await getSession(uploadId);
    if (!session) {
      return sendApiError(reply, 404, "UPLOAD_NOT_FOUND", "Invalid uploadId");
    }

    if (session.status === "completed") {
      return sendApiError(reply, 409, "UPLOAD_ALREADY_COMPLETED", "Upload is already finalized");
    }

    const idx = Number(index);
    const expectedHash = req.headers["x-chunk-sha256"];

    if (
      !Number.isInteger(idx) ||
      idx < 0 ||
      idx >= session.totalChunks ||
      typeof expectedHash !== "string"
    ) {
      return sendApiError(reply, 400, "INVALID_CHUNK", "Invalid chunk index or hash");
    }

    let part;
    try {
      part = await req.file();
    } catch {
      return sendApiError(reply, 400, "CHUNK_STREAM_ERROR", "Failed to read chunk stream", {
        retryable: true,
      });
    }

    if (!part || part.type !== "file") {
      return sendApiError(reply, 400, "INVALID_CHUNK", "Multipart file field required", {
        retryable: true,
      });
    }

    try {
      const isLastChunk = idx === session.totalChunks - 1;

      const expectedSize = isLastChunk
        ? session.sizeBytes - session.chunkSize * (session.totalChunks - 1)
        : session.chunkSize;

      await chunkStore.writeChunk(
        uploadId,
        idx,
        part.file,
        expectedHash,
        expectedSize,
        isLastChunk
      );

      const redis = getRedis();
      await redis.sadd(uploadKeys.chunks(uploadId), String(idx));

      return { ok: true, chunkIndex: idx };
    } catch (err: any) {
      const message = err?.message ?? "Chunk upload failed";

      if (err?.message === "CHUNK_IN_PROGRESS") {
        log.info({ uploadId, idx }, "Chunk upload already in progress");
        return sendApiError(
          reply,
          409,
          "CHUNK_IN_PROGRESS",
          "Chunk upload already in progress, retry shortly",
          { retryable: true }
        );
      }

      if (
        err?.message === "HASH_MISMATCH" ||
        err?.message === "CHUNK_TOO_LARGE" ||
        err?.message === "CHUNK_SIZE_MISMATCH" ||
        err?.message === "INVALID_LAST_CHUNK_SIZE"
      ) {
        return sendApiError(reply, 400, "INVALID_CHUNK", message, {
          retryable: false,
        });
      }

      log.warn({ uploadId, idx, err }, "Chunk upload failed");
      return sendApiError(
        reply,
        500,
        "CHUNK_UPLOAD_FAILED",
        message,
        { retryable: true }
      );
    }
  });

  app.get("/v1/uploads/:uploadId/status", async (req, reply) => {
    const { uploadId } = req.params as { uploadId: string };

    if (!isUuid(uploadId)) {
      return sendApiError(reply, 400, "INVALID_UPLOAD_ID", "uploadId must be a UUID");
    }

    const redis = getRedis();
    const [session, meta, members] = await Promise.all([
      getSession(uploadId),
      redis.hgetall<Record<string, string>>(uploadKeys.meta(uploadId)),
      redis.smembers(uploadKeys.chunks(uploadId)),
    ]);

    if (!session) {
      const status = meta?.status;
      if (!status) {
        return sendApiError(reply, 404, "UPLOAD_NOT_FOUND", "Invalid uploadId");
      }

      return {
        uploadId,
        chunkSize: meta?.chunkSize ? Number(meta.chunkSize) : null,
        totalChunks: meta?.totalChunks ? Number(meta.totalChunks) : null,
        receivedChunks: members.map(Number).sort((a, b) => a - b),
        expiresAt: meta?.expiresAt ? Number(meta.expiresAt) : null,
        status,
        ...(meta?.fileId ? { fileId: meta.fileId } : {}),
        ...(meta?.blobId ? { blobId: meta.blobId } : {}),
        ...(meta?.error ? { error: meta.error } : {}),
      };
    }

    return {
      uploadId,
      chunkSize: session.chunkSize,
      totalChunks: session.totalChunks,
      receivedChunks: members.map(Number).sort((a, b) => a - b),
      expiresAt: session.expiresAt,
      status: session.status,
    };
  });

  app.post("/v1/uploads/:uploadId/complete", async (req, reply) => {
    const log = req.log;
    const { uploadId } = req.params as { uploadId: string };

    if (!isUuid(uploadId)) {
      return sendApiError(reply, 400, "INVALID_UPLOAD_ID", "uploadId must be a UUID");
    }

    const redis = getRedis();
    const metaKey = uploadKeys.meta(uploadId);
    const [session, meta] = await Promise.all([
      getSession(uploadId),
      redis.hgetall<Record<string, string>>(metaKey),
    ]);

    const metaStatus = meta?.status;

    if (!session) {
      if (metaStatus === "completed") {
        if (!meta?.fileId || !meta?.blobId) {
          return sendApiError(
            reply,
            500,
            "INTERNAL_ERROR",
            "Completed upload metadata is corrupt"
          );
        }

        return reply.code(200).send({
          fileId: meta.fileId,
          blobId: meta.blobId,
          sizeBytes: Number(meta.sizeBytes ?? 0),
          status: "ready",
        });
      }

      if (metaStatus === "finalizing") {
        return sendApiError(
          reply,
          409,
          "UPLOAD_FINALIZATION_IN_PROGRESS",
          "Upload is currently finalizing",
          { retryable: true }
        );
      }

      return sendApiError(reply, 404, "UPLOAD_NOT_FOUND", "Invalid uploadId");
    }

    if (session.status === "completed" || metaStatus === "completed") {
      if (meta?.fileId && meta?.blobId) {
        return reply.code(200).send({
          fileId: meta.fileId,
          blobId: meta.blobId,
          sizeBytes: Number(meta.sizeBytes ?? session.sizeBytes),
          status: "ready",
        });
      }

      return sendApiError(
        reply,
        409,
        "UPLOAD_ALREADY_COMPLETED",
        "Upload is already finalized"
      );
    }

    const receivedChunks = await redis.scard(uploadKeys.chunks(uploadId));

    if (receivedChunks !== session.totalChunks) {
      return sendApiError(
        reply,
        400,
        "UPLOAD_INCOMPLETE",
        `Only ${receivedChunks}/${session.totalChunks} chunks uploaded`,
        { retryable: true }
      );
    }

    try {
      const result = await finalizeUpload(session);

      log.info(
        {
          uploadId,
          fileId: result.fileId,
          blobId: result.blobId,
        },
        "Upload finalized"
      );

      return reply.code(200).send(result);
    } catch (err: any) {
      if (err?.message === "UPLOAD_FINALIZATION_IN_PROGRESS") {
        return sendApiError(
          reply,
          409,
          "UPLOAD_FINALIZATION_IN_PROGRESS",
          "Upload is currently finalizing",
          { retryable: true }
        );
      }

      log.error({ uploadId, err }, "Upload finalization failed");

      return sendApiError(
        reply,
        502,
        "UPLOAD_FAILED",
        err.message ?? "Upload finalization failed",
        { retryable: true }
      );
    }
  });

  // Cancel an in-progress upload session. Best-effort cleanup; GC will catch any leftovers.
  // This endpoint is intentionally idempotent: repeated cancels return 200 when the upload
  // is already canceled/expired/failed.
  app.delete("/v1/uploads/:uploadId", async (req, reply) => {
    const log = req.log;
    const { uploadId } = req.params as { uploadId: string };

    if (!isUuid(uploadId)) {
      return sendApiError(reply, 400, "INVALID_UPLOAD_ID", "uploadId must be a UUID");
    }

    const redis = getRedis();
    const metaKey = uploadKeys.meta(uploadId);
    const lockKey = `${metaKey}:lock`;

    const [session, meta, hasLock] = await Promise.all([
      getSession(uploadId),
      redis.hgetall<Record<string, string>>(metaKey),
      redis.exists(lockKey),
    ]);

    if (hasLock) {
      return sendApiError(
        reply,
        409,
        "UPLOAD_FINALIZATION_IN_PROGRESS",
        "Upload is currently finalizing"
      );
    }

    const status = meta?.status;

    // If the session is already gone, treat delete as idempotent whenever we have
    // a meta record to explain what happened.
    if (!session) {
      if (!status) {
        return sendApiError(reply, 404, "UPLOAD_NOT_FOUND", "Invalid uploadId");
      }

      if (status === "completed") {
        return sendApiError(
          reply,
          409,
          "UPLOAD_ALREADY_COMPLETED",
          "Upload is already finalized"
        );
      }

      if (status === "canceled" || status === "failed" || status === "expired") {
        // Ensure this upload no longer counts against active capacity.
        await redis.srem(uploadKeys.gcIndex(), uploadId);
        return reply.code(200).send({ ok: true, uploadId, status });
      }

      // Unknown/legacy state: attempt to cancel + cleanup best-effort.
      await redis.hset(metaKey, {
        status: "canceled",
        canceledAt: String(Date.now()),
      });

      await Promise.all([
        chunkStore.cleanup(uploadId).catch(() => {}),
        fs.rm(finalBinPath(uploadId), { force: true }).catch(() => {}),
      ]);

      await redis
        .multi()
        .del(uploadKeys.session(uploadId))
        .del(uploadKeys.chunks(uploadId))
        .exec();

      log.info({ uploadId }, "Upload canceled");
      return reply.code(200).send({ ok: true, uploadId, status: "canceled" });
    }

    if (status === "completed" || session.status === "completed") {
      return sendApiError(
        reply,
        409,
        "UPLOAD_ALREADY_COMPLETED",
        "Upload is already finalized"
      );
    }

    await redis.hset(metaKey, {
      status: "canceled",
      canceledAt: String(Date.now()),
    });

    await Promise.all([
      chunkStore.cleanup(uploadId).catch(() => {}),
      fs.rm(finalBinPath(uploadId), { force: true }).catch(() => {}),
    ]);

    // Preserve meta for inspection, but remove active session/chunk state.
    await redis
      .multi()
      .del(uploadKeys.session(uploadId))
      .del(uploadKeys.chunks(uploadId))
      .srem(uploadKeys.gcIndex(), uploadId)
      .exec();

    log.info({ uploadId }, "Upload canceled");
    return reply.code(200).send({ ok: true, uploadId, status: "canceled" });
  });

}
