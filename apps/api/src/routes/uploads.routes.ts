// src/routes/uploads.routes.ts

import { FastifyInstance } from "fastify";
import crypto from "crypto";

import { sendApiError } from "../utils/apiError.js";
import { ChunkConfig } from "../config/uploads.config.js";
import { WalrusEpochLimits } from "../config/walrus.config.js";

import {
  createSession,
  getSession,
} from "../services/upload/upload.session.js";

import { finalizeUpload } from "../services/upload/upload.finalize.js";

import { chunkStore } from "../store/index.js";
import { getRedis } from "../state/client.js";
import { uploadKeys } from "../state/keys.js";

export default async function uploadRoutes(app: FastifyInstance) {

  app.post("/v1/uploads/create", async (req, reply) => {
    const log = req.log;
    const body = req.body as any;

    if (!body || typeof body !== "object") {
      return sendApiError(reply, 400, "INVALID_REQUEST_BODY", "Request body must be JSON");
    }

    const { filename, contentType, sizeBytes, chunkSize, epochs } = body;

    if (!filename || !contentType || !sizeBytes) {
      return sendApiError(reply, 400, "INVALID_CREATE_UPLOAD_REQUEST", "Missing required fields");
    }

    const fileSizeNum = Number(sizeBytes);
    if (!Number.isFinite(fileSizeNum) || fileSizeNum <= 0) {
      return sendApiError(reply, 400, "INVALID_FILE_SIZE", "sizeBytes must be positive");
    }

    const uploadId = crypto.randomUUID();

    const resolvedChunkSize = Math.min(
      ChunkConfig.maxBytes,
      Math.max(ChunkConfig.minBytes, chunkSize ?? ChunkConfig.defaultBytes)
    );

    const resolvedEpochs = Math.min(
      WalrusEpochLimits.max,
      Math.max(WalrusEpochLimits.min, epochs ?? WalrusEpochLimits.default)
    );

    const totalChunks = Math.ceil(fileSizeNum / resolvedChunkSize);

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
      return sendApiError(reply, 500, "SESSION_CREATE_FAILED", "Failed to create upload session", {
        retryable: true,
      });
    }
  });

  app.put("/v1/uploads/:uploadId/chunk/:index", async (req, reply) => {
    const log = req.log;
    const { uploadId, index } = req.params as any;

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
      log.warn({ uploadId, idx, err }, "Chunk upload failed");
      return sendApiError(
        reply,
        500,
        "CHUNK_UPLOAD_FAILED",
        err.message ?? "Chunk upload failed",
        { retryable: true }
      );
    }
  });

  app.get("/v1/uploads/:uploadId/status", async (req, reply) => {
    const { uploadId } = req.params as { uploadId: string };

    const session = await getSession(uploadId);
    if (!session) {
      return sendApiError(reply, 404, "UPLOAD_NOT_FOUND", "Invalid uploadId");
    }

    const redis = getRedis();
    const members = await redis.smembers(uploadKeys.chunks(uploadId));

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

    const session = await getSession(uploadId);
    if (!session) {
      return sendApiError(reply, 404, "UPLOAD_NOT_FOUND", "Invalid uploadId");
    }

    if (session.status === "completed") {
      return sendApiError(
        reply,
        409,
        "UPLOAD_ALREADY_COMPLETED",
        "Upload is already finalized"
      );
    }

    const redis = getRedis();
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
}
