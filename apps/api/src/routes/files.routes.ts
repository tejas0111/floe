// src/routes/files.routes.ts

import { FastifyInstance } from "fastify";
import { Readable } from "node:stream";

import { suiClient } from "../sui/client.js";
import { getRedis } from "../state/client.js";
import { fileKeys } from "../state/keys.js";
import { fetchWalrusBlob } from "../services/upload/walrus.read.js";
import { WalrusReadLimits } from "../config/walrus.config.js";

function inferContainerFromMime(mimeType: string): string | null {
  const m = (mimeType ?? "").toLowerCase();
  if (m.includes("mp4")) return "mp4";
  if (m.includes("webm")) return "webm";
  if (m.includes("quicktime")) return "mov";
  if (m.includes("x-matroska") || m.includes("mkv")) return "mkv";
  return null;
}

function shouldExposeBlobId(req: any): boolean {
  // Default: never expose blobId unless explicitly requested.
  if (process.env.FLOE_EXPOSE_BLOB_ID === "1") return true;
  const q = req?.query ?? {};
  const raw = q.includeBlobId ?? q.include_blob_id ?? q.includeStorage;
  return raw === "1" || raw === "true" || raw === true;
}

type ParsedRange = {
  start: number;
  end: number;
};

type NormalizedFileFields = {
  blobId: string;
  sizeBytes: number;
  mimeType: string;
  createdAt: number;
  owner: unknown;
};

function normalizeFileFields(fields: any): NormalizedFileFields | null {
  if (!fields || typeof fields !== "object") return null;

  const blobId = typeof fields.blob_id === "string" ? fields.blob_id.trim() : "";
  const rawSizeBytes = Number(fields.size_bytes);
  const rawCreatedAt = Number(fields.created_at);
  const mimeType =
    typeof fields.mime === "string" && fields.mime.trim().length > 0
      ? fields.mime
      : "application/octet-stream";

  if (!blobId) return null;
  if (!Number.isFinite(rawSizeBytes) || !Number.isInteger(rawSizeBytes) || rawSizeBytes <= 0) {
    return null;
  }
  if (!Number.isFinite(rawCreatedAt) || rawCreatedAt < 0) return null;

  return {
    blobId,
    sizeBytes: rawSizeBytes,
    mimeType,
    createdAt: rawCreatedAt,
    owner: fields.owner ?? null,
  };
}

function parseSingleRangeHeader(params: {
  rangeHeader: string;
  sizeBytes: number;
}): { range: ParsedRange; kind: "bounded" | "open" | "suffix" } | { error: "INVALID_RANGE" } {
  const { rangeHeader, sizeBytes } = params;

  const m = rangeHeader.trim().match(/^bytes=(\d*)-(\d*)$/i);
  if (!m) return { error: "INVALID_RANGE" };

  const rawStart = m[1];
  const rawEnd = m[2];

  // Suffix: bytes=-N
  if (rawStart === "" && rawEnd !== "") {
    const suffixLen = Number(rawEnd);
    if (!Number.isFinite(suffixLen) || suffixLen <= 0) return { error: "INVALID_RANGE" };

    const end = sizeBytes - 1;
    const start = Math.max(0, sizeBytes - suffixLen);
    return { range: { start, end }, kind: "suffix" };
  }

  const start = Number(rawStart);
  if (!Number.isFinite(start) || start < 0) return { error: "INVALID_RANGE" };

  // Open ended: bytes=N-
  if (rawEnd === "") {
    const end = sizeBytes - 1;
    if (start > end) return { error: "INVALID_RANGE" };
    return { range: { start, end }, kind: "open" };
  }

  const endRaw = Number(rawEnd);
  if (!Number.isFinite(endRaw) || endRaw < start) return { error: "INVALID_RANGE" };
  if (start >= sizeBytes) return { error: "INVALID_RANGE" };

  const end = Math.min(endRaw, sizeBytes - 1);
  return { range: { start, end }, kind: "bounded" };
}

const FILE_FIELDS_CACHE_TTL_MS = Number(
  process.env.FLOE_FILE_FIELDS_CACHE_TTL_MS ?? 24 * 60 * 60_000
);

function fileFieldsCacheKey(fileId: string) {
  return fileKeys.fields(fileId);
}

async function getFileFieldsCached(fileId: string): Promise<any | null> {
  const redis = getRedis();

  if (Number.isFinite(FILE_FIELDS_CACHE_TTL_MS) && FILE_FIELDS_CACHE_TTL_MS > 0) {
    const cached = await redis
      .get<string>(fileFieldsCacheKey(fileId))
      .catch(() => null);

    if (cached) {
      try {
        return JSON.parse(cached);
      } catch {
        // Ignore corrupt cache entries.
      }
    }
  }

  const obj = await suiClient.getObject({
    id: fileId,
    options: { showContent: true },
  });

  if (!obj.data?.content || obj.data.content.dataType !== "moveObject") {
    return null;
  }

  const fields = obj.data.content.fields as any;

  if (Number.isFinite(FILE_FIELDS_CACHE_TTL_MS) && FILE_FIELDS_CACHE_TTL_MS > 0) {
    await redis
      .set(fileFieldsCacheKey(fileId), JSON.stringify(fields), {
        px: FILE_FIELDS_CACHE_TTL_MS,
      })
      .catch(() => {});
  }

  return fields;
}

async function* walrusByteStream(params: {
  blobId: string;
  start: number;
  end: number;
  maxSegmentBytes: number;
  signal: AbortSignal;
}): AsyncGenerator<Uint8Array> {
  const makeWalrusReadError = (upstreamStatus: number, upstreamBody: string) => {
    const err = new Error(
      `WALRUS_RANGE_FAILED status=${upstreamStatus} ${upstreamBody || ""}`.trim()
    ) as Error & { statusCode?: number };

    if (upstreamStatus === 404) {
      err.statusCode = 404;
      err.message = "FILE_CONTENT_NOT_FOUND";
      return err;
    }

    // Upstream read errors should not be surfaced as a generic 500.
    err.statusCode = upstreamStatus >= 500 ? 503 : 502;
    return err;
  };

  const maxSegmentBytes =
    Number.isFinite(params.maxSegmentBytes) && params.maxSegmentBytes > 0
      ? params.maxSegmentBytes
      : 16 * 1024 * 1024;

  const minSegmentBytes = 256 * 1024; // 256KiB

  let offset = params.start;

  while (offset <= params.end) {
    if (params.signal.aborted) return;

    // Start optimistic, shrink on 416.
    let segSize = Math.min(maxSegmentBytes, params.end - offset + 1);

    while (true) {
      const segEnd = Math.min(params.end, offset + segSize - 1);

      const { res: upstream } = await fetchWalrusBlob({
        blobId: params.blobId,
        rangeHeader: `bytes=${offset}-${segEnd}`,
        signal: params.signal,
      });

      // Some public aggregators enforce strict/low max range sizes and return 416.
      // Adapt by shrinking the request until it succeeds.
      if (upstream.status === 416 && segSize > minSegmentBytes) {
        segSize = Math.max(minSegmentBytes, Math.floor(segSize / 2));
        continue;
      }

      const isFullObjectAttempt =
        params.start === 0 && offset === 0 && segEnd === params.end;

      if (upstream.status === 200 && isFullObjectAttempt) {
        // Some aggregators return 200 for full-span requests even with a Range header.
        // Accept this when we are fetching the whole object in one segment.
      } else if (upstream.status !== 206) {
        const text = await upstream.text().catch(() => "");
        throw makeWalrusReadError(upstream.status, text);
      }

      const body = upstream.body;
      if (!body) return;

      const rs = Readable.fromWeb(body as any);
      const expected = segEnd - offset + 1;
      let read = 0;

      for await (const chunk of rs) {
        if (params.signal.aborted) return;
        const buf = chunk as Uint8Array;
        read += buf.byteLength;
        yield buf;
      }

      if (read < expected) {
        if (read === 0) {
          throw new Error(
            `WALRUS_EMPTY_SEGMENT offset=${offset} end=${segEnd}`
          );
        }

        // Upstream cut the connection early. Retry the remaining bytes.
        // We keep the HTTP response to the client intact; this happens server-side.
        offset += read;
        segSize = Math.max(minSegmentBytes, Math.floor(segSize / 2));
        continue;
      }

      if (read > expected) {
        throw new Error(
          `WALRUS_SEGMENT_OVERRUN expected=${expected} read=${read}`
        );
      }

      offset = segEnd + 1;
      break;
    }
  }
}

export async function filesRoutes(app: FastifyInstance) {
  app.get("/v1/files/:fileId/metadata", async (req, res) => {
    const { fileId } = req.params as { fileId: string };

    let fields: any | null = null;
    try {
      fields = await getFileFieldsCached(fileId);
    } catch (err) {
      req.log.error({ err, fileId }, "Sui read failed");
      return res.status(503).send({
        error: "SUI_UNAVAILABLE",
        message: "Failed to fetch file metadata from Sui",
      });
    }

    if (!fields) {
      return res.status(404).send({ error: "FILE_NOT_FOUND" });
    }

    const normalized = normalizeFileFields(fields);
    if (!normalized) {
      req.log.error({ fileId, fields }, "Invalid file metadata fields");
      return res.status(502).send({
        error: "INVALID_FILE_METADATA",
        message: "File metadata is invalid",
      });
    }

    const exposeBlobId = shouldExposeBlobId(req);
    const container = inferContainerFromMime(normalized.mimeType);

    return {
      fileId,
      manifestVersion: 1,
      container,
      ...(exposeBlobId ? { blobId: normalized.blobId } : {}),
      sizeBytes: normalized.sizeBytes,
      mimeType: normalized.mimeType,
      owner: normalized.owner,
      createdAt: normalized.createdAt,
    };
  });

  app.get("/v1/files/:fileId/manifest", async (req, res) => {
    const { fileId } = req.params as { fileId: string };

    let fields: any | null = null;
    try {
      fields = await getFileFieldsCached(fileId);
    } catch (err) {
      req.log.error({ err, fileId }, "Sui read failed");
      return res.status(503).send({
        error: "SUI_UNAVAILABLE",
        message: "Failed to fetch file metadata from Sui",
      });
    }

    if (!fields) {
      return res.status(404).send({ error: "FILE_NOT_FOUND" });
    }

    const normalized = normalizeFileFields(fields);
    if (!normalized) {
      req.log.error({ fileId, fields }, "Invalid file metadata fields");
      return res.status(502).send({
        error: "INVALID_FILE_METADATA",
        message: "File metadata is invalid",
      });
    }

    const exposeBlobId = shouldExposeBlobId(req);
    const container = inferContainerFromMime(normalized.mimeType);

    return {
      manifestVersion: 1,
      fileId,
      createdAt: normalized.createdAt,
      sizeBytes: normalized.sizeBytes,
      mimeType: normalized.mimeType,
      container,
      layout: {
        type: "walrus_single_blob",
        segments: [
          {
            index: 0,
            offsetBytes: 0,
            sizeBytes: normalized.sizeBytes,
            ...(exposeBlobId ? { blobId: normalized.blobId } : {}),
          },
        ],
      },
    };
  });

  app.route({
    method: ["GET", "HEAD"],
    url: "/v1/files/:fileId/stream",
    handler: async (req, reply) => {
      const { fileId } = req.params as { fileId: string };

      let fields: any | null = null;
      try {
        fields = await getFileFieldsCached(fileId);
      } catch (err) {
        req.log.error({ err, fileId }, "Sui read failed");
        reply.type("application/json");
        return reply.status(503).send({
          error: "SUI_UNAVAILABLE",
          message: "Failed to fetch file metadata from Sui",
        });
      }

      if (!fields) {
        reply.type("application/json");
        return reply.status(404).send({ error: "FILE_NOT_FOUND" });
      }

      const normalized = normalizeFileFields(fields);
      if (!normalized) {
        req.log.error({ fileId, fields }, "Invalid file metadata fields");
        reply.type("application/json");
        return reply.status(502).send({
          error: "INVALID_FILE_METADATA",
          message: "File metadata is invalid",
        });
      }

      const blobId = normalized.blobId;
      const sizeBytes = normalized.sizeBytes;
      const mimeType = normalized.mimeType;

      reply.header("Accept-Ranges", "bytes");
      reply.header("ETag", blobId);

      // HEAD requests can be satisfied from metadata.
      if (req.method === "HEAD") {
        reply.header("Content-Type", mimeType);
        reply.header("Content-Length", String(sizeBytes));
        return reply.status(200).send();
      }

      // Only single-range is supported.
      const rangeHeader = (req.headers as any)?.range as string | undefined;

      req.log.debug({ fileId, range: rangeHeader }, "stream request");

      let start = 0;
      let end = sizeBytes - 1;
      let status = 200;

      if (rangeHeader) {
        const parsedOrErr = parseSingleRangeHeader({
          rangeHeader,
          sizeBytes,
        });

        if ("error" in parsedOrErr) {
          reply.type("application/json");
          return reply.status(416).send({
            error: "INVALID_RANGE",
            message: "Unsupported Range header",
          });
        }

        start = parsedOrErr.range.start;
        end = parsedOrErr.range.end;
        status = 206;
      }

      const abortController = new AbortController();
      const abortUpstream = () => abortController.abort();
      req.raw.once("aborted", abortUpstream);
      req.raw.once("close", abortUpstream);

      const span = end - start + 1;
      const isWholeFileRead = !rangeHeader;

      reply.header("Content-Type", mimeType);
      reply.header("Content-Length", String(span));

      if (status === 206) {
        reply.header("Content-Range", `bytes ${start}-${end}/${sizeBytes}`);
      }

      const stream = Readable.from(
        walrusByteStream({
          blobId,
          start,
          end,
          // Whole-file reads may be served as HTTP 200 by some Walrus aggregators.
          // Force one segment so the streamer can accept a full-object 200 response.
          maxSegmentBytes: isWholeFileRead ? span : WalrusReadLimits.maxRangeBytes,
          signal: abortController.signal,
        })
      );

      return reply.status(status).send(stream);
    },
  });
}
