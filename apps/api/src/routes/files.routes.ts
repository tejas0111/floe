// src/routes/files.routes.ts

import { FastifyInstance } from "fastify";
import { suiClient } from "../sui/client.js";
import { Readable } from "node:stream";
import { fetchWalrusBlob } from "../services/upload/walrus.read.js";

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

function parseSingleRangeHeader(
  rangeHeader: string,
  sizeBytes: number
): { start: number; end: number } | null {
  const m = rangeHeader.match(/^bytes=(\d*)-(\d*)$/);
  if (!m) return null;

  const rawStart = m[1];
  const rawEnd = m[2];

  // Suffix: bytes=-N
  if (rawStart === "" && rawEnd !== "") {
    const suffixLen = Number(rawEnd);
    if (!Number.isFinite(suffixLen) || suffixLen <= 0) return null;
    const end = sizeBytes - 1;
    const start = Math.max(0, sizeBytes - suffixLen);
    return { start, end };
  }

  const start = Number(rawStart);
  if (!Number.isFinite(start) || start < 0) return null;

  // Open ended: bytes=N-
  if (rawEnd === "") {
    const end = sizeBytes - 1;
    if (start > end) return null;
    return { start, end };
  }

  const end = Number(rawEnd);
  if (!Number.isFinite(end) || end < start) return null;
  if (start >= sizeBytes) return null;

  return { start, end: Math.min(end, sizeBytes - 1) };
}

async function getFileFields(fileId: string) {
  const obj = await suiClient.getObject({
    id: fileId,
    options: { showContent: true },
  });

  if (!obj.data?.content || obj.data.content.dataType !== "moveObject") {
    return null;
  }

  return obj.data.content.fields as any;
}

export async function filesRoutes(app: FastifyInstance) {
  app.get("/v1/files/:fileId/metadata", async (req, res) => {
    const { fileId } = req.params as { fileId: string };

    const fields = await getFileFields(fileId);
    if (!fields) {
      return res.status(404).send({ error: "FILE_NOT_FOUND" });
    }

    const exposeBlobId = shouldExposeBlobId(req);
    const blobId = fields.blob_id;
    const sizeBytes = Number(fields.size_bytes);
    const mimeType = fields.mime;
    const createdAt = Number(fields.created_at);
    const container = inferContainerFromMime(mimeType);

    return {
      fileId,
      manifestVersion: 1,
      container,
      ...(exposeBlobId ? { blobId } : {}),
      sizeBytes,
      mimeType,
      owner: fields.owner ?? null,
      createdAt,
    };
  });

  app.get("/v1/files/:fileId/manifest", async (req, res) => {
    const { fileId } = req.params as { fileId: string };

    const fields = await getFileFields(fileId);
    if (!fields) {
      return res.status(404).send({ error: "FILE_NOT_FOUND" });
    }

    const exposeBlobId = shouldExposeBlobId(req);
    const blobId = fields.blob_id;
    const sizeBytes = Number(fields.size_bytes);
    const mimeType = fields.mime;
    const createdAt = Number(fields.created_at);
    const container = inferContainerFromMime(mimeType);

    // Manifest v1 models each uploaded asset as a single Walrus blob. Future
    // versions can evolve to multi-blob layouts without breaking the fileId API.
    return {
      manifestVersion: 1,
      fileId,
      createdAt,
      sizeBytes,
      mimeType,
      container,
      layout: {
        type: "walrus_single_blob",
        segments: [
          {
            index: 0,
            offsetBytes: 0,
            sizeBytes,
            ...(exposeBlobId ? { blobId } : {}),
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
      const fields = await getFileFields(fileId);
      if (!fields) {
        return reply.status(404).send({ error: "FILE_NOT_FOUND" });
      }

      const blobId = fields.blob_id as string;
      const sizeBytes = Number(fields.size_bytes);
      const mimeType = fields.mime as string;

      reply.header("Accept-Ranges", "bytes");

      // HEAD requests can be satisfied from metadata.
      if (req.method === "HEAD") {
        reply.header("Content-Type", mimeType);
        reply.header("Content-Length", String(sizeBytes));
        return reply.status(200).send();
      }

      const rangeHeader = (req.headers as any)?.range as string | undefined;
      const parsed = rangeHeader
        ? parseSingleRangeHeader(rangeHeader, sizeBytes)
        : null;

      // Only single-range requests are supported in v1.
      if (rangeHeader && !parsed) {
        reply.type("application/json");
        return reply
          .status(416)
          .send({ error: "INVALID_RANGE", message: "Unsupported Range header" });
      }

      const upstreamRange =
        parsed ? `bytes=${parsed.start}-${parsed.end}` : undefined;

      const upstream = await fetchWalrusBlob({
        blobId,
        rangeHeader: upstreamRange,
      });

      // Strict HTTP Range semantics: if the client asked for a range and the upstream
      // didn't return 206, don't fall back to a full-body response.
      if (parsed && upstream.status !== 206) {
        const text = await upstream.text().catch(() => "");
        reply.type("application/json");
        return reply.status(502).send({
          error: "WALRUS_RANGE_UNSUPPORTED",
          status: upstream.status,
          message: text || "Upstream did not honor Range request",
        });
      }

      // Pass through key headers that matter for playback/seek.
      const copyHeaders = [
        "content-length",
        "content-range",
        "accept-ranges",
        "etag",
        "last-modified",
      ];
      for (const h of copyHeaders) {
        const v = upstream.headers.get(h);
        if (v) reply.header(h, v);
      }

      if (!upstream.ok && upstream.status !== 206) {
        const text = await upstream.text().catch(() => "");
        reply.type("application/json");
        return reply.status(upstream.status).send({
          error: "WALRUS_READ_FAILED",
          status: upstream.status,
          message: text || "Failed to read from Walrus aggregator",
        });
      }

      // For successful reads, serve the asset's declared content type.
      reply.header("Content-Type", mimeType);
      reply.status(upstream.status);
      const body = upstream.body;
      if (!body) return reply.send();
      return reply.send(Readable.fromWeb(body as any));
    },
  });
}
