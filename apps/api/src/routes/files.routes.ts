// src/routes/files.routes.ts

import { FastifyInstance } from "fastify";
import { suiClient } from "../sui/client.js";

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

export async function filesRoutes(app: FastifyInstance) {
  app.get("/v1/files/:fileId/metadata", async (req, res) => {
    const { fileId } = req.params as { fileId: string };

    const obj = await suiClient.getObject({
      id: fileId,
      options: { showContent: true },
    });

    if (!obj.data?.content || obj.data.content.dataType !== "moveObject") {
      return res.status(404).send({ error: "FILE_NOT_FOUND" });
    }

    const fields = obj.data.content.fields as any;
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

    const obj = await suiClient.getObject({
      id: fileId,
      options: { showContent: true },
    });

    if (!obj.data?.content || obj.data.content.dataType !== "moveObject") {
      return res.status(404).send({ error: "FILE_NOT_FOUND" });
    }

    const fields = obj.data.content.fields as any;
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
}
