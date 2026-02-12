// src/routes/files.routes.ts

import { FastifyInstance } from "fastify";
import { suiClient } from "../sui/client.js";

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

    return {
      fileId,
      blobId: fields.blob_id,
      sizeBytes: Number(fields.size_bytes),
      mimeType: fields.mime,
      owner: fields.owner ?? null,
      createdAt: Number(fields.created_at),
    };
  });
}
