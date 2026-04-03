import { FloeClient, FloeApiError } from "../dist/index.js";
import fs from "node:fs/promises";
import path from "node:path";

function parseArgs(argv) {
  const out = {
    file: "",
    baseUrl: process.env.FLOE_BASE_URL || "http://127.0.0.1:3001/v1",
    apiKey: process.env.FLOE_API_KEY || "",
    ownerAddress: process.env.FLOE_OWNER_ADDRESS || "",
    chunkSize: undefined,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--file") {
      out.file = argv[i + 1] || "";
      i += 1;
      continue;
    }
    if (arg === "--base-url") {
      out.baseUrl = argv[i + 1] || out.baseUrl;
      i += 1;
      continue;
    }
    if (arg === "--api-key") {
      out.apiKey = argv[i + 1] || "";
      i += 1;
      continue;
    }
    if (arg === "--owner-address") {
      out.ownerAddress = argv[i + 1] || "";
      i += 1;
      continue;
    }
    if (arg === "--chunk-size") {
      const n = Number(argv[i + 1]);
      out.chunkSize = Number.isFinite(n) && n > 0 ? Math.floor(n) : undefined;
      i += 1;
    }
  }

  return out;
}

function inferContentType(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === ".mp4") return "video/mp4";
  if (ext === ".webm") return "video/webm";
  if (ext === ".mov") return "video/quicktime";
  if (ext === ".json") return "application/json";
  if (ext === ".txt") return "text/plain";
  return "application/octet-stream";
}

function printUsage() {
  console.error(`Usage:
  npm run smoke --workspace=apps/sdk -- --file ./path/to/file.mp4 [--base-url http://127.0.0.1:3001/v1] [--api-key key]

Environment fallbacks:
  FLOE_BASE_URL
  FLOE_API_KEY
  FLOE_OWNER_ADDRESS
`);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.file) {
    printUsage();
    process.exitCode = 1;
    return;
  }

  const filePath = path.resolve(args.file);
  const bytes = await fs.readFile(filePath);
  const filename = path.basename(filePath);
  const contentType = inferContentType(filePath);

  const client = new FloeClient({
    baseUrl: args.baseUrl,
    auth: {
      ...(args.apiKey ? { apiKey: args.apiKey } : {}),
      ...(args.ownerAddress ? { ownerAddress: args.ownerAddress } : {}),
    },
  });

  console.log("Uploading", {
    filePath,
    sizeBytes: bytes.byteLength,
    contentType,
    baseUrl: args.baseUrl,
  });

  const result = await client.uploadBytes(bytes, {
    filename,
    contentType,
    ...(args.chunkSize ? { chunkSize: args.chunkSize } : {}),
    includeBlobId: true,
    onProgress(progress) {
      console.log("progress", {
        uploadId: progress.uploadId,
        uploadedChunks: progress.uploadedChunks,
        totalChunks: progress.totalChunks,
        uploadedBytes: progress.uploadedBytes,
        totalBytes: progress.totalBytes,
      });
    },
  });

  console.log("upload complete", result);

  const metadata = await client.getFileMetadata(result.fileId, {
    includeBlobId: true,
  });

  console.log("metadata", metadata);
  console.log("stream url", client.getFileStreamUrl(result.fileId));
}

main().catch((err) => {
  if (err instanceof FloeApiError) {
    console.error("Floe API error", {
      status: err.status,
      code: err.code,
      retryable: err.retryable,
      requestId: err.requestId,
      details: err.details,
      message: err.message,
    });
    process.exitCode = 1;
    return;
  }

  console.error(err);
  process.exitCode = 1;
});
