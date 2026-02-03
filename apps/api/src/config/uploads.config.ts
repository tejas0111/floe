// src/config/uploads.config.ts
import path from "path";

if (!process.env.UPLOAD_TMP_DIR) {
  throw new Error("Missing required env: UPLOAD_TMP_DIR");
}

export const UploadConfig = {
  tmpDir: path.resolve(process.env.UPLOAD_TMP_DIR),

  maxFileSizeBytes: 15 * 1024 * 1024 * 1024, // 15 GB
  maxActiveUploads: 100,
  sessionTtlMs: 6 * 60 * 60 * 1000, // 6 hours
};

export const ChunkConfig = {
  minBytes: 256 * 1024,          // 256 KB
  maxBytes: 20 * 1024 * 1024,    // 20 MB
  defaultBytes: 2 * 1024 * 1024, // 2 MB
};

export const GcConfig = {
  gcInterval: 5 * 60 * 1000,  // 5 minutes
  grace: 15 * 60 * 1000,      // 15 minutes
};

