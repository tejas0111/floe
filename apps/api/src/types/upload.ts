// src/types/upload.ts

export type UploadStatus =
  | "uploading"
  | "finalizing"
  | "completed"
  | "failed";

export interface UploadSession {
  uploadId: string;
  filename: string;
  contentType: string;
  sizeBytes: number;
  chunkSize: number;
  totalChunks: number;
  receivedChunks: number[];
  resolvedEpochs: number;
  status: UploadStatus;
  createdAt: number;
  expiresAt: number;
}

export interface CompleteUploadResult {
  fileId: string;
  sizeBytes: number;
  status: "ready";
}

