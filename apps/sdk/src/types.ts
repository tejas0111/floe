export type FloeFetch = typeof fetch;

export type HeaderValue = string | number | boolean;

export type HeaderRecord = Record<string, HeaderValue | null | undefined>;

export type AuthConfig = {
  apiKey?: string;
  bearerToken?: string;
  authUser?: string;
  walletAddress?: string;
  ownerAddress?: string;
};

export type HeaderProvider = HeaderRecord | (() => HeaderRecord | Promise<HeaderRecord>);

export type RetryConfig = {
  maxAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  retryOnStatuses?: number[];
};

export type ResumeStore = {
  get(key: string): string | null | undefined | Promise<string | null | undefined>;
  set(key: string, uploadId: string): void | Promise<void>;
  remove(key: string): void | Promise<void>;
};

export type FloeClientConfig = {
  baseUrl?: string;
  fetch?: FloeFetch;
  timeoutMs?: number;
  retry?: RetryConfig;
  auth?: AuthConfig;
  headers?: HeaderProvider;
  userAgent?: string;
  resumeStore?: ResumeStore;
};

export type CreateUploadInput = {
  filename: string;
  contentType: string;
  sizeBytes: number;
  chunkSize?: number;
  epochs?: number;
};

export type CreateUploadResponse = {
  uploadId: string;
  chunkSize: number;
  totalChunks: number;
  epochs: number;
  expiresAt: number;
};

export type FinalizeDiagnostics = {
  pollAfterMs?: number;
  finalizeAttemptState?: "running" | "retryable_failure" | "terminal_failure" | "completed";
  finalizeAttempts?: number;
  lastFinalizeRetryDelayMs?: number;
  failedReasonCode?: string;
  failedRetryable?: boolean;
  finalizeWarning?: string;
  finalizeWarningAt?: number;
};

export type UploadStatusResponse = {
  uploadId: string;
  chunkSize: number | null;
  totalChunks: number | null;
  receivedChunks: number[];
  receivedChunkCount: number;
  expiresAt: number | null;
  status: string;
  fileId?: string;
  blobId?: string;
  walrusEndEpoch?: number;
  error?: string;
} & FinalizeDiagnostics;

export type CompleteUploadReadyResponse = {
  fileId: string;
  blobId?: string;
  sizeBytes: number;
  status: "ready";
  walrusEndEpoch?: number;
};

export type CompleteUploadFinalizingResponse = {
  uploadId: string;
  status: "finalizing";
  pollAfterMs?: number;
  enqueued?: boolean;
  inProgress?: boolean;
} & FinalizeDiagnostics;

export type CompleteUploadResponse =
  | CompleteUploadReadyResponse
  | CompleteUploadFinalizingResponse;

export type FileMetadataResponse = {
  fileId: string;
  manifestVersion: number;
  container: string | null;
  blobId?: string;
  sizeBytes: number;
  mimeType: string;
  owner: unknown;
  createdAt: number;
  walrusEndEpoch?: number;
  streamUrl?: string;
};

export type FileManifestSegment = {
  index: number;
  offsetBytes: number;
  sizeBytes: number;
  blobId?: string;
};

export type FileManifestResponse = {
  fileId: string;
  manifestVersion: number;
  container: string | null;
  mimeType: string;
  sizeBytes: number;
  createdAt: number;
  walrusEndEpoch?: number;
  streamUrl?: string;
  layout: {
    type: "walrus_single_blob";
    segments: FileManifestSegment[];
  };
};

export type UploadProgress = {
  uploadId: string;
  uploadedChunks: number;
  totalChunks: number;
  uploadedBytes: number;
  totalBytes: number;
};

export type UploadBlobOptions = {
  filename: string;
  contentType?: string;
  chunkSize?: number;
  epochs?: number;
  parallel?: number;
  uploadId?: string;
  resumeKey?: string;
  includeBlobId?: boolean;
  finalizeMaxWaitMs?: number;
  finalizePollIntervalMs?: number;
  signal?: AbortSignal;
  onProgress?: (progress: UploadProgress) => void;
};

export type UploadBlobResult = {
  uploadId: string;
  fileId: string;
  sizeBytes: number;
  status: "ready";
  blobId?: string;
  walrusEndEpoch?: number;
  chunkSize: number;
  totalChunks: number;
};

export type WaitForUploadReadyOptions = {
  includeBlobId?: boolean;
  maxWaitMs?: number;
  pollIntervalMs?: number;
  signal?: AbortSignal;
};

export type RequestOptions = {
  signal?: AbortSignal;
  query?: Record<string, string | number | boolean | undefined>;
  headers?: HeaderRecord;
};

export type JsonRequestOptions = RequestOptions & {
  json?: unknown;
  body?: BodyInit;
};

export type FileStreamOptions = RequestOptions & {
  rangeStart?: number;
  rangeEnd?: number;
};
