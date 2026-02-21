# API Reference

Base URL (local default):

```text
http://localhost:3001
```

## Upload Endpoints

### `POST /v1/uploads/create`
Create an upload session.

Required JSON fields:

- `filename`
- `contentType`
- `sizeBytes`

Optional:

- `chunkSize`
- `epochs`

Returns `uploadId`, `chunkSize`, `totalChunks`, `epochs`, `expiresAt`.

### `PUT /v1/uploads/:uploadId/chunk/:index`
Upload a chunk for a session.

Required:

- multipart file field (`chunk`)
- header: `x-chunk-sha256`

Returns `{ ok: true, chunkIndex }` on success.

### `GET /v1/uploads/:uploadId/status`
Read upload status and received chunk indexes.

Response includes:

- `receivedChunks` (sorted ascending)
- `receivedChunkCount`
- optional `walrusEndEpoch` once upload has been published

### `POST /v1/uploads/:uploadId/complete`
Finalize upload, publish to Walrus, write Sui metadata.

Returns:

- `fileId`
- `sizeBytes`
- `status: "ready"`
- optional `blobId` (only when explicitly exposed)

### `DELETE /v1/uploads/:uploadId`
Cancel an in-progress upload session.

## File Endpoints

### `GET /v1/files/:fileId/metadata`
Returns normalized file metadata.

### `GET /v1/files/:fileId/manifest`
Returns stream layout contract.

### `GET /v1/files/:fileId/stream`
Byte-range stream endpoint.

Supports `Range` header and returns proper range semantics (`200/206/416`).

### `HEAD /v1/files/:fileId/stream`
Metadata-only stream headers, including range-aware status/headers.

## Blob ID Exposure

By default, `blobId` is hidden in file/upload responses.

Expose only when needed:

- query: `?includeBlobId=1` (or equivalent flags)
- or server env: `FLOE_EXPOSE_BLOB_ID=1`

## Request Tiers and Limits

Current default limits are tier-based.

Public tier:

- `upload_control`: `5` per 60s
- `upload_chunk`: `30` per 60s
- max upload size: `100MB`

Authenticated-context tier:

- `upload_control`: `120` per 60s
- `upload_chunk`: `1200` per 60s
- `file_read`: `1200` per 60s
- max upload size: `15GB`

Public `file_read` default:

- `file_read`: `120` per 60s

These are configurable via env vars in `apps/api/src/config/auth.config.ts`.

## Auth Context Headers (Current Scaffold)

Current system treats these as authenticated-context signals (scaffold stage):

- `Authorization: Bearer ...`
- `x-api-key`
- `x-auth-user`
- `x-wallet-address`
- `x-owner-address`

Note: this is not cryptographic identity verification yet.

## Errors

Error format:

```json
{
  "error": {
    "code": "ERROR_CODE",
    "message": "Human readable message",
    "retryable": false
  }
}
```

Common codes include:

- `INVALID_*`
- `UPLOAD_NOT_FOUND`
- `UPLOAD_INCOMPLETE`
- `UPLOAD_FINALIZATION_IN_PROGRESS`
- `UPLOAD_CAPACITY_REACHED`
- `RATE_LIMITED`
- `UPLOAD_FAILED`
- `OWNER_MISMATCH`
- `FILE_BLOB_UNAVAILABLE`

## Upload Lifecycle Contract

States:

- `uploading`
- `finalizing`
- `completed`
- `failed`
- `canceled`
- `expired`

Lifecycle notes:

- `complete` is idempotent: if already completed, returns existing result.
- `cancel` is idempotent for canceled/failed/expired sessions.
- `status` always returns deterministic chunk ordering.

## Contract Stability (v1)

For `/v1/*`, clients can depend on:

- stable endpoint paths and HTTP methods
- canonical error envelope
- rate-limit headers on limited routes
- `x-request-id` on all responses

## Response Headers

All responses include:

- `x-request-id`

Rate-limited endpoints also include:

- `X-RateLimit-Limit`
- `X-RateLimit-Remaining`
- `X-RateLimit-Window`
- `Retry-After` (when limited)
