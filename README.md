# Floe

Developer-first video ingestion for Walrus: resumable chunked uploads, server-side state tracking, and a stable on-chain `fileId` for each uploaded asset.

## Features (v1)
- Resumable uploads with unordered, parallel chunk transfer
- Per-chunk integrity verification via `x-chunk-sha256`
- Redis-backed session state with crash-safe reconciliation
- Finalization publishes to Walrus and mints an on-chain `fileId` (permanent identifier)
- Cancel endpoint and garbage collection for incomplete uploads
- Stable asset model:
  - `GET /v1/files/:fileId/metadata` (summary)
  - `GET /v1/files/:fileId/manifest` (read-path layout contract)
  - Walrus `blobId` is hidden by default; include with `?includeBlobId=1` for debugging

## Architecture
Floe separates the control plane from the data plane:
- Control plane: upload sessions, status, and asset identifiers (`fileId`)
- Data plane: chunk persistence, assembly, and Walrus publishing

Storage:
- Chunks are written to disk first (crash-safe)
- Redis tracks upload state and metadata
- Final blobs are published to Walrus

Lifecycle:
```
Create session -> Upload chunks -> Verify completion -> Finalize to Walrus
```

```
src/
├── routes/          API endpoints
├── services/        Upload lifecycle + Walrus integration
├── store/           Chunk persistence + metadata
├── state/           Redis client + GC workers
└── config/          Runtime settings
```

This layout reflects current responsibilities and may evolve as streaming and SDK layers are introduced.

**Key pieces:**

`upload.session.ts` – Creates and tracks upload sessions  
`upload.finalize.ts` – Assembles chunks and publishes to Walrus  
`walrus.upload.ts` – Retry logic and blob publishing  
`upload.gc.worker.ts` – Cleans up expired/failed uploads 

## Running Locally

**Requirements:**
- Node.js 20+
- Redis
- Access to a Walrus node

**Setup:**
```bash
git clone https://github.com/tejas0111/floe.git
cd floe && npm install
cp .env.example .env
# Edit .env with your Walrus endpoint

npm run dev
# or
npm run build && npm run start
```

**Upload a file:**
```bash
# Default API base is http://localhost:3001/v1/uploads
# If your API runs on a different port, use --api or set FLOE_API_BASE.
./floe-upload.sh "path/to/video.mp4" --chunk 2 --epochs 3 --parallel 3
```

The CLI:
- creates an upload session
- splits the file into server-approved chunk sizes (max 20MB per chunk)
- uploads chunks in parallel (unordered)
- finalizes into a Walrus blob
- mints an on-chain `fileId` (permanent object ID)

On success it prints the `fileId` and a metadata URL:
- `GET /v1/files/<fileId>/metadata`

If the upload is interrupted, you can resume without re-uploading completed chunks:

```bash
# Auto-resume if a state file exists: <file>.floe-upload.json
./floe-upload.sh "path/to/video.mp4" --parallel 3

# Or explicitly resume by uploadId
./floe-upload.sh "path/to/video.mp4" --resume <uploadId> --parallel 3
```

**CLI settings:**
- `--api <url>` overrides the API base (example: `http://localhost:3000/v1/uploads`)
- `FLOE_API_BASE` can be used instead of `--api`
- Curl tuning:
  - `FLOE_CURL_CONNECT_TIMEOUT_S` (default: 5)
  - `FLOE_CURL_MAX_TIME_S` (default: 240)
  - `FLOE_CURL_RETRY` (default: 3)
  - `FLOE_CURL_RETRY_DELAY_S` (default: 1)

## API reference

Floe v1 exposes an ingestion control plane and a stable asset model. It does not
serve video playback yet (streaming/read endpoints are a planned next step).

Simple REST endpoints:

```
POST   /v1/uploads/create           # Start new upload session
PUT    /v1/uploads/:id/chunk/:n     # Upload chunk n
GET    /v1/uploads/:id/status       # Check upload progress
POST   /v1/uploads/:id/complete     # Finalize to Walrus
DELETE /v1/uploads/:id              # Cancel upload

GET    /v1/files/:fileId/metadata   # Asset metadata (stable fields)
GET    /v1/files/:fileId/manifest   # Asset layout manifest (read-path contract)
```

Each chunk upload includes an `x-chunk-sha256` header for integrity verification.
By default, Floe does not expose the underlying Walrus `blobId` in file
responses. To include it for debugging, pass `?includeBlobId=1`.

## Default Limits (v1)
These defaults are intended to prevent accidental DoS when exposed publicly:
- Max chunk size: 20 MiB
- Max file size: 15 GiB
- Max total chunks per upload: 200,000
- Max active uploads admitted: 100

## Roadmap
- Playback/read endpoints (HTTP Range) built on top of `manifest`
- Developer SDK
- Optional access control (signed URLs) for playback endpoints
- Operational packaging (Docker/K8s) and metrics

## Contributing
Issues and pull requests are welcome. Please open a discussion for large changes so the API and storage model stay consistent.

## License
MIT
