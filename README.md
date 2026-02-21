# Floe

Floe is a developer-first video ingestion backend for Walrus.

It provides resumable chunk uploads, integrity verification, crash-safe session state, and finalization to a stable on-chain `fileId`.

## What Floe Solves

- Large uploads over unstable networks
- Safe resume after client or server interruption
- Deterministic upload lifecycle with explicit status
- Reliable finalize flow with lock + retry behavior
- Stable read model (`fileId`) decoupled from storage internals

## Core Capabilities

- Chunked upload sessions (`/v1/uploads/*`)
- Unordered chunk uploads with `x-chunk-sha256` validation
- Session + metadata persistence in Redis
- Temporary chunk persistence on disk with GC cleanup
- Finalization to Walrus + on-chain metadata write to Sui
- File read endpoints (`/v1/files/:fileId/*`) including byte-range stream
- Tiered request limits (public vs authenticated request context)

## Architecture

High-level flow:

1. Client creates an upload session.
2. Client uploads chunks in any order.
3. Floe validates hashes and tracks progress.
4. Client requests complete.
5. Floe assembles + uploads to Walrus and writes `FileMeta` on Sui.
6. Clients read through `/v1/files/:fileId/metadata|manifest|stream`.

Runtime components:

- **API**: Fastify routes + upload orchestration
- **Redis**: session state, chunk index, locks, capacity/rate keys
- **Disk**: temporary chunk files and assembly output
- **Walrus**: blob storage publish/read path
- **Sui**: stable file metadata object (maps to `fileId`)

## Local Development

### Requirements

- Node.js `>=20`
- Upstash Redis credentials
- Sui key + RPC access
- Walrus publisher + aggregator endpoints

### Setup

```bash
git clone https://github.com/tejas0111/floe.git
cd floe
npm install
cp .env.example .env
```

Set required environment values in `.env`.

Minimal required `.env`:

```dotenv
PORT=3001
NODE_ENV=development
UPLOAD_TMP_DIR=/home/tejas/Floe/apps/api/tmp/upload/
UPSTASH_REDIS_REST_URL=https://<your-upstash-url>.upstash.io
UPSTASH_REDIS_REST_TOKEN=<your-upstash-token>
WALRUS_PUBLISHER_URL=https://publisher.walrus-testnet.walrus.space
WALRUS_AGGREGATOR_URL=https://aggregator.suicore.com
FLOE_NETWORK=testnet
SUI_PRIVATE_KEY=suiprivkey...
SUI_PACKAGE_ID=0x<your-package-id>
```

All optional tuning values are listed in `.env.example`.

### Run

```bash
npm run dev
```

### Build

```bash
npm run build
npm run start
```

## Upload CLI

Use the bundled uploader:

```bash
./floe-upload.sh "path/to/video.mp4" --parallel 3 --epochs 3
```

Supports resume and API override:

```bash
./floe-upload.sh "path/to/video.mp4" --resume <uploadId>
./floe-upload.sh "path/to/video.mp4" --api http://localhost:3001/v1/uploads
```

## Documentation

- `docs/API.md` – endpoints, request/response behavior, limits
- `docs/OPERATIONS.md` – runtime model, GC, limits, failure handling
- `docs/SECURITY.md` – current auth model and production hardening path

## License

MIT (`LICENSE`)
