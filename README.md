# Floe

Floe is a video upload backend for Walrus. It handles chunking, retries, state tracking, and cleanup for large uploads into decentralized storage.

Built for teams that need reliable video infrastructure without the operational overhead.

---

## What it does

Most video platforms treat uploads as an afterthought. Floe treats them as a first-class problem worth solving properly.

**Core capabilities:**
- Resumable uploads via chunked transfers
- Automatic retry logic with exponential backoff
- Server-side state tracking
- Safe finalization into Walrus blobs
- Garbage collection for failed uploads

**Why it matters:**
Large file uploads fail constantly. Networks drop, browsers crash, users close tabs. Without proper chunking and state management, you're stuck restarting from zero every time. That's wasteful and frustrating.

Floe fixes this by treating every upload as a session with explicit lifecycle state. Chunks upload independently, get verified, persist to disk, then assemble into final blobs only when everything's accounted for.

---

## Technical approach

The system separates concerns cleanly:

**Control plane** handles upload sessions, metadata, and lifecycle transitions.  
**Data plane** handles chunk persistence, assembly, and publishing to Walrus.

This split means you can scale, debug, and modify each side independently. The API layer remains stateless, while upload progress and lifecycle state are persisted explicitly.

**Storage layer:**
- Chunks land on disk first (crash-safe)
- Redis tracks upload state and metadata
- Final blobs publish to Walrus

**Lifecycle:**
```
Create session → Upload chunks → Verify completion → Finalize to Walrus
```

**Key feature:** Chunks can arrive in any order. The API doesn't enforce sequential uploads-you can send chunk 5 before chunk 2, upload in parallel from multiple clients, or retry individual chunks without affecting others. The system tracks what's arrived and only finalizes when all chunks are accounted for.

If anything fails mid-stream, the session stays intact. You resume from where you left off.

---

## What's working now

This isn't a prototype. It's running code that handles real uploads. The current implementation has been exercised against real video files and failure scenarios during development.

- Unordered chunk uploads (chunks can arrive in any sequence)
- Parallel uploads from multiple clients
- SHA256 verification per chunk
- Crash recovery and state reconciliation
- Redis-backed state management
- Automatic garbage collection
- Walrus epoch configuration and publishing
- Structured logging with observability


---

## Architecture

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

---

## Running it locally

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

npm run dev or 
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

**Resume an upload:**

If the upload is interrupted, you can resume without re-uploading completed chunks.

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

---

## API reference

These endpoints expose the upload control plane only; they do not serve video playback.

Simple REST endpoints:

```
POST   /v1/uploads/create           # Start new upload session
PUT    /v1/uploads/:id/chunk/:n     # Upload chunk n
GET    /v1/uploads/:id/status       # Check upload progress
POST   /v1/uploads/:id/complete     # Finalize to Walrus
DELETE /v1/uploads/:id              # Cancel upload
```

Each chunk upload includes an `x-chunk-sha256` header for integrity verification.

---

## What's not here yet

Floe currently focuses on reliable ingestion. It doesn't handle:

- Streaming or playback APIs
- Developer SDKs (beyond the reference CLI)
- Access control or encryption
- Transcoding or processing
- Web UI or dashboard
- Advanced analytics

These aren't missing by accident-they're deliberately out of scope for v1. These features are intentionally deferred until the ingestion pipeline is proven stable.

---

## Design philosophy

**Explicit over implicit.**  
Upload state shouldn't be a black box. You should always know what's happening.

**Fail loudly.**  
Better to error clearly than silently corrupt data.

**Don't hide Walrus.**  
This is infrastructure on top of Walrus, not a replacement. The storage layer is transparent.

**Build incrementally.**  
Each piece should work independently before adding the next layer.

---

## Why unordered chunks matter

Most upload systems require chunks to arrive sequentially (chunk 0, then 1, then 2...). This creates unnecessary constraints:

- Parallel uploads from different workers get serialized
- Network hiccups force you to wait for retries in order
- Multi-region uploads can't optimize for fastest paths
- Browser connection limits become bottlenecks

Floe lets chunks arrive in any order. The benefits:

**True parallelism** – Upload chunk 99 and chunk 0 simultaneously. No artificial sequencing.

**Smarter retries** – If chunk 5 fails, retry it immediately. Don't block chunk 6 from succeeding.

**Flexible clients** – Different workers can grab different chunk ranges without coordination.

**Better network utilization** – Use all available connections efficiently, not sequentially.

The tradeoff is complexity in reassembly (you need to track which chunks arrived), but that's the server's job, not yours. Floe accepts the additional server-side complexity so client implementations can remain simple and robust.

---

## Why Walrus

Walrus provides durable, decentralized blob storage with strong correctness guarantees. That's the right foundation for long-term video storage.

But raw blob storage isn't enough. You still need upload orchestration, state management, retry logic, and cleanup-basically everything Floe provides.

Think of Walrus as the storage engine and Floe as the ingestion pipeline.

---

## Production readiness

The core upload pipeline is solid. Tested with real video files, network interruptions, process crashes, and continuous operation.

What's production-ready:

Default v1 limits (to prevent accidental DoS when exposed):
- max chunk size: 20MB
- max file size: 15GB
- max total chunks per upload: 200,000
- max active uploads admitted: 100


- Upload correctness and resumability
- Unordered chunk handling
- State persistence via Redis
- Automatic cleanup and recovery
- Walrus integration with retries

What needs work:
- Horizontal scaling (currently single-node)
- Advanced monitoring and metrics
- Deployment tooling (Docker, K8s)
- Performance tuning under high load

If you're evaluating this for production use, the reliability primitives are there. The operational maturity is still evolving.

---

## Roadmap

This roadmap reflects intent, not commitments.

Near term:
- Prometheus metrics integration
- Multi-node deployment support
- S3-compatible chunk storage backend

Medium term:
- JavaScript SDK
- Streaming-optimized read APIs
- Access control and permissions

Long term:
- Full video platform capabilities
- Managed hosting option
- Enterprise features

Each phase builds on what's working. No rewrites, no pivots.

---

## Why this exists

We needed reliable video infrastructure on Walrus and couldn't find it. Existing solutions were either too opaque, too expensive, or too tightly coupled to centralized platforms.

Walrus gives us the storage layer. Floe gives us everything else needed to actually build video products on top of it.

This started as internal tooling. We're open-sourcing it because the problem is universal and someone else will need this too.

---

## Use cases

This is infrastructure, so use cases are broad:

- Creator platforms and video hosting
- Educational content and course delivery
- Media archives and libraries
- Internal tools and demos
- AI training data storage
- Any application with large file uploads

If you're moving multi-megabyte files around and need them to arrive reliably, Floe probably helps.

---

## Contributing

This is open source (MIT license) and contributions are welcome.

If you find bugs, open an issue. If you want to add features, start a discussion first so we can align on approach.

The goal is to keep this codebase maintainable and understandable. Clever solutions are discouraged unless the problem actually demands them.

---

## Team

Built by developers who needed this to exist.

We're committed to maintaining this long-term because we use it in our own products. That's the best sustainability model-building infrastructure you depend on yourself.

---

## Acknowledgments

Built on [Walrus](https://walrus.xyz) by Mysten Labs.

Inspired by years of painful experience with fragile upload systems, opaque video platforms, and infrastructure that breaks in production.

---

## License

MIT

---

## Status

Active development. APIs may evolve but won't break gratuitously.

If you're building something serious on this, reach out. Early feedback shapes better decisions.
