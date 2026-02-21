# Operations Guide

## Runtime Model

Floe combines:

- Fastify API process
- Redis state store
- Disk temp storage for chunks/assembly
- Background GC worker
- Walrus publish/read integration
- Sui metadata write path

## Required Environment

Minimum required env values:

- `UPLOAD_TMP_DIR`
- `UPSTASH_REDIS_REST_URL`
- `UPSTASH_REDIS_REST_TOKEN`
- `WALRUS_PUBLISHER_URL`
- `WALRUS_AGGREGATOR_URL`
- `FLOE_NETWORK`
- `SUI_PRIVATE_KEY`
- `SUI_PACKAGE_ID`

## Environment Reference

Use `.env.example` as the source of truth. Labels below match runtime behavior.

Mandatory:

- `PORT` (example: `3001`)
- `NODE_ENV` (example: `development`)
- `UPLOAD_TMP_DIR` (example: `/home/tejas/Floe/apps/api/tmp/upload/`)
- `UPSTASH_REDIS_REST_URL` (example: `https://<your-upstash-url>.upstash.io`)
- `UPSTASH_REDIS_REST_TOKEN` (example: `<your-upstash-token>`)
- `WALRUS_PUBLISHER_URL` (example: `https://publisher.walrus-testnet.walrus.space`)
- `WALRUS_AGGREGATOR_URL` (example: `https://walrus-testnet-aggregator.nodes.guru`)
- `FLOE_NETWORK` (`mainnet` or `testnet`)
- `SUI_PRIVATE_KEY` (example: `suiprivkey...`)
- `SUI_PACKAGE_ID` (example: `0x<your-package-id>`)

Optional:

- `SUI_RPC_URL` (auto-default by `FLOE_NETWORK` when omitted)
- `WALRUS_AGGREGATOR_FALLBACK_URLS` (comma-separated aggregator URLs)
- `WALRUS_SEND_OBJECT_TO` (transfer Blob object to this address via publisher flow)
- `WALRUS_READ_TIMEOUT_MS` (default: `600000`)
- `WALRUS_READ_MAX_RETRIES` (default: `2`)
- `WALRUS_READ_RETRY_DELAY_MS` (default: `250`)
- `FLOE_MAX_FILE_SIZE_BYTES` (default: `16106127360`)
- `FLOE_MAX_TOTAL_CHUNKS` (default: `200000`)
- `FLOE_MAX_ACTIVE_UPLOADS` (default: `100`)
- `FLOE_UPLOAD_SESSION_TTL_MS` (default: `21600000`)
- `FLOE_CHUNK_MIN_BYTES` (default: `262144`)
- `FLOE_CHUNK_MAX_BYTES` (default: `20971520`)
- `FLOE_CHUNK_DEFAULT_BYTES` (default: `2097152`)
- `FLOE_GC_INTERVAL_MS` (default: `300000`)
- `FLOE_GC_GRACE_MS` (default: `900000`)
- `FLOE_RATE_LIMIT_WINDOW_SECONDS` (default: `60`)
- `FLOE_RATE_LIMIT_UPLOAD_CONTROL_PUBLIC` (default: `5`)
- `FLOE_RATE_LIMIT_UPLOAD_CONTROL_AUTH` (default: `120`)
- `FLOE_RATE_LIMIT_UPLOAD_CHUNK_PUBLIC` (default: `30`)
- `FLOE_RATE_LIMIT_UPLOAD_CHUNK_AUTH` (default: `1200`)
- `FLOE_RATE_LIMIT_FILE_READ_PUBLIC` (default: `120`)
- `FLOE_RATE_LIMIT_FILE_READ_AUTH` (default: `1200`)
- `FLOE_PUBLIC_MAX_FILE_SIZE_BYTES` (default: `104857600`)
- `FLOE_AUTH_MAX_FILE_SIZE_BYTES` (default: `16106127360`)
- `FLOE_ENFORCE_UPLOAD_OWNER` (`0`/`1`, default: `0`)
- `FLOE_DEFAULT_OWNER_ADDRESS` (fallback owner for FileMeta when request has no owner header)
- `FLOE_EXPOSE_BLOB_ID` (`0`/`1`, default hidden)
- `FLOE_FILE_FIELDS_CACHE_TTL_MS` (default: `86400000`)
- `FLOE_STREAM_MAX_RANGE_BYTES` (default: `67108864`)
- `FLOE_LOG_WALRUS_METRICS` (`0`/`1`, default: `0`)
- `FLOE_CORS_ORIGINS` (comma-separated list, empty means CORS disabled)

## Default Limits

From current config:

- max file size: `15GB`
- max chunk size: `20MB`
- max active uploads: `100`
- max total chunks/upload: `200000`
- session TTL: `6h`

Tiered policy defaults:

- public upload size cap: `100MB`
- public request rates: lower than authenticated tier

## Garbage Collection

GC scans tracked uploads and cleans failed/expired/canceled artifacts.

Important behaviors:

- lock-aware safety for finalization-in-progress uploads
- reconciliation path for orphan disk entries
- capacity index cleanup to avoid stale admission pressure

## Finalization Behavior

Finalize path includes:

- lock acquisition + periodic lock refresh
- chunk completeness checks
- assembly to temp file
- Walrus publish with retry
- Sui metadata write
- Redis state transition to completed
- cleanup of session/chunk keys and disk artifacts

## Operational Recommendations

- run behind reverse proxy with request size/timeouts aligned to chunk policy
- monitor 429, 5xx, finalize failures, and GC lag
- alert on Redis/Sui/Walrus connectivity errors
- keep `UPLOAD_TMP_DIR` on reliable local storage

## Backups and Recovery

- Redis stores active session metadata, not final blobs
- final source of content is Walrus blob + Sui `FileMeta`
- ensure key management and environment secrets are backed up securely
