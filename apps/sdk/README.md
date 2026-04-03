# Floe SDK

`@floehq/sdk` is the official TypeScript SDK for the Floe HTTP API.

It provides:

- typed upload and file API methods
- retry-aware request handling
- resumable upload helpers
- browser and Node-compatible blob uploads

## Install

```bash
npm install @floehq/sdk
```

## Basic Usage

```ts
import { FloeClient } from "@floehq/sdk";

const floe = new FloeClient({
  baseUrl: "http://127.0.0.1:3001/v1",
  auth: {
    apiKey: process.env.FLOE_API_KEY,
  },
});

const result = await floe.uploadBytes(new TextEncoder().encode("hello"), {
  filename: "hello.txt",
  contentType: "text/plain",
});

console.log(result.fileId);
```

## Design Notes

- The SDK wraps the current Floe API contract instead of inventing a parallel abstraction.
- `uploadBlob()` and `uploadBytes()` are the main high-level helpers.
- CLI tooling should be built on top of this package rather than reimplementing transport logic.
