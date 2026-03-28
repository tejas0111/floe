#!/usr/bin/env node

import process from "node:process";

function usage() {
  console.error(
    "Usage: npm run measure:stream -- <fileId> [--base-url http://localhost:3001] [--range bytes=0-1048575] [--runs 2]"
  );
}

function parseArgs(argv) {
  const args = [...argv];
  const out = {
    fileId: "",
    baseUrl: process.env.FLOE_MEASURE_BASE_URL ?? "http://localhost:3001",
    range: process.env.FLOE_MEASURE_RANGE ?? "bytes=0-1048575",
    runs: Number(process.env.FLOE_MEASURE_RUNS ?? 2),
  };

  while (args.length > 0) {
    const arg = args.shift();
    if (!arg) break;

    if (!out.fileId && !arg.startsWith("--")) {
      out.fileId = arg;
      continue;
    }
    if (arg === "--base-url") {
      out.baseUrl = String(args.shift() ?? "");
      continue;
    }
    if (arg === "--range") {
      out.range = String(args.shift() ?? "");
      continue;
    }
    if (arg === "--runs") {
      out.runs = Number(args.shift() ?? "");
      continue;
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  if (!out.fileId) throw new Error("Missing required fileId");
  if (!Number.isInteger(out.runs) || out.runs < 1) {
    throw new Error("--runs must be an integer >= 1");
  }
  if (!/^https?:\/\//.test(out.baseUrl)) {
    throw new Error("--base-url must start with http:// or https://");
  }

  return out;
}

function toMs(startNs, endNs) {
  return Number(endNs - startNs) / 1_000_000;
}

async function measureOne(url, headers) {
  const startedAt = process.hrtime.bigint();
  const res = await fetch(url, { headers });
  const responseAt = process.hrtime.bigint();

  if (!res.ok && res.status !== 206) {
    const body = await res.text().catch(() => "");
    throw new Error(
      `Request failed status=${res.status}${body ? ` body=${body.slice(0, 200)}` : ""}`
    );
  }

  let firstChunkAt = null;
  let bytesRead = 0;
  const reader = res.body?.getReader();
  if (!reader) {
    throw new Error("Response body missing");
  }

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    if (firstChunkAt === null) firstChunkAt = process.hrtime.bigint();
    bytesRead += value.byteLength;
  }

  const finishedAt = process.hrtime.bigint();

  return {
    status: res.status,
    contentLength: res.headers.get("content-length"),
    contentRange: res.headers.get("content-range"),
    ttfbMs: toMs(startedAt, firstChunkAt ?? responseAt),
    headersMs: toMs(startedAt, responseAt),
    totalMs: toMs(startedAt, finishedAt),
    bytesRead,
  };
}

function summarize(results) {
  const sortedTtfb = [...results].map((r) => r.ttfbMs).sort((a, b) => a - b);
  const sortedTotal = [...results].map((r) => r.totalMs).sort((a, b) => a - b);
  const pick = (arr, p) => arr[Math.min(arr.length - 1, Math.floor((arr.length - 1) * p))];

  return {
    p50TtfbMs: pick(sortedTtfb, 0.5),
    p95TtfbMs: pick(sortedTtfb, 0.95),
    p50TotalMs: pick(sortedTotal, 0.5),
    p95TotalMs: pick(sortedTotal, 0.95),
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const url = `${args.baseUrl.replace(/\/+$/, "")}/v1/files/${encodeURIComponent(args.fileId)}/stream`;
  const headers = { Range: args.range };

  const results = [];
  for (let i = 0; i < args.runs; i++) {
    const run = await measureOne(url, headers);
    results.push(run);
    console.log(
      JSON.stringify(
        {
          run: i + 1,
          range: args.range,
          cold: i === 0,
          ...run,
        },
        null,
        2
      )
    );
  }

  console.log(
    JSON.stringify(
      {
        fileId: args.fileId,
        baseUrl: args.baseUrl,
        range: args.range,
        runs: args.runs,
        summary: summarize(results),
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  usage();
  process.exit(1);
});
