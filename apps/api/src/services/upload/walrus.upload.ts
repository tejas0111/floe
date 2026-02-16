// src/services/upload/walrus.upload.ts

import type { Readable } from "stream";
import { Ed25519Keypair } from "@mysten/sui/keypairs/ed25519";
import { fromB64, fromHEX, toB64 } from "@mysten/sui/utils";
import { SuiClient } from "@mysten/sui/client";
import { decodeSuiPrivateKey } from "@mysten/sui/cryptography";
import { nodeToWeb } from "../../utils/nodeToWeb.js";
import { WalrusEnv } from "../../config/walrus.config.js";

const MAINNET_RPC = "https://fullnode.mainnet.sui.io:443";
const TESTNET_RPC = "https://fullnode.testnet.sui.io:443";
const MIN_BALANCE_MIST = 1_000_000_000n;
const FETCH_TIMEOUT_MS = 5 * 60 * 1000; // 5 min


const NETWORK = (() => {
  const net = process.env.FLOE_NETWORK;
  if (net !== "mainnet" && net !== "testnet") {
    throw new Error("FLOE_NETWORK must be 'mainnet' or 'testnet'");
  }
  return net;
})();

const IS_MAINNET = NETWORK === "mainnet";
const SUI_RPC_URL = (() => {
  const configured = process.env.SUI_RPC_URL?.trim();
  const fallback = IS_MAINNET ? MAINNET_RPC : TESTNET_RPC;
  const url = configured || fallback;

  if (!/^https?:\/\//.test(url)) {
    throw new Error("SUI_RPC_URL must start with http:// or https://");
  }

  if (IS_MAINNET && /testnet/i.test(url)) {
    throw new Error("NETWORK_MISMATCH: mainnet Floe cannot use testnet Sui RPC");
  }
  if (!IS_MAINNET && /mainnet/i.test(url)) {
    throw new Error("NETWORK_MISMATCH: testnet Floe cannot use mainnet Sui RPC");
  }

  return url;
})();

const WALRUS_PUBLISHER_URL = (() => {
  const url = WalrusEnv.publisherUrl;
  if (!url || !url.startsWith("http")) {
    throw new Error("WALRUS_PUBLISHER_URL is missing or invalid");
  }
  return url.replace(/\/$/, "");
})();

if (IS_MAINNET && /testnet/i.test(WALRUS_PUBLISHER_URL)) {
  console.warn(
    "WALRUS_PUBLISHER_URL looks like testnet but FLOE_NETWORK=mainnet; verify your env"
  );
}
if (!IS_MAINNET && /mainnet/i.test(WALRUS_PUBLISHER_URL)) {
  console.warn(
    "WALRUS_PUBLISHER_URL looks like mainnet but FLOE_NETWORK=testnet; verify your env"
  );
}

let cachedKeypair: Ed25519Keypair | null = null;
let lastBalanceCheck = 0;

function loadWalletKeypair(): Ed25519Keypair {
  if (cachedKeypair) return cachedKeypair;

  const raw = process.env.SUI_PRIVATE_KEY;
  if (!raw) throw new Error("SUI_PRIVATE_KEY not set");

  const key = raw.trim();

  if (key.startsWith("suiprivkey1")) {
    const { schema, secretKey } = decodeSuiPrivateKey(key);
    if (schema !== "ED25519") {
      throw new Error(`Unsupported key schema: ${schema}`);
    }
    cachedKeypair = Ed25519Keypair.fromSecretKey(secretKey);
    return cachedKeypair;
  }

  if (key.startsWith("[")) {
    try {
      const arr = JSON.parse(key);
      cachedKeypair = Ed25519Keypair.fromSecretKey(
        Uint8Array.from(arr).slice(0, 32)
      );
      return cachedKeypair;
    } catch (err: any) {
      throw new Error(
        `Invalid JSON array SUI_PRIVATE_KEY: ${err?.message ?? "parse error"}`
      );
    }
  }

  if (/^[A-Za-z0-9+/]+=*$/.test(key)) {
    try {
      cachedKeypair = Ed25519Keypair.fromSecretKey(fromB64(key));
      return cachedKeypair;
    } catch (err: any) {
      throw new Error(
        `Invalid base64 SUI_PRIVATE_KEY: ${err?.message ?? "decode error"}`
      );
    }
  }

  if (/^(0x)?[0-9a-fA-F]+$/.test(key)) {
    try {
      cachedKeypair = Ed25519Keypair.fromSecretKey(
        fromHEX(key.replace(/^0x/, "")).slice(0, 32)
      );
      return cachedKeypair;
    } catch (err: any) {
      throw new Error(
        `Invalid hex SUI_PRIVATE_KEY: ${err?.message ?? "decode error"}`
      );
    }
  }

  throw new Error("Unrecognized SUI_PRIVATE_KEY format");
}

async function checkBalanceOnce(
  client: SuiClient,
  address: string
) {
  const now = Date.now();
  if (now - lastBalanceCheck < 60_000) return;

  const bal = await client.getBalance({ owner: address });
  if (BigInt(bal.totalBalance) < MIN_BALANCE_MIST) {
    throw new Error("INSUFFICIENT_BALANCE");
  }

  lastBalanceCheck = now;
}

async function createAuthHeaders(
  keypair: Ed25519Keypair
): Promise<Record<string, string>> {
  const address = keypair.getPublicKey().toSuiAddress();
  const timestamp = Date.now();
  const msg = `${WALRUS_PUBLISHER_URL}:${address}:${timestamp}`;

  const sig = await keypair.signPersonalMessage(
    new TextEncoder().encode(msg)
  );

  return {
    "X-Sui-Address": address,
    "X-Sui-Timestamp": String(timestamp),
    "X-Sui-Signature": toB64(Uint8Array.from(sig.signature)),
  };
}

export async function uploadToWalrusOnce(
  streamFactory: () => Readable,
  epochs: number
): Promise<{
  blobId: string;
  objectId?: string;
  cost?: number;
  endEpoch?: number;
}> {
  if (!Number.isInteger(epochs) || epochs <= 0) {
    throw new Error("INVALID_EPOCHS");
  }

  const params = new URLSearchParams({ epochs: String(epochs) });
  const headers: Record<string, string> = {
    "Content-Type": "application/octet-stream",
  };

  if (IS_MAINNET) {
    const keypair = loadWalletKeypair();
    const client = new SuiClient({ url: SUI_RPC_URL });

    await checkBalanceOnce(
      client,
      keypair.getPublicKey().toSuiAddress()
    );

    Object.assign(headers, await createAuthHeaders(keypair));
  }

  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    FETCH_TIMEOUT_MS
  );

  try {
    const res = await fetch(
      `${WALRUS_PUBLISHER_URL}/v1/blobs?${params.toString()}`,
      {
        method: "PUT",
        headers,
        body: nodeToWeb(streamFactory()),
        duplex: "half",
        signal: controller.signal,
      }
    );

    if (!res.ok) {
      const text = await safeReadText(res);
      throw new Error(`WALRUS_UPLOAD_FAILED:${res.status}:${text}`);
    }

    const json = (await res.json()) as any;
    const blobId =
      json?.newlyCreated?.blobObject?.blobId ??
      json?.alreadyCertified?.blobId ??
      json?.blobObject?.blobId;

    if (!blobId) throw new Error("WALRUS_MISSING_BLOB_ID");

    return {
      blobId,
      objectId: json?.newlyCreated?.blobObject?.id,
      cost: json?.newlyCreated?.cost,
      endEpoch:
        json?.newlyCreated?.blobObject?.storage?.endEpoch ??
        json?.alreadyCertified?.endEpoch,
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function safeReadText(res: Response): Promise<string> {
  try {
    return await res.text();
  } catch {
    return "";
  }
}
