// src/services/upload/walrus.upload.ts

import type { Readable } from "stream";
import { Ed25519Keypair } from "@mysten/sui/keypairs/ed25519";
import { toB64 } from "@mysten/sui/utils";
import { nodeToWeb } from "../../utils/nodeToWeb.js";
import { WalrusEnv } from "../../config/walrus.config.js";
import { suiClient, suiNetwork, suiSigner } from "../../sui/client.js";

const MIN_BALANCE_MIST = 1_000_000_000n;
const FETCH_TIMEOUT_MS = 5 * 60 * 1000; // 5 min

const IS_MAINNET = suiNetwork === "mainnet";

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

let lastBalanceCheck = 0;
async function checkBalanceOnce(clientAddress: string) {
  const now = Date.now();
  if (now - lastBalanceCheck < 60_000) return;

  const bal = await suiClient.getBalance({ owner: clientAddress });
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
    const keypair = suiSigner;

    await checkBalanceOnce(keypair.getPublicKey().toSuiAddress());

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
