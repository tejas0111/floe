import { SuiClient } from "@mysten/sui/client";
import { Ed25519Keypair } from "@mysten/sui/keypairs/ed25519";
import { decodeSuiPrivateKey } from "@mysten/sui/cryptography";

const RPC_URL = process.env.SUI_RPC_URL;
const PRIVATE_KEY = process.env.SUI_PRIVATE_KEY;
const FLOE_NETWORK = process.env.FLOE_NETWORK;

if (!RPC_URL) {
  throw new Error("SUI_RPC_URL is not set");
}

if (!PRIVATE_KEY) {
  throw new Error("SUI_PRIVATE_KEY is not set");
}

if (!FLOE_NETWORK) {
  throw new Error("FLOE_NETWORK is not set");
}

if (
  FLOE_NETWORK === "testnet" &&
  RPC_URL.includes("mainnet")
) {
  throw new Error(
    "NETWORK_MISMATCH: testnet Floe cannot use mainnet Sui RPC"
  );
}

export const suiClient = new SuiClient({
  url: RPC_URL,
});

function createSignerFromEnv(key: string): Ed25519Keypair {
  if (key.startsWith("suiprivkey")) {
    const decoded = decodeSuiPrivateKey(key);

    if (decoded.schema !== "ED25519") {
      throw new Error(`Unsupported key schema: ${decoded.schema}`);
    }

    return Ed25519Keypair.fromSecretKey(decoded.secretKey);
  }

  const raw = Buffer.from(key, "base64");
  return Ed25519Keypair.fromSecretKey(raw);
}

export const suiSigner = createSignerFromEnv(PRIVATE_KEY);
