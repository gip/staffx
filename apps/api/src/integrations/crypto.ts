import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";

const ALGORITHM = "aes-256-gcm";
const IV_LENGTH = 12;
let cachedKey: Buffer | null = null;

function decodeIntegrationKey(raw: string): Buffer {
  const base64 = Buffer.from(raw, "base64");
  if (base64.length === 32) return base64;

  if (/^[0-9a-f]{64}$/i.test(raw)) {
    const hex = Buffer.from(raw, "hex");
    if (hex.length === 32) return hex;
  }

  if (raw.length === 32) {
    const ascii = Buffer.from(raw, "utf8");
    if (ascii.length === 32) return ascii;
  }

  throw new Error("INTEGRATION_ENCRYPTION_KEY must decode to 32 bytes");
}

function resolveEncryptionKey(): Buffer {
  if (cachedKey) return cachedKey;

  const raw = process.env.INTEGRATION_ENCRYPTION_KEY;
  if (!raw) {
    throw new Error("INTEGRATION_ENCRYPTION_KEY is required");
  }

  const key = decodeIntegrationKey(raw);
  cachedKey = key;
  return key;
}

export function encryptToken(clearText: string): string {
  const key = resolveEncryptionKey();
  const iv = randomBytes(IV_LENGTH);
  const cipher = createCipheriv(ALGORITHM, key, iv);
  const encrypted = Buffer.concat([cipher.update(clearText, "utf8"), cipher.final()]);
  const tag = cipher.getAuthTag();
  return `${iv.toString("base64")}.${tag.toString("base64")}.${encrypted.toString("base64")}`;
}

export function decryptToken(value: string): string {
  const key = resolveEncryptionKey();
  const [ivEncoded, tagEncoded, cipherEncoded] = value.split(".");
  if (!ivEncoded || !tagEncoded || !cipherEncoded) {
    throw new Error("Invalid encrypted token format");
  }

  const iv = Buffer.from(ivEncoded, "base64");
  const tag = Buffer.from(tagEncoded, "base64");
  const cipherText = Buffer.from(cipherEncoded, "base64");

  const decipher = createDecipheriv(ALGORITHM, key, iv);
  decipher.setAuthTag(tag);
  const clearText = Buffer.concat([decipher.update(cipherText), decipher.final()]);
  return clearText.toString("utf8");
}
