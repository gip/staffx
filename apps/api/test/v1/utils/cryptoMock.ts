import { vi } from "vitest";

const encryptedByPlainText = new Map<string, string>();
const plaintextByEncrypted = new Map<string, string>();

export const encryptTokenMock = vi.fn((value: string): string => {
  const encoded = `enc::${value}`;
  encryptedByPlainText.set(value, encoded);
  plaintextByEncrypted.set(encoded, value);
  return encoded;
});

export const decryptTokenMock = vi.fn((value: string): string => {
  return plaintextByEncrypted.get(value) ?? value.replace(/^enc::/, "");
});

export function getEncryptionStore() {
  return {
    encryptedByPlainText: new Map(encryptedByPlainText),
    plaintextByEncrypted: new Map(plaintextByEncrypted),
  };
}

export function resetCryptoMocks() {
  encryptTokenMock.mockClear();
  decryptTokenMock.mockClear();
  encryptedByPlainText.clear();
  plaintextByEncrypted.clear();
}

export function createCryptoMockModule() {
  return {
    encryptToken: encryptTokenMock,
    decryptToken: decryptTokenMock,
  };
}
