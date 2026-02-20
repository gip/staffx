import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { vi } from "vitest";

let bundleDirSuffix = "openship-test-bundle";

const generateOpenShipFileBundleMock = vi.fn(async (_threadId: string, workspace: string): Promise<string> => {
  const path = join(workspace, bundleDirSuffix);
  await mkdir(path, { recursive: true });
  await writeFile(join(path, "README.md"), "# openship bundle", "utf8");
  await writeFile(join(path, "AGENTS.md"), "# Agent", "utf8");
  return path;
});

export function setOpenShipBundleDirSuffix(value: string) {
  bundleDirSuffix = value;
}

export function resetOpenShipMock() {
  bundleDirSuffix = "openship-test-bundle";
  generateOpenShipFileBundleMock.mockClear();
}

export function createOpenShipSyncMockModule() {
  return {
    generateOpenShipFileBundle: generateOpenShipFileBundleMock,
  };
}
