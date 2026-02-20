import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["test/v1/**/*.test.ts"],
    restoreMocks: true,
    clearMocks: true,
  },
});
