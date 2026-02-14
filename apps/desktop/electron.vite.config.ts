import { defineConfig } from "electron-vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  main: {
    envPrefix: "VITE_",
    build: {
      outDir: "out/main",
      rollupOptions: {
        external: ["@anthropic-ai/claude-agent-sdk"],
      },
    },
  },
  preload: {
    build: {
      outDir: "out/preload",
      lib: {
        entry: "src/preload/index.ts",
        formats: ["cjs"],
      },
      rollupOptions: {
        output: {
          entryFileNames: "index.js",
        },
      },
    },
  },
  renderer: {
    root: "src/renderer",
    plugins: [react()],
    build: {
      outDir: "../../out/renderer",
    },
  },
});
