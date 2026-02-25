import type { FastifyInstance } from "fastify";
import { handleMcpDelete, handleMcpGet, handleMcpPost } from "./server.js";
import { mcpResourceMetadata } from "./auth.js";

export async function mcpRoutes(app: FastifyInstance) {
  app.get("/.well-known/oauth-protected-resource", async (req) => {
    return mcpResourceMetadata(req);
  });

  app.get("/.well-known/oauth-protected-resource/mcp", async (req) => {
    return mcpResourceMetadata(req);
  });

  app.post("/mcp", async (req, reply) => {
    await handleMcpPost(app, req, reply);
  });

  app.get("/mcp", async (req, reply) => {
    await handleMcpGet(req, reply);
  });

  app.delete("/mcp", async (req, reply) => {
    await handleMcpDelete(req, reply);
  });
}
