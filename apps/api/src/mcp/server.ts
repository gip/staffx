import { randomUUID } from "node:crypto";
import type { FastifyInstance, FastifyReply, FastifyRequest } from "fastify";
import { callToolViaV1, readResourceViaV1, AdapterHttpError } from "./adapters.js";
import { MCP_RESOURCE_DEFINITIONS, MCP_TOOL_DEFINITIONS, getToolDefinition } from "./catalog.js";
import { authenticateMcpRequest, enforceRequiredScopes } from "./auth.js";
import { requiredScopesForResourceUri, requiredScopesForTool } from "./scopes.js";

type JsonRpcId = string | number | null;

interface JsonRpcRequest {
  jsonrpc?: string;
  id?: JsonRpcId;
  method?: string;
  params?: unknown;
}

interface McpSession {
  id: string;
  userId: string;
  createdAt: number;
  expiresAt: number;
}

const PROTOCOL_VERSION = "2025-11-05";
const SERVER_NAME = "staffx-mcp";
const SERVER_VERSION = "0.1.0";
const SESSION_TTL_MS = Number.parseInt(process.env.STAFFX_MCP_SESSION_TTL_MS ?? "3600000", 10);

const sessions = new Map<string, McpSession>();

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function normalizeJsonRpcId(value: unknown): JsonRpcId | undefined {
  if (typeof value === "string" || typeof value === "number" || value === null) return value;
  return undefined;
}

function mcpSuccess(id: JsonRpcId | undefined, result: unknown) {
  return {
    jsonrpc: "2.0" as const,
    ...(typeof id === "undefined" ? {} : { id }),
    result,
  };
}

function mcpError(id: JsonRpcId | undefined, code: number, message: string, data?: unknown) {
  return {
    jsonrpc: "2.0" as const,
    ...(typeof id === "undefined" ? { id: null } : { id }),
    error: {
      code,
      message,
      ...(typeof data === "undefined" ? {} : { data }),
    },
  };
}

function parseMethodParams<T extends Record<string, unknown>>(params: unknown): T {
  return (isRecord(params) ? params : {}) as T;
}

function cleanupExpiredSessions(now = Date.now()) {
  for (const [sessionId, session] of sessions) {
    if (session.expiresAt <= now) sessions.delete(sessionId);
  }
}

function getSessionId(req: FastifyRequest): string | null {
  const rawHeader = req.headers["mcp-session-id"];
  if (!rawHeader) return null;
  const first = Array.isArray(rawHeader) ? rawHeader[0] : rawHeader;
  if (!first) return null;
  const trimmed = first.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function ensureSession(req: FastifyRequest, reply: FastifyReply): McpSession | null {
  const sessionId = getSessionId(req);
  if (!sessionId) return null;

  cleanupExpiredSessions();
  const session = sessions.get(sessionId) ?? null;
  if (!session) {
    reply.code(400).send(mcpError(normalizeJsonRpcId((req.body as JsonRpcRequest | undefined)?.id), -32002, "Invalid MCP session"));
    return null;
  }
  return session;
}

function makeToolResult(payload: unknown, isError = false) {
  const text = typeof payload === "string" ? payload : JSON.stringify(payload, null, 2);
  return {
    content: [{ type: "text", text }],
    structuredContent: payload,
    ...(isError ? { isError: true } : {}),
  };
}

async function handleToolCall(
  app: FastifyInstance,
  token: string,
  name: string,
  args: unknown,
): Promise<unknown> {
  const result = await callToolViaV1(app, token, name, args);
  return makeToolResult(result);
}

async function handleResourceRead(
  app: FastifyInstance,
  token: string,
  uri: string,
): Promise<unknown> {
  const payload = await readResourceViaV1(app, token, uri);
  return {
    contents: [{
      uri,
      mimeType: "application/json",
      text: JSON.stringify(payload, null, 2),
    }],
  };
}

export async function handleMcpPost(app: FastifyInstance, req: FastifyRequest, reply: FastifyReply) {
  cleanupExpiredSessions();
  const auth = await authenticateMcpRequest(req, reply);
  if (!auth) return;

  const raw = req.body;
  if (!isRecord(raw)) {
    return reply.code(400).send(mcpError(undefined, -32600, "Invalid Request"));
  }

  const request = raw as JsonRpcRequest;
  const id = normalizeJsonRpcId(request.id);

  if (request.jsonrpc !== "2.0" || typeof request.method !== "string") {
    return reply.code(400).send(mcpError(id, -32600, "Invalid Request"));
  }

  if (request.method !== "initialize") {
    const hasSessionHeader = Boolean(getSessionId(req));
    if (hasSessionHeader && ensureSession(req, reply) === null && reply.sent) return;
  }

  if (request.method === "initialize") {
    const sessionId = randomUUID();
    const now = Date.now();
    sessions.set(sessionId, {
      id: sessionId,
      userId: auth.user.id,
      createdAt: now,
      expiresAt: now + Math.max(SESSION_TTL_MS, 60_000),
    });

    reply.header("MCP-Session-Id", sessionId);
    return reply.code(200).send(mcpSuccess(id, {
      protocolVersion: PROTOCOL_VERSION,
      capabilities: {
        tools: { listChanged: false },
        resources: { listChanged: false, subscribe: false },
      },
      serverInfo: {
        name: SERVER_NAME,
        version: SERVER_VERSION,
      },
    }));
  }

  if (request.method === "notifications/initialized") {
    return reply.code(204).send();
  }

  if (request.method === "ping") {
    return reply.code(200).send(mcpSuccess(id, {}));
  }

  if (request.method === "tools/list") {
    return reply.code(200).send(mcpSuccess(id, { tools: MCP_TOOL_DEFINITIONS }));
  }

  if (request.method === "resources/list") {
    return reply.code(200).send(mcpSuccess(id, { resources: MCP_RESOURCE_DEFINITIONS }));
  }

  if (request.method === "tools/call") {
    const params = parseMethodParams<{ name?: unknown; arguments?: unknown }>(request.params);
    const name = typeof params.name === "string" ? params.name : "";
    if (!name || !getToolDefinition(name)) {
      return reply.code(400).send(mcpError(id, -32602, "Invalid params", { detail: "Unknown tool name" }));
    }

    const requiredScopes = requiredScopesForTool(name);
    if (!enforceRequiredScopes(req, reply, auth, requiredScopes)) return;

    try {
      const result = await handleToolCall(app, auth.token, name, params.arguments);
      return reply.code(200).send(mcpSuccess(id, result));
    } catch (error) {
      if (error instanceof AdapterHttpError) {
        return reply.code(error.statusCode).send(mcpError(id, -32000, "Tool execution failed", {
          statusCode: error.statusCode,
          body: error.body,
        }));
      }
      req.log.error({ err: error, tool: name }, "Unhandled MCP tool error");
      return reply.code(500).send(mcpError(id, -32000, "Tool execution failed"));
    }
  }

  if (request.method === "resources/read") {
    const params = parseMethodParams<{ uri?: unknown }>(request.params);
    const uri = typeof params.uri === "string" ? params.uri : "";
    if (!uri) {
      return reply.code(400).send(mcpError(id, -32602, "Invalid params", { detail: "uri is required" }));
    }

    const requiredScopes = requiredScopesForResourceUri(uri);
    if (!enforceRequiredScopes(req, reply, auth, requiredScopes)) return;

    try {
      const result = await handleResourceRead(app, auth.token, uri);
      return reply.code(200).send(mcpSuccess(id, result));
    } catch (error) {
      if (error instanceof AdapterHttpError) {
        return reply.code(error.statusCode).send(mcpError(id, -32000, "Resource read failed", {
          statusCode: error.statusCode,
          body: error.body,
        }));
      }
      req.log.error({ err: error, uri }, "Unhandled MCP resource read error");
      return reply.code(500).send(mcpError(id, -32000, "Resource read failed"));
    }
  }

  return reply.code(404).send(mcpError(id, -32601, "Method not found"));
}

export async function handleMcpGet(req: FastifyRequest, reply: FastifyReply) {
  cleanupExpiredSessions();
  const auth = await authenticateMcpRequest(req, reply);
  if (!auth) return;

  const acceptHeader = Array.isArray(req.headers.accept) ? req.headers.accept[0] : req.headers.accept;
  if (!acceptHeader?.includes("text/event-stream")) {
    const sessionId = getSessionId(req);
    return reply.code(200).send({
      ok: true,
      sessionId,
      userId: auth.user.id,
      message: "Set Accept: text/event-stream for MCP streaming.",
    });
  }

  reply.raw.setHeader("Content-Type", "text/event-stream");
  reply.raw.setHeader("Cache-Control", "no-cache");
  reply.raw.setHeader("Connection", "keep-alive");
  reply.raw.write("event: ready\n");
  reply.raw.write(`data: ${JSON.stringify({ ok: true })}\n\n`);

  let closed = false;
  const heartbeat = setInterval(() => {
    if (closed) return;
    reply.raw.write(": heartbeat\n\n");
  }, 15000);

  reply.raw.on("close", () => {
    closed = true;
    clearInterval(heartbeat);
  });

  while (!closed) {
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  clearInterval(heartbeat);
  reply.raw.end();
}

export async function handleMcpDelete(req: FastifyRequest, reply: FastifyReply) {
  const auth = await authenticateMcpRequest(req, reply);
  if (!auth) return;

  const sessionIdFromHeader = getSessionId(req);
  const sessionIdFromQuery = isRecord(req.query) && typeof req.query.sessionId === "string"
    ? req.query.sessionId
    : null;
  const sessionId = sessionIdFromHeader ?? sessionIdFromQuery;

  if (!sessionId) {
    return reply.code(400).send({
      error: "sessionId is required (MCP-Session-Id header or query parameter).",
    });
  }

  const session = sessions.get(sessionId);
  if (!session || session.userId !== auth.user.id) {
    return reply.code(404).send({ error: "Session not found." });
  }

  sessions.delete(sessionId);
  reply.code(204).send();
}
