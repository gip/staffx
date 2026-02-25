import type { FastifyReply, FastifyRequest } from "fastify";
import {
  authenticateBearerToken,
  getMcpAudiences,
  listMissingScopes,
  parseBearerToken,
  type AuthenticatedTokenContext,
} from "../auth.js";
import { MCP_SCOPES } from "./scopes.js";

interface BearerChallengeOptions {
  error?: "invalid_token" | "insufficient_scope";
  errorDescription?: string;
  requiredScopes?: readonly string[];
}

function requestOrigin(req: FastifyRequest): string {
  const forwardedProto = req.headers["x-forwarded-proto"];
  const forwardedHost = req.headers["x-forwarded-host"];
  const protocol = (Array.isArray(forwardedProto) ? forwardedProto[0] : forwardedProto) ?? req.protocol;
  const host = (Array.isArray(forwardedHost) ? forwardedHost[0] : forwardedHost)
    ?? (Array.isArray(req.headers.host) ? req.headers.host[0] : req.headers.host)
    ?? "localhost:3001";
  return `${protocol}://${host}`;
}

function auth0Issuer(): string {
  const domain = process.env.AUTH0_DOMAIN?.trim();
  if (!domain) return "";
  return `https://${domain}/`;
}

export function mcpResourceMetadata(req: FastifyRequest) {
  const origin = requestOrigin(req);
  return {
    resource: `${origin}/mcp`,
    authorization_servers: auth0Issuer() ? [auth0Issuer()] : [],
    bearer_methods_supported: ["header"],
    scopes_supported: MCP_SCOPES,
    audiences_supported: getMcpAudiences(),
  };
}

function escapeHeaderValue(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

function bearerChallengeValue(req: FastifyRequest, options: BearerChallengeOptions): string {
  const metadataUrl = `${requestOrigin(req)}/.well-known/oauth-protected-resource/mcp`;
  const parts = [
    `realm="staffx-mcp"`,
    `resource_metadata="${escapeHeaderValue(metadataUrl)}"`,
  ];

  if (options.requiredScopes && options.requiredScopes.length > 0) {
    parts.push(`scope="${escapeHeaderValue(options.requiredScopes.join(" "))}"`);
  }
  if (options.error) {
    parts.push(`error="${options.error}"`);
  }
  if (options.errorDescription) {
    parts.push(`error_description="${escapeHeaderValue(options.errorDescription)}"`);
  }

  return `Bearer ${parts.join(", ")}`;
}

function writeAuthProblem(
  req: FastifyRequest,
  reply: FastifyReply,
  status: 401 | 403,
  title: string,
  detail: string,
  challenge: BearerChallengeOptions,
) {
  reply
    .header("WWW-Authenticate", bearerChallengeValue(req, challenge))
    .code(status)
    .type("application/problem+json")
    .send({
      type: "https://tools.ietf.org/html/rfc7807#section-3.1",
      title,
      status,
      detail,
      instance: req.url,
    });
}

export async function authenticateMcpRequest(
  req: FastifyRequest,
  reply: FastifyReply,
): Promise<AuthenticatedTokenContext | null> {
  const token = parseBearerToken(req.headers.authorization);
  if (!token) {
    writeAuthProblem(
      req,
      reply,
      401,
      "Unauthorized",
      "Missing or invalid Authorization header",
      {
        error: "invalid_token",
        errorDescription: "Missing bearer token",
      },
    );
    return null;
  }

  try {
    return await authenticateBearerToken(token, { audience: getMcpAudiences() });
  } catch (error) {
    req.log.warn({ err: error }, "MCP bearer token verification failed");
    writeAuthProblem(
      req,
      reply,
      401,
      "Unauthorized",
      "Invalid bearer token",
      {
        error: "invalid_token",
        errorDescription: "Token verification failed",
      },
    );
    return null;
  }
}

export function enforceRequiredScopes(
  req: FastifyRequest,
  reply: FastifyReply,
  auth: AuthenticatedTokenContext,
  requiredScopes: readonly string[],
): boolean {
  if (requiredScopes.length === 0) return true;

  const missing = listMissingScopes(auth.scopes, requiredScopes);
  if (missing.length === 0) return true;

  writeAuthProblem(
    req,
    reply,
    403,
    "Forbidden",
    `Missing required scope: ${missing.join(", ")}`,
    {
      error: "insufficient_scope",
      errorDescription: "Insufficient scope for requested MCP operation",
      requiredScopes: missing,
    },
  );

  return false;
}
