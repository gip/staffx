import { createRemoteJWKSet, jwtVerify, type JWTPayload } from "jose";
import type { FastifyReply, FastifyRequest } from "fastify";
import { colors, uniqueNamesGenerator, animals } from "unique-names-generator";
import { randomUUID } from "node:crypto";
import { query } from "./db.js";

interface Auth0Payload extends JWTPayload {
  sub: string;
  scope?: string;
  email?: string;
  name?: string;
  picture?: string;
  nickname?: string;
  orgId?: string;
  org_id?: string;
  organization?: string;
}

interface UserRow {
  id: string;
  auth0_id: string;
  email: string | null;
  name: string | null;
  picture: string | null;
  handle: string;
  github_handle: string | null;
  created_at: Date;
  updated_at: Date;
}

export interface AuthUser {
  id: string;
  auth0Id: string;
  email: string | null;
  name: string | null;
  picture: string | null;
  handle: string;
  githubHandle: string | null;
  orgId: string | null;
  scope: string | null;
  createdAt: Date;
  updatedAt: Date;
}

declare module "fastify" {
  interface FastifyRequest {
    auth: AuthUser;
  }
}

export interface AuthenticatedTokenContext {
  token: string;
  payload: Auth0Payload;
  user: AuthUser;
  scopes: Set<string>;
}

function withProblem(
  reply: FastifyReply,
  status: number,
  title: string,
  detail: string,
  instance?: string,
) {
  reply.code(status).type("application/problem+json").send({
    type: "https://tools.ietf.org/html/rfc7807#section-3.1",
    title,
    status,
    detail,
    instance,
  });
}

function extractOrgId(payload: Auth0Payload): string | null {
  const candidates: Array<unknown> = [
    payload.orgId,
    payload.org_id,
    payload.organization,
    (payload as { [key: string]: unknown })["https://staffx.io/org_id"],
    (payload as { [key: string]: unknown })["https://staffx.io/organization"],
  ];

  const first = candidates.find((value) => typeof value === "string");
  if (typeof first !== "string") return null;

  const normalized = first.trim();
  return normalized.length > 0 ? normalized : null;
}

function mapRow(row: UserRow, scope: string | null, orgId: string | null): AuthUser {
  return {
    id: row.id,
    auth0Id: row.auth0_id,
    email: row.email,
    name: row.name,
    picture: row.picture,
    handle: row.handle,
    githubHandle: row.github_handle,
    orgId,
    scope,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function generateHandle(): string {
  return uniqueNamesGenerator({ dictionaries: [colors, animals], separator: "-" });
}

function toScope(raw: string | undefined): string | null {
  if (!raw) return null;
  const trimmed = raw.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function deriveOrgId(orgId: string | null, fallbackUserOrg: string | null): string {
  return orgId ?? fallbackUserOrg ?? randomUUID();
}

async function fetchUserProfile(
  domain: string,
  accessToken: string,
): Promise<{ email?: string; name?: string; picture?: string; nickname?: string }> {
  const res = await fetch(`https://${domain}/userinfo`, {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
  if (!res.ok) throw new Error(`Failed to fetch userinfo: ${res.status}`);
  return res.json() as Promise<{ email?: string; name?: string; picture?: string; nickname?: string }>;
}

function fallbackProfileFromPayload(payload: Auth0Payload): {
  email?: string;
  name?: string;
  picture?: string;
  nickname?: string;
} {
  return {
    email: typeof payload.email === "string" ? payload.email : undefined,
    name: typeof payload.name === "string" ? payload.name : undefined,
    picture: typeof payload.picture === "string" ? payload.picture : undefined,
    nickname: typeof payload.nickname === "string" ? payload.nickname : undefined,
  };
}

async function findOrCreateUser(domain: string, payload: Auth0Payload, token: string): Promise<AuthUser> {
  const orgId = extractOrgId(payload);
  const requestedScope = toScope(payload.scope);

  const existing = await query<UserRow>("SELECT * FROM users WHERE auth0_id = $1", [payload.sub]);
  if (existing.rows.length > 0) {
    return mapRow(existing.rows[0], requestedScope, orgId ?? null);
  }

  const fallbackProfile = fallbackProfileFromPayload(payload);
  const profile = await fetchUserProfile(domain, token)
    .then((value) => ({ ...fallbackProfile, ...value }))
    .catch(() => fallbackProfile);

  const isGitHub = payload.sub.startsWith("github|");
  const githubHandle = isGitHub ? (profile.nickname ?? null) : null;

  const maxAttempts = 3;
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const handle = isGitHub && profile.nickname ? profile.nickname : generateHandle();

    try {
      const result = await query<UserRow>(
        `INSERT INTO users (id, auth0_id, email, name, picture, handle, github_handle, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, now())
         ON CONFLICT (auth0_id) DO UPDATE SET
           email = EXCLUDED.email,
           name = EXCLUDED.name,
           picture = EXCLUDED.picture,
           github_handle = EXCLUDED.github_handle,
           updated_at = now()
         RETURNING *`,
        [
          randomUUID(),
          payload.sub,
          profile.email ?? null,
          profile.name ?? null,
          profile.picture ?? null,
          handle,
          githubHandle,
        ],
      );

      return mapRow(result.rows[0], requestedScope, orgId ?? null);
    } catch (err: unknown) {
      const isHandleConflict =
        err instanceof Error &&
        "code" in err &&
        (err as { code: string }).code === "23505" &&
        "constraint" in err &&
        (err as { constraint: string }).constraint === "users_handle_key";

      if (!isHandleConflict || attempt === maxAttempts - 1) throw err;
    }
  }

  throw new Error("Failed to create user after handle collision retries");
}

const AUTH0_DOMAIN = process.env.AUTH0_DOMAIN;
const AUTH0_AUDIENCE = process.env.AUTH0_AUDIENCE;
const AUTH0_MCP_AUDIENCE = process.env.AUTH0_MCP_AUDIENCE?.trim() || null;

if (!AUTH0_DOMAIN || !AUTH0_AUDIENCE) {
  throw new Error("AUTH0_DOMAIN and AUTH0_AUDIENCE must be set");
}

const domain: string = AUTH0_DOMAIN;
const audience: string = AUTH0_AUDIENCE;
const issuer = `https://${domain}/`;
const jwks = createRemoteJWKSet(new URL(`${issuer}.well-known/jwks.json`));

function normalizeAudiences(value: string | string[] | undefined): string | string[] {
  if (!value) return audience;
  if (Array.isArray(value)) {
    const filtered = value.map((entry) => entry.trim()).filter((entry) => entry.length > 0);
    return filtered.length <= 1 ? (filtered[0] ?? audience) : filtered;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : audience;
}

class AuthTokenVerificationError extends Error {}
class AuthUserResolutionError extends Error {}

export function parseBearerToken(headerValue: string | string[] | undefined): string | null {
  if (!headerValue) return null;
  const rawHeader = Array.isArray(headerValue) ? headerValue[0] : headerValue;
  if (!rawHeader || !rawHeader.startsWith("Bearer ")) return null;
  const token = rawHeader.slice(7).trim();
  return token.length > 0 ? token : null;
}

export function parseScopeSet(scope: string | null | undefined): Set<string> {
  if (!scope) return new Set();
  return new Set(
    scope
      .split(/\s+/g)
      .map((entry) => entry.trim())
      .filter((entry) => entry.length > 0),
  );
}

export function listMissingScopes(scopes: Iterable<string>, requiredScopes: readonly string[]): string[] {
  const existing = new Set(scopes);
  return requiredScopes.filter((required) => !existing.has(required));
}

export function getMcpAudiences(): string[] {
  const values = [AUTH0_MCP_AUDIENCE, audience]
    .map((entry) => entry?.trim())
    .filter((entry): entry is string => Boolean(entry));
  return Array.from(new Set(values));
}

async function verifyAuthToken(
  token: string,
  options?: { audience?: string | string[] },
): Promise<Auth0Payload> {
  try {
    const { payload } = await jwtVerify(token, jwks, { issuer, audience: normalizeAudiences(options?.audience) });
    const auth0Payload = payload as Auth0Payload;
    if (!auth0Payload.sub || typeof auth0Payload.sub !== "string") {
      throw new AuthTokenVerificationError("Missing subject claim");
    }
    return auth0Payload;
  } catch (error) {
    throw new AuthTokenVerificationError(error instanceof Error ? error.message : "Invalid token");
  }
}

export async function authenticateBearerToken(
  token: string,
  options?: { audience?: string | string[] },
): Promise<AuthenticatedTokenContext> {
  const payload = await verifyAuthToken(token, options);
  try {
    const user = await findOrCreateUser(domain, payload, token);
    return {
      token,
      payload,
      user,
      scopes: parseScopeSet(user.scope),
    };
  } catch (error) {
    throw new AuthUserResolutionError(error instanceof Error ? error.message : "User lookup failed");
  }
}

async function authenticateRequest(
  req: FastifyRequest,
  reply: FastifyReply,
  options: { required: boolean; audience?: string | string[] },
): Promise<AuthUser | null> {
  const token = parseBearerToken(req.headers.authorization);
  if (!token) {
    if (options.required) {
      withProblem(reply, 401, "Unauthorized", "Missing or invalid Authorization header", req.url);
    }
    return null;
  }
  try {
    const context = await authenticateBearerToken(token, { audience: options.audience });
    req.auth = context.user;
    return req.auth;
  } catch (err) {
    if (err instanceof AuthUserResolutionError) {
      req.log.error({ err }, "User lookup/creation failed");
      withProblem(reply, 500, "Internal Server Error", "User lookup or creation failed", req.url);
      return null;
    }
    req.log.warn({ err }, "JWT verification failed");
    withProblem(reply, 401, "Unauthorized", "Invalid bearer token", req.url);
    return null;
  }
}

export async function verifyAuth(req: FastifyRequest, reply: FastifyReply) {
  await authenticateRequest(req, reply, { required: true, audience });
}

export async function verifyOptionalAuth(req: FastifyRequest, reply: FastifyReply) {
  await authenticateRequest(req, reply, { required: false, audience });
}

export async function verifyMcpAuth(req: FastifyRequest, reply: FastifyReply) {
  await authenticateRequest(req, reply, { required: true, audience: getMcpAudiences() });
}
