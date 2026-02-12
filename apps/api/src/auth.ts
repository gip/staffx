import { createRemoteJWKSet, jwtVerify, type JWTPayload } from "jose";
import type { FastifyRequest, FastifyReply } from "fastify";
import { query } from "./db.js";

interface Auth0Payload extends JWTPayload {
  sub: string;
}

interface UserRow {
  id: string;
  auth0_id: string;
  email: string | null;
  name: string | null;
  picture: string | null;
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
  githubHandle: string | null;
  createdAt: Date;
  updatedAt: Date;
}

declare module "fastify" {
  interface FastifyRequest {
    auth: AuthUser;
  }
}

function mapRow(row: UserRow): AuthUser {
  return {
    id: row.id,
    auth0Id: row.auth0_id,
    email: row.email,
    name: row.name,
    picture: row.picture,
    githubHandle: row.github_handle,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
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

async function findOrCreateUser(domain: string, payload: Auth0Payload, token: string): Promise<AuthUser> {
  const existing = await query<UserRow>("SELECT * FROM users WHERE auth0_id = $1", [payload.sub]);

  if (existing.rows.length > 0) {
    return mapRow(existing.rows[0]);
  }

  const profile = await fetchUserProfile(domain, token);

  const result = await query<UserRow>(
    `INSERT INTO users (auth0_id, email, name, picture, github_handle, updated_at)
     VALUES ($1, $2, $3, $4, $5, now())
     ON CONFLICT (auth0_id) DO UPDATE SET
       email = EXCLUDED.email,
       name = EXCLUDED.name,
       picture = EXCLUDED.picture,
       github_handle = EXCLUDED.github_handle,
       updated_at = now()
     RETURNING *`,
    [payload.sub, profile.email ?? null, profile.name ?? null, profile.picture ?? null, profile.nickname ?? null],
  );

  return mapRow(result.rows[0]);
}

const AUTH0_DOMAIN = process.env.AUTH0_DOMAIN;
const AUTH0_AUDIENCE = process.env.AUTH0_AUDIENCE;

if (!AUTH0_DOMAIN || !AUTH0_AUDIENCE) {
  throw new Error("AUTH0_DOMAIN and AUTH0_AUDIENCE must be set");
}

const domain: string = AUTH0_DOMAIN;
const audience: string = AUTH0_AUDIENCE;
const issuer = `https://${domain}/`;
const jwks = createRemoteJWKSet(new URL(`${issuer}.well-known/jwks.json`));

export async function verifyAuth(req: FastifyRequest, reply: FastifyReply) {
  const header = req.headers.authorization;
  if (!header?.startsWith("Bearer ")) {
    return reply.code(401).send({ error: "Missing or invalid Authorization header" });
  }

  const token = header.slice(7);

  try {
    const { payload } = await jwtVerify(token, jwks, { issuer, audience });
    const auth0Payload = payload as Auth0Payload;
    req.auth = await findOrCreateUser(domain, auth0Payload, token);
  } catch (err) {
    req.log.warn({ err }, "JWT verification failed");
    return reply.code(401).send({ error: "Invalid token" });
  }
}
