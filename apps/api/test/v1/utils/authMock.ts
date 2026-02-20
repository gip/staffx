import type { FastifyReply, FastifyRequest } from "fastify";
import { vi } from "vitest";

export interface MockAuthUser {
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

interface VerifyRequest extends FastifyRequest {
  auth?: MockAuthUser;
}

type TokenMap = Map<string, MockAuthUser>;

const tokenToUser: TokenMap = new Map<string, MockAuthUser>();

function tokenFromHeaders(headers: FastifyRequest["headers"]): string | null {
  const raw = headers.authorization;
  if (!raw || typeof raw !== "string") return null;
  if (!raw.startsWith("Bearer ")) return null;
  return raw.slice(7).trim();
}

function unauthorizedPayload(url: string) {
  return {
    type: "https://tools.ietf.org/html/rfc7807#section-3.1",
    title: "Unauthorized",
    status: 401,
    detail: "Missing or invalid Authorization header",
    instance: url,
  };
}

async function verifyAuth(req: VerifyRequest, reply: FastifyReply) {
  const token = tokenFromHeaders(req.headers);
  const user = token ? tokenToUser.get(token) : undefined;

  if (!user) {
    await reply.code(401).send(unauthorizedPayload(req.url));
    return;
  }

  req.auth = user;
}

async function verifyOptionalAuth(req: VerifyRequest, reply: FastifyReply) {
  const token = tokenFromHeaders(req.headers);
  const user = token ? tokenToUser.get(token) : undefined;

  if (user) {
    req.auth = user;
  }
}

export const verifyAuthMock = vi.fn(verifyAuth);
export const verifyOptionalAuthMock = vi.fn(verifyOptionalAuth);

export function setAuthToken(token: string, user: MockAuthUser) {
  tokenToUser.set(token, user);
}

export function clearAuthTokens() {
  tokenToUser.clear();
}

export function getMockAuthMap() {
  return tokenToUser;
}

export function resetAuthMocks() {
  verifyAuthMock.mockClear();
  verifyOptionalAuthMock.mockClear();
  clearAuthTokens();
}

export function getAuthMockModule() {
  return {
    verifyAuth: verifyAuthMock,
    verifyOptionalAuth: verifyOptionalAuthMock,
  };
}
