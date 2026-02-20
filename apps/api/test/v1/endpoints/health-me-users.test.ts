import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { buildV1TestApp } from "../utils/app.js";
import * as authMock from "../utils/authMock.js";
import * as dbMock from "../utils/dbMock.js";
import { ACTIVE_USER, FIXTURE_NOW, PUBLIC_PROJECT, PRIVATE_PROJECT, VIEWER_USER, TEAM_OWNER_USER } from "../utils/fixtures.js";

vi.mock("../../../src/auth.js", async () => {
  const mocked = await import("../utils/authMock.js");
  return mocked.getAuthMockModule();
});
vi.mock("../../../src/db.js", async () => {
  const mocked = await import("../utils/dbMock.js");
  return mocked.createDbModuleMock();
});

const AUTH_TOKEN = "token-owner";
const VIEWER_TOKEN = "token-viewer";

async function buildApp() {
  const app = await buildV1TestApp();
  return app;
}

describe("/v1 health, me, users", () => {
  beforeEach(() => {
    authMock.setAuthToken(AUTH_TOKEN, ACTIVE_USER);
    authMock.setAuthToken(VIEWER_TOKEN, VIEWER_USER);
    authMock.resetAuthMocks();
    authMock.setAuthToken(AUTH_TOKEN, ACTIVE_USER);
    authMock.setAuthToken(VIEWER_TOKEN, VIEWER_USER);
    dbMock.resetDbMock();
    dbMock.clearQueryLog();
  });

  afterEach(async () => {
    authMock.clearAuthTokens();
    authMock.resetAuthMocks();
    dbMock.resetDbMock();
  });

  describe("GET /v1/health", () => {
    it("returns health payload when DB resolves", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("SELECT NOW() AS time")) {
          return dbMock.queryRows({ time: new Date(FIXTURE_NOW) });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildApp();
      const response = await app.inject({ method: "GET", url: "/v1/health" });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json()).toMatchObject({ status: "ok" });
      expect(typeof response.json().time).toBe("string");
    });

    it("returns 500 when DB throws", async () => {
      dbMock.setQueryHandler(async () => {
        throw new Error("db-down");
      });

      const app = await buildApp();
      const response = await app.inject({ method: "GET", url: "/v1/health" });
      await app.close();

      expect(response.statusCode).toBe(500);
    });
  });

  describe("GET /v1/me", () => {
    it("requires auth", async () => {
      const app = await buildApp();
      const response = await app.inject({ method: "GET", url: "/v1/me" });
      await app.close();

      expect(response.statusCode).toBe(401);
      expect(response.json()).toMatchObject({
        title: "Unauthorized",
        status: 401,
      });
    });

    it("returns authenticated user payload", async () => {
      const app = await buildApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/me",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json()).toMatchObject({
        id: ACTIVE_USER.id,
        handle: ACTIVE_USER.handle,
      });
    });
  });

  describe("GET /v1/users", () => {
    it("denies user search without auth", async () => {
      const app = await buildApp();
      const response = await app.inject({ method: "GET", url: "/v1/users/search?q=abc" });
      await app.close();
      expect(response.statusCode).toBe(401);
    });

    it("validates missing/blank search query", async () => {
      const app = await buildApp();
      const noQuery = await app.inject({
        method: "GET",
        url: "/v1/users/search",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      expect(noQuery.statusCode).toBe(400);
      expect(noQuery.json()).toEqual({ error: "q is required" });

      const blank = await app.inject({
        method: "GET",
        url: "/v1/users/search?q=   ",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      expect(blank.statusCode).toBe(400);
      expect(blank.json()).toEqual({ error: "q is required" });
      await app.close();
    });

    it("searches users with trimmed LIKE pattern and returns mapping", async () => {
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("SELECT handle, name, picture FROM users")) {
          expect(params?.[0]).toBe("%Al%");
          return dbMock.queryRows(
            { handle: "alice", name: "Alice", picture: "alice.png" },
            { handle: "alexa", name: "Alexa", picture: null },
          );
        }
        return dbMock.queryNoRows();
      });

      const app = await buildApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/users/search?q= Al ",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual([
        { handle: "alice", name: "Alice", picture: "alice.png" },
        { handle: "alexa", name: "Alexa", picture: null },
      ]);
    });
  });

  describe("GET /v1/users/:handle", () => {
    it("returns 404 when handle is unknown", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM users")) {
          return dbMock.queryNoRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/users/unknown",
      });
      await app.close();
      expect(response.statusCode).toBe(404);
      expect(response.json()).toEqual({ error: "User not found" });
    });

    it("hides private projects from unauthenticated viewers and shows public only", async () => {
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("FROM users")) {
          return dbMock.queryRows({
            id: TEAM_OWNER_USER.id,
            handle: TEAM_OWNER_USER.handle,
            name: TEAM_OWNER_USER.name,
            picture: TEAM_OWNER_USER.picture,
            github_handle: null,
            created_at: TEAM_OWNER_USER.createdAt,
          });
        }
        if (text.includes("FROM user_projects target_up")) {
          expect(params?.[0]).toBe(TEAM_OWNER_USER.id);
          expect(params?.[1]).toBe(null);
          return dbMock.queryRows({
            name: PUBLIC_PROJECT.name,
            description: PUBLIC_PROJECT.description,
            visibility: "public",
            owner_handle: TEAM_OWNER_USER.handle,
            role: "Owner",
            created_at: PUBLIC_PROJECT.created_at,
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildApp();
      const response = await app.inject({ method: "GET", url: `/v1/users/${TEAM_OWNER_USER.handle}` });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({
        handle: TEAM_OWNER_USER.handle,
        name: TEAM_OWNER_USER.name,
        picture: TEAM_OWNER_USER.picture,
        githubHandle: null,
        memberSince: TEAM_OWNER_USER.createdAt.toISOString(),
        projects: [
          {
            name: PUBLIC_PROJECT.name,
            description: PUBLIC_PROJECT.description,
            visibility: "public",
            ownerHandle: TEAM_OWNER_USER.handle,
            role: "Owner",
            createdAt: PUBLIC_PROJECT.created_at.toISOString(),
          },
        ],
      });
    });

    it("returns visible projects for viewer when authenticated", async () => {
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("FROM users")) {
          return dbMock.queryRows({
            id: TEAM_OWNER_USER.id,
            handle: TEAM_OWNER_USER.handle,
            name: TEAM_OWNER_USER.name,
            picture: TEAM_OWNER_USER.picture,
            github_handle: null,
            created_at: TEAM_OWNER_USER.createdAt,
          });
        }
        if (text.includes("FROM user_projects target_up")) {
          expect(params?.[1]).toBe(VIEWER_USER.id);
          return dbMock.queryRows(
            {
              name: PUBLIC_PROJECT.name,
              description: PUBLIC_PROJECT.description,
              visibility: "public",
              owner_handle: TEAM_OWNER_USER.handle,
              role: "Viewer",
              created_at: PUBLIC_PROJECT.created_at,
            },
            {
              name: PRIVATE_PROJECT.name,
              description: PRIVATE_PROJECT.description,
              visibility: "private",
              owner_handle: TEAM_OWNER_USER.handle,
              role: "Viewer",
              created_at: PRIVATE_PROJECT.created_at,
            },
          );
        }
        return dbMock.queryNoRows();
      });

      const app = await buildApp();
      const response = await app.inject({
        method: "GET",
        url: `/v1/users/${TEAM_OWNER_USER.handle.toUpperCase()}`,
        headers: { authorization: `Bearer ${VIEWER_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json().projects).toHaveLength(2);
      expect(response.json().projects[0].visibility).toBe("public");
    });

    it("trims handle path value before lookup", async () => {
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("FROM users")) {
          expect(params?.[0]).toBe(TEAM_OWNER_USER.handle);
          return dbMock.queryNoRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildApp();
      const response = await app.inject({
        method: "GET",
        url: `/v1/users/${encodeURIComponent(` ${TEAM_OWNER_USER.handle}  `)}`,
      });
      await app.close();

      expect(response.statusCode).toBe(404);
    });
  });
});
