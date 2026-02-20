import { beforeEach, describe, expect, it, vi } from "vitest";
import { buildV1TestApp } from "../utils/app.js";
import * as authMock from "../utils/authMock.js";
import * as dbMock from "../utils/dbMock.js";
import { ACTIVE_USER } from "../utils/fixtures.js";

vi.mock("../../../src/auth.js", async () => {
  const mocked = await import("../utils/authMock.js");
  return mocked.getAuthMockModule();
});
vi.mock("../../../src/db.js", async () => {
  const mocked = await import("../utils/dbMock.js");
  return mocked.createDbModuleMock();
});

const AUTH_TOKEN = "token-owner";

const sampleProjectRows = [
  {
    id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    name: "Alpha",
    description: "Alpha project",
    visibility: "public",
    access_role: "Owner",
    owner_handle: ACTIVE_USER.handle,
    created_at: new Date("2025-01-01T01:00:00.000Z"),
    thread_count: "10",
  },
  {
    id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    name: "Beta",
    description: "Beta project",
    visibility: "private",
    access_role: "Editor",
    owner_handle: ACTIVE_USER.handle,
    created_at: new Date("2025-01-02T02:00:00.000Z"),
    thread_count: "1",
  },
];

describe("/v1/projects", () => {
  beforeEach(() => {
    authMock.setAuthToken(AUTH_TOKEN, ACTIVE_USER);
    authMock.resetAuthMocks();
    authMock.setAuthToken(AUTH_TOKEN, ACTIVE_USER);
    dbMock.resetDbMock();
  });

  describe("GET /v1/projects", () => {
    it("requires auth", async () => {
      const app = await buildV1TestApp();
      const response = await app.inject({ method: "GET", url: "/v1/projects" });
      await app.close();
      expect(response.statusCode).toBe(401);
    });

    it("uses defaults page and pageSize", async () => {
      const tooManyRows = Array.from({ length: 51 }, (_, index) => ({
        ...sampleProjectRows[index % sampleProjectRows.length],
        id: `project-${index}`,
      }));
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("FROM projects p")) {
          expect(params?.[0]).toBe(ACTIVE_USER.id);
          expect(params?.[1]).toBe(51);
          expect(params?.[2]).toBe(0);
          return dbMock.queryRows(...tooManyRows);
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/projects",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      const json = response.json();
      expect(json.page).toBe(1);
      expect(json.pageSize).toBe(50);
      expect(json.nextCursor).toBe("2");
      expect(json.items).toHaveLength(50);
      expect(json.items[0].threadCount).toBe(10);
    });

    it("returns nextCursor when additional projects are available", async () => {
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("FROM projects p")) {
          expect(params?.[1]).toBe(2);
          expect(params?.[2]).toBe(0);
          return dbMock.queryRows(...sampleProjectRows);
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/projects?pageSize=1",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json().page).toBe(1);
      expect(response.json().pageSize).toBe(1);
      expect(response.json().nextCursor).toBe("2");
      expect(response.json().items).toHaveLength(1);
    });

    it("falls back for invalid page and pageSize", async () => {
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("FROM projects p")) {
          expect(params?.[1]).toBe(51);
          expect(params?.[2]).toBe(0);
          return dbMock.queryRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/projects?page=0&pageSize=-1",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      const payload = response.json();
      expect(payload.page).toBe(1);
      expect(payload.pageSize).toBe(50);
    });

    it("clamps pageSize to max 200", async () => {
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("FROM projects p")) {
          expect(params?.[1]).toBe(201);
          return dbMock.queryRows(...Array.from({ length: 201 }, (_, index) => ({
            ...sampleProjectRows[index % sampleProjectRows.length],
            id: `proj-${index}`,
          })));
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/projects?pageSize=9999",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      const payload = response.json();
      expect(payload.pageSize).toBe(200);
    });

    it("adds name filter to SQL pattern", async () => {
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("FROM projects p")) {
          expect(params).toContain("%Alpha%");
          expect(params).toContain(ACTIVE_USER.id);
          return dbMock.queryRows(...sampleProjectRows);
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/projects?name=Alpha",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();
      expect(response.statusCode).toBe(200);
    });
  });

  describe("POST /v1/projects", () => {
    it("validates missing/invalid names and visibility", async () => {
      dbMock.setQueryHandler(async () => dbMock.queryNoRows());
      const app = await buildV1TestApp();

      const missing = await app.inject({
        method: "POST",
        url: "/v1/projects",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: {},
      });
      const badPattern = await app.inject({
        method: "POST",
        url: "/v1/projects",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { name: "_bad-name", visibility: "public" },
      });
      const longName = await app.inject({
        method: "POST",
        url: "/v1/projects",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { name: "a".repeat(81), visibility: "public" },
      });
      const badVisibility = await app.inject({
        method: "POST",
        url: "/v1/projects",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { name: "ok-project", visibility: "public-read" },
      });
      await app.close();

      expect(missing.statusCode).toBe(400);
      expect(badPattern.statusCode).toBe(400);
      expect(longName.statusCode).toBe(400);
      expect(badVisibility.statusCode).toBe(400);
      expect(missing.json().title).toBe("Invalid name");
      expect(badVisibility.json().title).toBe("Invalid visibility");
    });

    it("rejects duplicate project names for owner", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("SELECT 1 FROM projects")) return dbMock.queryRows({ id: "exists" });
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/projects",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { name: "Alpha", visibility: "public" },
      });
      await app.close();

      expect(response.statusCode).toBe(409);
    });

    it("creates project in transaction and returns payload", async () => {
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("SELECT 1 FROM projects")) {
          return dbMock.queryNoRows();
        }
        return dbMock.queryNoRows();
      });
      dbMock.setConnectClientQueries([
        dbMock.queryNoRows(),
        dbMock.queryNoRows(),
        dbMock.queryNoRows(),
        dbMock.queryNoRows(),
        dbMock.queryNoRows(),
      ]);

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/projects",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { name: "New-Project", visibility: "public", description: "new" },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json().name).toBe("New-Project");
      expect(response.json().visibility).toBe("public");
      expect(response.json().accessRole).toBe("Owner");
      expect(response.json().threadCount).toBe(1);
      expect(response.json().id).toBeDefined();
    });

    it("rolls back transaction when insertion fails", async () => {
      dbMock.setQueryHandler(async () => dbMock.queryNoRows());
      dbMock.setConnectClientQueries([
        dbMock.queryNoRows(),
        new Error("insert-failed"),
      ]);

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/projects",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { name: "Explode", visibility: "public" },
      });
      await app.close();

      expect(response.statusCode).toBe(500);
      expect(dbMock.getQueryLog().some((entry) => entry.text.includes("ROLLBACK"))).toBe(true);
    });
  });

  describe("GET /v1/projects/check-name", () => {
    it("requires auth", async () => {
      const app = await buildV1TestApp();
      const response = await app.inject({ method: "GET", url: "/v1/projects/check-name" });
      await app.close();
      expect(response.statusCode).toBe(401);
    });

    it("validates query and checks availability", async () => {
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("SELECT 1 FROM projects WHERE owner_id = $1 AND name = $2")) {
          if (params?.[1] === "Taken") return dbMock.queryRows({ id: "yes" });
          return dbMock.queryNoRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const missing = await app.inject({
        method: "GET",
        url: "/v1/projects/check-name",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      const available = await app.inject({
        method: "GET",
        url: "/v1/projects/check-name?name=Free",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      const unavailable = await app.inject({
        method: "GET",
        url: "/v1/projects/check-name?name=Taken",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(missing.statusCode).toBe(400);
      expect(available.statusCode).toBe(200);
      expect(unavailable.statusCode).toBe(200);
      expect(available.json()).toEqual({ available: true });
      expect(unavailable.json()).toEqual({ available: false });
    });
  });
});
