import { beforeEach, describe, expect, it, vi } from "vitest";
import { buildV1TestApp } from "../utils/app.js";
import * as authMock from "../utils/authMock.js";
import * as dbMock from "../utils/dbMock.js";
import * as openshipMock from "../utils/openshipMock.js";
import * as fsPromises from "node:fs/promises";
import * as eventsMock from "../utils/eventsMock.js";
import { ACTIVE_USER, EDITOR_USER, TEAM_OWNER_USER, THREAD_SUMMARY } from "../utils/fixtures.js";

vi.mock("node:fs/promises", async () => {
  const actual = await vi.importActual("node:fs/promises");
  return {
    ...actual,
    mkdtemp: vi.fn((prefix: string) => actual.mkdtemp(prefix)),
    rm: vi.fn((target: string, options?: { recursive?: boolean; force?: boolean }) => actual.rm(target, options)),
  };
});

vi.mock("../../../src/auth.js", async () => {
  const mocked = await import("../utils/authMock.js");
  return mocked.getAuthMockModule();
});
vi.mock("../../../src/db.js", async () => {
  const mocked = await import("../utils/dbMock.js");
  return mocked.createDbModuleMock();
});
vi.mock("../../../src/agent-runner.js", async () => {
  const mocked = await import("../utils/openshipMock.js");
  return mocked.createOpenShipSyncMockModule();
});
vi.mock("../../../src/events.js", async () => {
  const mocked = await import("../utils/eventsMock.js");
  return mocked.createEventsMockModule();
});

const AUTH_TOKEN = "token-owner";
const EDITOR_TOKEN = "token-editor";
const VIEW_TOKEN = "token-viewer";

const threadSystemId = "ssssssss-ssss-ssss-ssss-ssssssssssss";

describe("/v1/threads", () => {
  beforeEach(() => {
    authMock.setAuthToken(AUTH_TOKEN, ACTIVE_USER);
    authMock.setAuthToken(EDITOR_TOKEN, EDITOR_USER);
    authMock.setAuthToken(VIEW_TOKEN, TEAM_OWNER_USER);
    authMock.resetAuthMocks();
    authMock.setAuthToken(AUTH_TOKEN, ACTIVE_USER);
    authMock.setAuthToken(EDITOR_TOKEN, EDITOR_USER);
    authMock.setAuthToken(VIEW_TOKEN, TEAM_OWNER_USER);
    dbMock.resetDbMock();
    openshipMock.resetOpenShipMock();
    eventsMock.clearQueryEvents();
    vi.mocked(fsPromises.mkdtemp).mockClear();
    vi.mocked(fsPromises.rm).mockClear();
  });

  describe("GET /v1/threads", () => {
    it("requires auth", async () => {
      const app = await buildV1TestApp();
      const response = await app.inject({ method: "GET", url: "/v1/threads" });
      await app.close();
      expect(response.statusCode).toBe(401);
    });

    it("lists threads with defaults and no project filter", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows(
            {
              ...THREAD_SUMMARY,
              created_by_handle: TEAM_OWNER_USER.handle,
              owner_handle: TEAM_OWNER_USER.handle,
            },
          );
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/threads",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      const json = response.json();
      expect(response.statusCode).toBe(200);
      expect(json.page).toBe(1);
      expect(json.pageSize).toBe(50);
      expect(json.nextCursor).toBeNull();
      expect(json.items[0].projectName).toBe(THREAD_SUMMARY.project_name);
    });

    it("applies project filter", async () => {
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("FROM threads t")) {
          expect(params?.[3]).toBe(THREAD_SUMMARY.project_id);
          return dbMock.queryRows({
            ...THREAD_SUMMARY,
            created_by_handle: TEAM_OWNER_USER.handle,
            owner_handle: TEAM_OWNER_USER.handle,
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: `/v1/threads?projectId=${THREAD_SUMMARY.project_id}`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
    });

    it("returns nextCursor when there are more threads than pageSize", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows(
            {
              ...THREAD_SUMMARY,
              created_by_handle: TEAM_OWNER_USER.handle,
              owner_handle: TEAM_OWNER_USER.handle,
            },
            {
              ...THREAD_SUMMARY,
              id: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
              project_id: THREAD_SUMMARY.project_id,
              created_by_handle: TEAM_OWNER_USER.handle,
              owner_handle: TEAM_OWNER_USER.handle,
            },
          );
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/threads?pageSize=1",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json().page).toBe(1);
      expect(response.json().pageSize).toBe(1);
      expect(response.json().nextCursor).toBe("2");
      expect(response.json().items).toHaveLength(1);
    });
  });

  describe("POST /v1/threads", () => {
    it("validates projectId", async () => {
      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/threads",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { projectId: "bad-id" },
      });
      await app.close();
      expect(response.statusCode).toBe(400);
      expect(response.json().title).toBe("Invalid projectId");
    });

    it("validates sourceThreadId", async () => {
      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/threads",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: {
          projectId: THREAD_SUMMARY.project_id,
          sourceThreadId: "bad-source",
        },
      });
      await app.close();
      expect(response.statusCode).toBe(400);
      expect(response.json().title).toBe("Invalid sourceThreadId");
    });

    it("returns 400 when no source thread exists", async () => {
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("FROM projects p")) {
          return dbMock.queryRows({
            project_id: THREAD_SUMMARY.project_id,
            owner_id: ACTIVE_USER.id,
            visibility: "public",
            owner_handle: ACTIVE_USER.handle,
            is_archived: false,
            access_role: "Owner",
            name: "Project",
          });
        }
        if (text.includes("SELECT id FROM threads WHERE project_id = $1 ORDER BY updated_at DESC LIMIT 1")) {
          return dbMock.queryNoRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/threads",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { projectId: THREAD_SUMMARY.project_id },
      });
      await app.close();
      expect(response.statusCode).toBe(400);
      expect(response.json().title).toBe("No source thread");
    });

    it("forbids non-editors", async () => {
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("FROM projects p")) {
          return dbMock.queryRows({
            project_id: THREAD_SUMMARY.project_id,
            owner_id: ACTIVE_USER.id,
            visibility: "public",
            owner_handle: ACTIVE_USER.handle,
            is_archived: false,
            access_role: "Viewer",
            name: "Project",
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/threads",
        headers: { authorization: `Bearer ${VIEW_TOKEN}` },
        payload: { projectId: THREAD_SUMMARY.project_id },
      });
      await app.close();
      expect(response.statusCode).toBe(403);
      expect(response.json().title).toBe("Forbidden");
    });

    it("creates thread with default title when absent", async () => {
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("FROM projects p")) {
          return dbMock.queryRows({
            project_id: THREAD_SUMMARY.project_id,
            owner_id: ACTIVE_USER.id,
            visibility: "private",
            owner_handle: ACTIVE_USER.handle,
            is_archived: false,
            access_role: "Owner",
            name: "Project",
          });
        }
        if (text.includes("SELECT id FROM threads WHERE project_id = $1 ORDER BY updated_at DESC LIMIT 1")) {
          return dbMock.queryRows({ id: THREAD_SUMMARY.id });
        }
        if (text.includes("SELECT") && text.includes("FROM threads t")) {
          return dbMock.queryRows({
            ...THREAD_SUMMARY,
            project_id: THREAD_SUMMARY.project_id,
            owner_handle: ACTIVE_USER.handle,
            created_by_handle: ACTIVE_USER.handle,
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/threads",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { projectId: THREAD_SUMMARY.project_id },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json().title).toBe("Thread summary");
      expect(response.json().projectId).toBe(THREAD_SUMMARY.project_id);
      expect(response.json().status).toBe("open");
    });
  });

  describe("GET /v1/threads/:threadId", () => {
    it("validates thread UUID", async () => {
      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/threads/not-a-uuid",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();
      expect(response.statusCode).toBe(400);
      expect(response.json().title).toBe("Invalid threadId");
    });

    it("returns 404 when thread access is denied", async () => {
      dbMock.setQueryHandler(async () => dbMock.queryNoRows());
      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: `/v1/threads/${THREAD_SUMMARY.id}`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();
      expect(response.statusCode).toBe(404);
    });

    it("returns thread payload with permissions topology matrix and chat", async () => {
      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("JOIN projects p") && text.includes("p.is_archived = false")) {
          return dbMock.queryRows({
            id: THREAD_SUMMARY.id,
            project_id: THREAD_SUMMARY.project_id,
            title: THREAD_SUMMARY.title,
            description: THREAD_SUMMARY.description,
            status: THREAD_SUMMARY.status,
            created_at: THREAD_SUMMARY.created_at,
            updated_at: THREAD_SUMMARY.updated_at,
            source_thread_id: null,
            access_role: "Owner",
          });
        }
        if (text.includes("SELECT p.name AS project_name")) {
          return dbMock.queryRows({
            project_name: "Project",
            owner_handle: ACTIVE_USER.handle,
            creator_handle: ACTIVE_USER.handle,
          });
        }
        if (text.includes("SELECT m.id, m.action_id")) {
          return dbMock.queryRows({
            id: "m1",
            action_id: "a1",
            action_type: "Chat",
            action_position: 1,
            role: "User",
            content: "Hi",
            created_at: new Date("2025-06-10T00:00:00.000Z"),
          });
        }
        if (text.includes("FROM threads t") && text.includes("thread_current_system($1) AS id") && params?.[0] === THREAD_SUMMARY.id) {
          return dbMock.queryRows({ id: threadSystemId });
        }
        if (text.includes("SELECT n.id, n.name, n.kind::text AS kind")) {
          return dbMock.queryRows({
            id: "n1",
            name: "root",
            kind: "Root",
            parent_id: null,
            metadata: { layout: { x: 1, y: 2 } },
          });
        }
        if (text.includes("SELECT e.id, e.from_node_id")) {
          return dbMock.queryRows({
            id: "e1",
            from_node_id: "n1",
            to_node_id: "n2",
            type: "Contains",
            metadata: { protocol: "http" },
          });
        }
        if (text.includes("SELECT mr.node_id, mr.concern")) {
          return dbMock.queryRows({
            node_id: "s.root",
            concern: "content",
            hash: "h1",
            title: "Doc",
            kind: "Document",
            language: "md",
            source_type: "local",
            source_url: null,
            source_external_id: null,
          });
        }
        if (text.includes("SELECT name, position FROM concerns")) {
          return dbMock.queryRows({ name: "Scope", position: 1 });
        }
        if (text.includes("SELECT hash, kind::text AS kind, title")) {
          return dbMock.queryRows({
            hash: "h1",
            kind: "Document",
            title: "Doc",
            language: "md",
            text: "Content",
            source_type: "local",
            source_url: null,
            source_external_id: null,
            source_metadata: {},
            source_connected_user_id: null,
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: `/v1/threads/${THREAD_SUMMARY.id}`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      const json = response.json();
      expect(response.statusCode).toBe(200);
      expect(json.thread.id).toBe(THREAD_SUMMARY.id);
      expect(json.permissions.canEdit).toBe(true);
      expect(Array.isArray(json.topology.nodes)).toBe(true);
      expect(Array.isArray(json.matrix.nodes)).toBe(true);
      expect(Array.isArray(json.chat.messages)).toBe(true);
    });
  });

  describe("PATCH /v1/threads/:threadId", () => {
    it("validates thread UUID", async () => {
      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "PATCH",
        url: "/v1/threads/not-a-uuid",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { title: "Updated" },
      });
      await app.close();

      expect(response.statusCode).toBe(400);
      expect(response.json().title).toBe("Invalid threadId");
    });

    it("validates payload", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows({
            id: THREAD_SUMMARY.id,
            project_id: THREAD_SUMMARY.project_id,
            title: THREAD_SUMMARY.title,
            description: THREAD_SUMMARY.description,
            status: "open",
            created_at: THREAD_SUMMARY.created_at,
            updated_at: THREAD_SUMMARY.updated_at,
            source_thread_id: null,
            access_role: "Owner",
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const noPayload = await app.inject({
        method: "PATCH",
        url: `/v1/threads/${THREAD_SUMMARY.id}`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: {},
      });
      const badTitle = await app.inject({
        method: "PATCH",
        url: `/v1/threads/${THREAD_SUMMARY.id}`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { title: "   " },
      });
      const badStatus = await app.inject({
        method: "PATCH",
        url: `/v1/threads/${THREAD_SUMMARY.id}`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { status: "invalid" },
      });
      await app.close();

      expect(noPayload.statusCode).toBe(400);
      expect(badTitle.statusCode).toBe(400);
      expect(badStatus.statusCode).toBe(400);
    });

    it("forbids non-editors", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows({
            id: THREAD_SUMMARY.id,
            project_id: THREAD_SUMMARY.project_id,
            title: THREAD_SUMMARY.title,
            description: THREAD_SUMMARY.description,
            status: "open",
            created_at: THREAD_SUMMARY.created_at,
            updated_at: THREAD_SUMMARY.updated_at,
            source_thread_id: null,
            access_role: "Viewer",
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "PATCH",
        url: `/v1/threads/${THREAD_SUMMARY.id}`,
        headers: { authorization: `Bearer ${VIEW_TOKEN}` },
        payload: { title: "Updated" },
      });
      await app.close();

      expect(response.statusCode).toBe(403);
    });

    it("updates title and status", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows({
            id: THREAD_SUMMARY.id,
            project_id: THREAD_SUMMARY.project_id,
            title: THREAD_SUMMARY.title,
            description: THREAD_SUMMARY.description,
            status: "open",
            created_at: THREAD_SUMMARY.created_at,
            updated_at: THREAD_SUMMARY.updated_at,
            source_thread_id: null,
            access_role: "Owner",
          });
        }
        if (text.includes("UPDATE threads")) {
          return dbMock.queryRows({
            id: THREAD_SUMMARY.id,
            title: "Updated",
            description: "Updated",
            status: "committed",
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "PATCH",
        url: `/v1/threads/${THREAD_SUMMARY.id}`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { title: "Updated", description: "Updated", status: "committed" },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json().thread.status).toBe("committed");
    });
  });

  describe("DELETE /v1/threads/:threadId", () => {
    it("validates thread UUID", async () => {
      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "DELETE",
        url: "/v1/threads/not-a-uuid",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(400);
      expect(response.json().title).toBe("Invalid threadId");
    });

    it("requires owner/editor", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows({
            id: THREAD_SUMMARY.id,
            project_id: THREAD_SUMMARY.project_id,
            title: THREAD_SUMMARY.title,
            description: THREAD_SUMMARY.description,
            status: "open",
            created_at: THREAD_SUMMARY.created_at,
            updated_at: THREAD_SUMMARY.updated_at,
            source_thread_id: null,
            access_role: "Viewer",
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "DELETE",
        url: `/v1/threads/${THREAD_SUMMARY.id}`,
        headers: { authorization: `Bearer ${VIEW_TOKEN}` },
      });
      await app.close();
      expect(response.statusCode).toBe(403);
    });

    it("deletes thread successfully", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows({
            id: THREAD_SUMMARY.id,
            project_id: THREAD_SUMMARY.project_id,
            title: THREAD_SUMMARY.title,
            description: THREAD_SUMMARY.description,
            status: "open",
            created_at: THREAD_SUMMARY.created_at,
            updated_at: THREAD_SUMMARY.updated_at,
            source_thread_id: null,
            access_role: "Owner",
          });
        }
        if (text.startsWith("DELETE FROM threads")) return dbMock.queryRows();
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "DELETE",
        url: `/v1/threads/${THREAD_SUMMARY.id}`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(204);
      expect(response.body).toBe("");
    });
  });

  describe("GET /v1/threads/:threadId/matrix", () => {
    it("requires valid thread id", async () => {
      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: `/v1/threads/not-a-uuid/matrix`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();
      expect(response.statusCode).toBe(400);
      expect(response.json().title).toBe("Invalid threadId");
    });

    it("returns 500 when matrix system is missing", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows({
            id: THREAD_SUMMARY.id,
            project_id: THREAD_SUMMARY.project_id,
            title: THREAD_SUMMARY.title,
            description: THREAD_SUMMARY.description,
            status: "open",
            created_at: THREAD_SUMMARY.created_at,
            updated_at: THREAD_SUMMARY.updated_at,
            source_thread_id: null,
            access_role: "Owner",
          });
        }
        if (text.includes("thread_current_system")) return dbMock.queryNoRows();
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: `/v1/threads/${THREAD_SUMMARY.id}/matrix`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(500);
      expect(response.json().detail).toContain("no current system");
    });

    it("returns topology and matrix", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows({
            id: THREAD_SUMMARY.id,
            project_id: THREAD_SUMMARY.project_id,
            title: THREAD_SUMMARY.title,
            description: THREAD_SUMMARY.description,
            status: "open",
            created_at: THREAD_SUMMARY.created_at,
            updated_at: THREAD_SUMMARY.updated_at,
            source_thread_id: null,
            access_role: "Owner",
          });
        }
        if (text.includes("thread_current_system")) {
          return dbMock.queryRows({ system_id: threadSystemId });
        }
        if (text.includes("FROM nodes n")) {
          return dbMock.queryRows({
            id: "n1",
            name: "root",
            kind: "Root",
            parent_id: null,
            metadata: { layout: { x: 1, y: 2 } },
          });
        }
        if (text.includes("FROM edges e")) {
          return dbMock.queryRows({
            id: "e1",
            from_node_id: "n1",
            to_node_id: "n2",
            type: "Contains",
            metadata: { protocol: "http" },
          });
        }
        if (text.includes("SELECT mr.node_id, mr.concern")) {
          return dbMock.queryRows({
            node_id: "s.root",
            concern: "content",
            hash: "h1",
            title: "Doc",
            kind: "Document",
            language: "md",
            source_type: "local",
            source_url: null,
            source_external_id: null,
          });
        }
        if (text.includes("SELECT name, position FROM concerns")) {
          return dbMock.queryRows({ name: "Scope", position: 1 });
        }
        if (text.includes("SELECT hash, kind::text AS kind, title")) {
          return dbMock.queryRows({
            hash: "h1",
            kind: "Document",
            title: "Doc",
            language: "md",
            text: "Content",
            source_type: "local",
            source_url: null,
            source_external_id: null,
            source_metadata: {},
            source_connected_user_id: null,
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: `/v1/threads/${THREAD_SUMMARY.id}/matrix`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      const json = response.json();
      expect(response.statusCode).toBe(200);
      expect(json.threadId).toBe(THREAD_SUMMARY.id);
      expect(Array.isArray(json.topology.nodes)).toBe(true);
      expect(Array.isArray(json.matrix.nodes)).toBe(true);
      expect(json.matrix.concerns[0].name).toBe("Scope");
    });
  });

  describe("PATCH /v1/threads/:threadId/matrix", () => {
    it("validates thread UUID", async () => {
      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "PATCH",
        url: "/v1/threads/not-a-uuid/matrix",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { layout: [{ nodeId: "n1", x: 1, y: 2 }] },
      });
      await app.close();

      expect(response.statusCode).toBe(400);
      expect(response.json().title).toBe("Invalid threadId");
    });

    it("validates payload layout", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows({
            id: THREAD_SUMMARY.id,
            project_id: THREAD_SUMMARY.project_id,
            title: THREAD_SUMMARY.title,
            description: THREAD_SUMMARY.description,
            status: "open",
            created_at: THREAD_SUMMARY.created_at,
            updated_at: THREAD_SUMMARY.updated_at,
            source_thread_id: null,
            access_role: "Owner",
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "PATCH",
        url: `/v1/threads/${THREAD_SUMMARY.id}/matrix`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { layout: [{ nodeId: "", x: "a", y: 1 }] },
      });
      await app.close();
      expect(response.statusCode).toBe(400);
      expect(response.json().title).toBe("Invalid matrix payload");
    });

    it("forbids non-editors", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows({
            id: THREAD_SUMMARY.id,
            project_id: THREAD_SUMMARY.project_id,
            title: THREAD_SUMMARY.title,
            description: THREAD_SUMMARY.description,
            status: "open",
            created_at: THREAD_SUMMARY.created_at,
            updated_at: THREAD_SUMMARY.updated_at,
            source_thread_id: null,
            access_role: "Viewer",
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "PATCH",
        url: `/v1/threads/${THREAD_SUMMARY.id}/matrix`,
        headers: { authorization: `Bearer ${VIEW_TOKEN}` },
        payload: { layout: [{ nodeId: "n1", x: 1, y: 2 }] },
      });
      await app.close();
      expect(response.statusCode).toBe(403);
    });

    it("returns updated matrix with changed count", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows({
            id: THREAD_SUMMARY.id,
            project_id: THREAD_SUMMARY.project_id,
            title: THREAD_SUMMARY.title,
            description: THREAD_SUMMARY.description,
            status: "open",
            created_at: THREAD_SUMMARY.created_at,
            updated_at: THREAD_SUMMARY.updated_at,
            source_thread_id: null,
            access_role: "Owner",
          });
        }
        if (text.includes("thread_current_system")) {
          return dbMock.queryRows({ system_id: threadSystemId });
        }
        if (text.includes("UPDATE nodes")) {
          return dbMock.queryRows({ changed: 1 });
        }
        if (text.includes("SELECT mr.node_id, mr.concern")) {
          return dbMock.queryRows({
            node_id: "s.root",
            concern: "content",
            hash: "h1",
            title: "Doc",
            kind: "Document",
            language: "md",
            source_type: "local",
            source_url: null,
            source_external_id: null,
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "PATCH",
        url: `/v1/threads/${THREAD_SUMMARY.id}/matrix`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { layout: [{ nodeId: "n1", x: 1, y: 1 }] },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json().changed).toBe(1);
      expect(response.json().matrix.nodes).toHaveLength(1);
    });

    it("returns 404 when no nodes updated", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows({
            id: THREAD_SUMMARY.id,
            project_id: THREAD_SUMMARY.project_id,
            title: THREAD_SUMMARY.title,
            description: THREAD_SUMMARY.description,
            status: "open",
            created_at: THREAD_SUMMARY.created_at,
            updated_at: THREAD_SUMMARY.updated_at,
            source_thread_id: null,
            access_role: "Owner",
          });
        }
        if (text.includes("thread_current_system")) return dbMock.queryRows({ system_id: threadSystemId });
        if (text.includes("UPDATE nodes")) return dbMock.queryNoRows();
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "PATCH",
        url: `/v1/threads/${THREAD_SUMMARY.id}/matrix`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { layout: [{ nodeId: "n1", x: 1, y: 1 }] },
      });
      await app.close();

      expect(response.statusCode).toBe(404);
      expect(response.json().title).toBe("No nodes updated");
    });
  });

  describe("GET /v1/threads/:threadId/openship/bundle", () => {
    it("requires editor role for bundling", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows({
            id: THREAD_SUMMARY.id,
            project_id: THREAD_SUMMARY.project_id,
            title: THREAD_SUMMARY.title,
            description: THREAD_SUMMARY.description,
            status: "open",
            created_at: THREAD_SUMMARY.created_at,
            updated_at: THREAD_SUMMARY.updated_at,
            source_thread_id: null,
            access_role: "Viewer",
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: `/v1/threads/${THREAD_SUMMARY.id}/openship/bundle`,
        headers: { authorization: `Bearer ${VIEW_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(403);
      expect(response.json().title).toBe("Forbidden");
    });

    it("requires authz and valid system before bundling", async () => {
      const app = await buildV1TestApp();
      const invalidUuid = await app.inject({
        method: "GET",
        url: "/v1/threads/not-a-uuid/openship/bundle",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });

      dbMock.setQueryHandler(async (text, params) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows({
            id: THREAD_SUMMARY.id,
            project_id: THREAD_SUMMARY.project_id,
            title: THREAD_SUMMARY.title,
            description: THREAD_SUMMARY.description,
            status: "open",
            created_at: THREAD_SUMMARY.created_at,
            updated_at: THREAD_SUMMARY.updated_at,
            source_thread_id: null,
            access_role: "Owner",
          });
        }
        if (text.includes("thread_current_system")) return dbMock.queryNoRows();
        return dbMock.queryNoRows();
      });

      const noSystem = await app.inject({
        method: "GET",
        url: `/v1/threads/${THREAD_SUMMARY.id}/openship/bundle`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(invalidUuid.statusCode).toBe(400);
      expect(noSystem.statusCode).toBe(500);
    });

    it("returns bundled files and attempts cleanup", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows({
            id: THREAD_SUMMARY.id,
            project_id: THREAD_SUMMARY.project_id,
            title: THREAD_SUMMARY.title,
            description: THREAD_SUMMARY.description,
            status: "open",
            created_at: THREAD_SUMMARY.created_at,
            updated_at: THREAD_SUMMARY.updated_at,
            source_thread_id: null,
            access_role: "Owner",
          });
        }
        if (text.includes("thread_current_system")) return dbMock.queryRows({ system_id: threadSystemId });
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: `/v1/threads/${THREAD_SUMMARY.id}/openship/bundle`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      const json = response.json();
      expect(response.statusCode).toBe(200);
      expect(json.threadId).toBe(THREAD_SUMMARY.id);
      expect(json.systemId).toBe(threadSystemId);
      expect(Array.isArray(json.files)).toBe(true);
      expect(json.files.length).toBeGreaterThan(0);
      expect(vi.mocked(fsPromises.mkdtemp).mock.calls).toHaveLength(1);
      expect(vi.mocked(fsPromises.rm).mock.calls).toHaveLength(1);
      const [expectedWorkspace] = vi.mocked(fsPromises.mkdtemp).mock.calls[0] ?? [];
      const [cleanupTarget] = vi.mocked(fsPromises.rm).mock.calls[0] ?? [];
      expect(cleanupTarget).toBeDefined();
      expect(typeof cleanupTarget === "string" && typeof expectedWorkspace === "string").toBe(true);
      if (typeof cleanupTarget === "string" && typeof expectedWorkspace === "string") {
        expect(cleanupTarget.startsWith(expectedWorkspace)).toBe(true);
      }
    });
  });

  describe("POST /v1/threads/:threadId/chat", () => {
    it("validates thread id and content", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows({
            id: THREAD_SUMMARY.id,
            project_id: THREAD_SUMMARY.project_id,
            title: THREAD_SUMMARY.title,
            description: THREAD_SUMMARY.description,
            status: "open",
            created_at: THREAD_SUMMARY.created_at,
            updated_at: THREAD_SUMMARY.updated_at,
            source_thread_id: null,
            access_role: "Owner",
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const invalid = await app.inject({
        method: "POST",
        url: "/v1/threads/not-id/chat",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { content: "Hi" },
      });
      const empty = await app.inject({
        method: "POST",
        url: `/v1/threads/${THREAD_SUMMARY.id}/chat`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { content: "   " },
      });
      const badRole = await app.inject({
        method: "POST",
        url: `/v1/threads/${THREAD_SUMMARY.id}/chat`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { content: "Hi", role: "Invalid" },
      });
      await app.close();

      expect(invalid.statusCode).toBe(400);
      expect(empty.statusCode).toBe(400);
      expect(badRole.statusCode).toBe(400);
    });

    it("forbids viewers from posting", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows({
            id: THREAD_SUMMARY.id,
            project_id: THREAD_SUMMARY.project_id,
            title: THREAD_SUMMARY.title,
            description: THREAD_SUMMARY.description,
            status: "open",
            created_at: THREAD_SUMMARY.created_at,
            updated_at: THREAD_SUMMARY.updated_at,
            source_thread_id: null,
            access_role: "Viewer",
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: `/v1/threads/${THREAD_SUMMARY.id}/chat`,
        headers: { authorization: `Bearer ${VIEW_TOKEN}` },
        payload: { content: "Hello" },
      });
      await app.close();

      expect(response.statusCode).toBe(403);
    });

    it("appends message and increments action position", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows({
            id: THREAD_SUMMARY.id,
            project_id: THREAD_SUMMARY.project_id,
            title: THREAD_SUMMARY.title,
            description: THREAD_SUMMARY.description,
            status: "open",
            created_at: THREAD_SUMMARY.created_at,
            updated_at: THREAD_SUMMARY.updated_at,
            source_thread_id: null,
            access_role: "Owner",
          });
        }
        if (text.includes("COALESCE(MAX(position), 0) + 1")) {
          return dbMock.queryRows({ position: 4 });
        }
        if (text.includes("INSERT INTO actions")) return dbMock.queryRows();
        if (text.includes("INSERT INTO messages")) {
          return dbMock.queryRows({
            id: "m1",
            action_id: "a1",
            role: "User",
            content: "Hello",
            created_at: new Date("2025-06-20T00:00:00.000Z"),
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: `/v1/threads/${THREAD_SUMMARY.id}/chat`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { content: "Hello", role: "User" },
      });
      await app.close();

      const json = response.json();
      expect(response.statusCode).toBe(200);
      expect(json.messages[0].actionPosition).toBe(4);
      expect(json.messages[0].role).toBe("User");
      expect(json.messages[0].content).toBe("Hello");
    });
  });
});
