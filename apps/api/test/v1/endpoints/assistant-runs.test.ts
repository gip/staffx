import { beforeEach, describe, expect, it, vi } from "vitest";
import { buildV1TestApp } from "../utils/app.js";
import * as agentQueueMock from "../utils/agentQueueMock.js";
import * as authMock from "../utils/authMock.js";
import * as dbMock from "../utils/dbMock.js";
import * as eventsMock from "../utils/eventsMock.js";
import { ACTIVE_USER, EDITOR_USER, FIXTURE_NOW, THREAD_SUMMARY, VIEWER_USER } from "../utils/fixtures.js";

vi.mock("../../../src/auth.js", async () => {
  const mocked = await import("../utils/authMock.js");
  return mocked.getAuthMockModule();
});
vi.mock("../../../src/db.js", async () => {
  const mocked = await import("../utils/dbMock.js");
  return mocked.createDbModuleMock();
});
vi.mock("../../../src/agent-queue.js", async () => {
  const mocked = await import("../utils/agentQueueMock.js");
  return mocked.createAgentQueueMockModule();
});
vi.mock("../../../src/events.js", async () => {
  const mocked = await import("../utils/eventsMock.js");
  return mocked.createEventsMockModule();
});

const AUTH_TOKEN = "token-owner";
const VIEW_TOKEN = "token-view";

function threadAccessRow(role: "Owner" | "Viewer" | "Editor", threadId = THREAD_SUMMARY.id) {
  return {
    id: threadId,
    project_id: THREAD_SUMMARY.project_id,
    title: THREAD_SUMMARY.title,
    description: THREAD_SUMMARY.description,
    status: "open",
    created_at: THREAD_SUMMARY.created_at,
    updated_at: THREAD_SUMMARY.updated_at,
    source_thread_id: null,
    access_role: role,
  };
}

describe("/v1 assistant runs", () => {
  beforeEach(() => {
    authMock.setAuthToken(AUTH_TOKEN, ACTIVE_USER);
    authMock.setAuthToken(VIEW_TOKEN, VIEWER_USER);
    authMock.setAuthToken("token-editor", EDITOR_USER);
    authMock.resetAuthMocks();
    authMock.setAuthToken(AUTH_TOKEN, ACTIVE_USER);
    authMock.setAuthToken(VIEW_TOKEN, VIEWER_USER);
    authMock.setAuthToken("token-editor", EDITOR_USER);
    dbMock.resetDbMock();
    agentQueueMock.resetAgentQueueMocks();
    eventsMock.clearQueryEvents();
  });

  describe("POST /v1/threads/:threadId/assistants/:assistantType/runs", () => {
    it("validates threadId and assistant type", async () => {
      const app = await buildV1TestApp();
      const invalidType = await app.inject({
        method: "POST",
        url: `/v1/threads/${THREAD_SUMMARY.id}/assistants/invalid/runs`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: {},
      });
      const invalidThread = await app.inject({
        method: "POST",
        url: `/v1/threads/not-a-uuid/assistants/direct/runs`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(invalidType.statusCode).toBe(400);
      expect(invalidType.json().detail).toBe("assistantType must be direct or plan");
      expect(invalidThread.statusCode).toBe(400);
      expect(invalidThread.json().detail).toBe("threadId must be a UUID.");
    });

    it("validates chatMessageId format", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows(threadAccessRow("Owner"));
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: `/v1/threads/${THREAD_SUMMARY.id}/assistants/direct/runs`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { chatMessageId: "bad-id" },
      });
      await app.close();

      expect(response.statusCode).toBe(400);
      expect(response.json().title).toBe("Invalid chatMessageId");
    });

    it("requires editor privilege to start assistant runs", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows(threadAccessRow("Viewer"));
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: `/v1/threads/${THREAD_SUMMARY.id}/assistants/plan/runs`,
        headers: { authorization: `Bearer ${VIEW_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(403);
      expect(response.json().title).toBe("Forbidden");
    });

    it("uses default prompt when none is provided", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows(threadAccessRow("Owner"));
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: `/v1/threads/${THREAD_SUMMARY.id}/assistants/direct/runs`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: {},
      });
      await app.close();

      const enqueueCall = vi.mocked(agentQueueMock.agentQueueMock.enqueueAgentRunWithWait).mock.calls.at(-1)?.[0];
      expect(response.statusCode).toBe(200);
      expect(enqueueCall?.prompt).toBe("Run this request.");
      expect(enqueueCall?.chatMessageId).toBeNull();
    });

    it("returns queued run summary after start", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows(threadAccessRow("Editor"));
        }
        return dbMock.queryNoRows();
      });
      agentQueueMock.setEnqueueRunId("run-started-001");

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: `/v1/threads/${THREAD_SUMMARY.id}/assistants/plan/runs`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { prompt: "Summarize this thread", chatMessageId: null, wait: false },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({
        runId: "run-started-001",
        status: "queued",
        mode: "plan",
        threadId: THREAD_SUMMARY.id,
      });
    });
  });

  describe("GET /v1/assistant-runs/:runId", () => {
    it("returns 400 for invalid run id", async () => {
      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/assistant-runs/not-a-uuid",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(400);
      expect(response.json().title).toBe("Invalid run id");
    });

    it("returns 404 when run does not exist", async () => {
      dbMock.setQueryHandler(async () => dbMock.queryNoRows());
      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/assistant-runs/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(404);
      expect(response.json().title).toBe("Run not found");
    });

    it("forbids access to run for unauthorized users", async () => {
      agentQueueMock.setAgentQueueRun({
        id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        thread_id: THREAD_SUMMARY.id,
        status: "queued",
        mode: "direct",
        prompt: "Run request",
        system_prompt: null,
        run_result_status: null,
        run_result_messages: null,
        run_result_changes: null,
        run_error: null,
        created_at: FIXTURE_NOW,
        started_at: null,
        completed_at: null,
      });
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryNoRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/assistant-runs/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        headers: { authorization: `Bearer ${VIEW_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(403);
      expect(response.json().title).toBe("Forbidden");
    });

    it("maps and returns assistant run row", async () => {
      agentQueueMock.setAgentQueueRun({
        id: "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        thread_id: THREAD_SUMMARY.id,
        status: "running",
        mode: "direct",
        prompt: "Run request",
        system_prompt: null,
        run_result_status: null,
        run_result_messages: ["ready"],
        run_result_changes: [],
        run_error: null,
        created_at: FIXTURE_NOW,
        started_at: FIXTURE_NOW,
        completed_at: null,
      });
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows(threadAccessRow("Owner"));
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/assistant-runs/cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json()).toMatchObject({
        runId: "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        threadId: THREAD_SUMMARY.id,
        status: "running",
        mode: "direct",
        runResultStatus: null,
        runResultMessages: ["ready"],
        runResultChanges: [],
      });
    });
  });

  describe("POST /v1/assistant-runs/:runId/claim", () => {
    it("returns 400 for invalid run id", async () => {
      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/assistant-runs/not-a-uuid/claim",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: {},
      });
      await app.close();

      expect(response.statusCode).toBe(400);
      expect(response.json().title).toBe("Invalid run id");
    });

    it("returns 404 when run does not exist", async () => {
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows(threadAccessRow("Owner"));
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/assistant-runs/eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee/claim",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(404);
      expect(response.json().title).toBe("Run not found");
    });

    it("forbids claims when thread access is missing", async () => {
      agentQueueMock.setAgentQueueRun({
        id: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        thread_id: THREAD_SUMMARY.id,
        status: "queued",
        mode: "plan",
        prompt: "Run request",
        system_prompt: null,
        run_result_status: null,
        run_result_messages: null,
        run_result_changes: null,
        run_error: null,
        created_at: FIXTURE_NOW,
        started_at: null,
        completed_at: null,
      });
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryNoRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/assistant-runs/dddddddd-dddd-4ddd-8ddd-dddddddddddd/claim",
        headers: { authorization: `Bearer ${VIEW_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(403);
      expect(response.json().title).toBe("Forbidden");
    });

    it("reports conflict when run cannot be claimed", async () => {
      agentQueueMock.setAgentQueueRun({
        id: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        thread_id: THREAD_SUMMARY.id,
        status: "running",
        mode: "direct",
        prompt: "Run request",
        system_prompt: null,
        run_result_status: null,
        run_result_messages: null,
        run_result_changes: null,
        run_error: null,
        created_at: FIXTURE_NOW,
        started_at: FIXTURE_NOW,
        completed_at: null,
      });
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows(threadAccessRow("Owner"));
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/assistant-runs/eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee/claim",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { runnerId: "agent-1" },
      });
      await app.close();

      expect(response.statusCode).toBe(409);
      expect(response.json().detail).toBe("Run is not available for claiming");
    });

    it("claims a run with explicit runner id", async () => {
      agentQueueMock.setAgentQueueRun({
        id: "ffffffff-ffff-4fff-8fff-ffffffffffff",
        thread_id: THREAD_SUMMARY.id,
        status: "queued",
        mode: "direct",
        prompt: "Run request",
        system_prompt: null,
        run_result_status: null,
        run_result_messages: null,
        run_result_changes: null,
        run_error: null,
        created_at: FIXTURE_NOW,
        started_at: null,
        completed_at: null,
      });
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
        return dbMock.queryRows(threadAccessRow("Owner"));
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const withRunner = await app.inject({
        method: "POST",
        url: "/v1/assistant-runs/ffffffff-ffff-4fff-8fff-ffffffffffff/claim",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { runnerId: "agent-explicit" },
      });
      await app.close();

      expect(withRunner.statusCode).toBe(200);
      expect(withRunner.json().runId).toBe("ffffffff-ffff-4fff-8fff-ffffffffffff");
      const firstClaimCall = vi.mocked(agentQueueMock.agentQueueMock.claimAgentRunById).mock.calls[0];
      expect(firstClaimCall?.[1]).toBe("agent-explicit");
    });

    it("claims a run with default runner id when omitted", async () => {
      agentQueueMock.setAgentQueueRun({
        id: "11111111-1111-4111-8111-111111111111",
        thread_id: THREAD_SUMMARY.id,
        status: "queued",
        mode: "direct",
        prompt: "Run request",
        system_prompt: null,
        run_result_status: null,
        run_result_messages: null,
        run_result_changes: null,
        run_error: null,
        created_at: FIXTURE_NOW,
        started_at: null,
        completed_at: null,
      });
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows(threadAccessRow("Owner"));
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const fallbackRunner = await app.inject({
        method: "POST",
        url: "/v1/assistant-runs/11111111-1111-4111-8111-111111111111/claim",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: {},
      });
      await app.close();

      expect(fallbackRunner.statusCode).toBe(200);
      const secondClaimCall = vi.mocked(agentQueueMock.agentQueueMock.claimAgentRunById).mock.calls.at(-1);
      expect(secondClaimCall?.[1]).toMatch(/^desktop-/);
    });
  });

  describe("POST /v1/assistant-runs/:runId/complete", () => {
    it("validates status and message payload", async () => {
      const runId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
      agentQueueMock.setAgentQueueRun({
        id: runId,
        thread_id: THREAD_SUMMARY.id,
        status: "running",
        mode: "direct",
        prompt: "Run request",
        system_prompt: null,
        run_result_status: null,
        run_result_messages: null,
        run_result_changes: null,
        run_error: null,
        created_at: FIXTURE_NOW,
        started_at: FIXTURE_NOW,
        completed_at: null,
      });
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows(threadAccessRow("Owner"));
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const invalidStatus = await app.inject({
        method: "POST",
        url: `/v1/assistant-runs/${runId}/complete`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { status: "bad", messages: ["x"] },
      });
      const missingMessages = await app.inject({
        method: "POST",
        url: `/v1/assistant-runs/${runId}/complete`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { status: "success", messages: [] },
      });
      const badMessages = await app.inject({
        method: "POST",
        url: `/v1/assistant-runs/${runId}/complete`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { status: "success", messages: ["   ", 42, "keep"] },
      });
      await app.close();

      expect(invalidStatus.statusCode).toBe(400);
      expect(invalidStatus.json().title).toBe("Invalid status");
      expect(missingMessages.statusCode).toBe(400);
      expect(missingMessages.json().title).toBe("Invalid payload");
      expect(badMessages.statusCode).toBe(200);
      const badCall = vi.mocked(agentQueueMock.agentQueueMock.updateAgentRunResult).mock.calls.at(-1);
      expect(badCall?.[2].messages).toEqual(["keep"]);
    });

    it("rejects completion when messages is not an array", async () => {
      agentQueueMock.setAgentQueueRun({
        id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbba",
        thread_id: THREAD_SUMMARY.id,
        status: "running",
        mode: "direct",
        prompt: "Run request",
        system_prompt: null,
        run_result_status: null,
        run_result_messages: null,
        run_result_changes: null,
        run_error: null,
        created_at: FIXTURE_NOW,
        started_at: FIXTURE_NOW,
        completed_at: null,
      });
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows(threadAccessRow("Owner"));
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/assistant-runs/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbba/complete",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: { status: "success", messages: "done" as unknown as string[] },
      });
      await app.close();

      expect(response.statusCode).toBe(400);
      expect(response.json().title).toBe("Invalid payload");
    });

    it("forbids completion for users without thread access", async () => {
      const runId = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
      agentQueueMock.setAgentQueueRun({
        id: runId,
        thread_id: THREAD_SUMMARY.id,
        status: "running",
        mode: "direct",
        prompt: "Run request",
        system_prompt: null,
        run_result_status: null,
        run_result_messages: null,
        run_result_changes: null,
        run_error: null,
        created_at: FIXTURE_NOW,
        started_at: FIXTURE_NOW,
        completed_at: null,
      });
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryNoRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: `/v1/assistant-runs/${runId}/complete`,
        headers: { authorization: `Bearer ${VIEW_TOKEN}` },
        payload: { status: "success", messages: ["done"] },
      });
      await app.close();

      expect(response.statusCode).toBe(403);
      expect(response.json().title).toBe("Forbidden");
    });

    it("sanitizes messages and plan changes while completing", async () => {
      agentQueueMock.setAgentQueueRun({
        id: "aaaaaaaa-1111-4aaa-8aaa-111111111111",
        thread_id: THREAD_SUMMARY.id,
        status: "running",
        mode: "plan",
        prompt: "Run request",
        system_prompt: null,
        run_result_status: null,
        run_result_messages: null,
        run_result_changes: null,
        run_error: null,
        created_at: FIXTURE_NOW,
        started_at: FIXTURE_NOW,
        completed_at: null,
      });
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows(threadAccessRow("Owner"));
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/assistant-runs/aaaaaaaa-1111-4aaa-8aaa-111111111111/complete",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: {
          status: "failed",
          messages: ["  keep ", "", " second ", 10, "trim-me "],
          changes: [
            {
              target_table: "nodes",
              operation: "Create",
              target_id: { id: "n1" },
              previous: null,
              current: { kind: "Root" },
            },
            {
              target_table: "bad",
              operation: "Nope",
              target_id: {},
            },
          ],
          runnerId: "agent-1",
        },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      const args = vi.mocked(agentQueueMock.agentQueueMock.updateAgentRunResult).mock.calls.at(-1);
      expect(args?.[1]).toBe("failed");
      expect(args?.[2].messages).toEqual(["keep", "second", "trim-me"]);
      expect(args?.[2].changes).toEqual([
        {
          target_table: "nodes",
          operation: "Create",
          target_id: { id: "n1" },
          previous: null,
          current: { kind: "Root" },
        },
      ]);
      expect(response.json().status).toBe("failed");
      expect(response.json().runResultStatus).toBe("failed");
    });

    it("returns conflict if completion is already finalized", async () => {
      agentQueueMock.setAgentQueueRun({
        id: "bbbbbbbb-2222-4bbb-8bbb-222222222222",
        thread_id: THREAD_SUMMARY.id,
        status: "success",
        mode: "direct",
        prompt: "Run request",
        system_prompt: null,
        run_result_status: "success",
        run_result_messages: ["done"],
        run_result_changes: [],
        run_error: null,
        created_at: FIXTURE_NOW,
        started_at: FIXTURE_NOW,
        completed_at: FIXTURE_NOW,
      });
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows(threadAccessRow("Owner"));
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/assistant-runs/bbbbbbbb-2222-4bbb-8bbb-222222222222/complete",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        payload: {
          status: "success",
          messages: ["x"],
        },
      });
      await app.close();

      expect(response.statusCode).toBe(409);
      expect(response.json().title).toBe("Run cannot be completed");
    });
  });

  describe("POST /v1/assistant-runs/:runId/cancel", () => {
    it("validates run id", async () => {
      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/assistant-runs/not-a-uuid/cancel",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(400);
      expect(response.json().title).toBe("Invalid run id");
    });

    it("returns forbidden for run from inaccessible thread", async () => {
      agentQueueMock.setAgentQueueRun({
        id: "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        thread_id: THREAD_SUMMARY.id,
        status: "queued",
        mode: "direct",
        prompt: "Run request",
        system_prompt: null,
        run_result_status: null,
        run_result_messages: null,
        run_result_changes: null,
        run_error: null,
        created_at: FIXTURE_NOW,
        started_at: null,
        completed_at: null,
      });
      dbMock.setQueryHandler(async (text) => {
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/assistant-runs/cccccccc-cccc-4ccc-8ccc-cccccccccccc/cancel",
        headers: { authorization: `Bearer ${VIEW_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(403);
      expect(response.json().title).toBe("Forbidden");
    });

    it("returns 409 when already finalized", async () => {
      agentQueueMock.setAgentQueueRun({
        id: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        thread_id: THREAD_SUMMARY.id,
        status: "success",
        mode: "direct",
        prompt: "Run request",
        system_prompt: null,
        run_result_status: "success",
        run_result_messages: null,
        run_result_changes: null,
        run_error: null,
        created_at: FIXTURE_NOW,
        started_at: null,
        completed_at: FIXTURE_NOW,
      });
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows(threadAccessRow("Owner"));
        }
        if (text.includes("UPDATE agent_runs")) {
          return dbMock.queryNoRows();
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/assistant-runs/dddddddd-dddd-4ddd-8ddd-dddddddddddd/cancel",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(409);
      expect(response.json().detail).toBe("Run is already finalized");
    });

    it("cancels queued/running run and returns final row", async () => {
      agentQueueMock.setAgentQueueRun({
        id: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        thread_id: THREAD_SUMMARY.id,
        status: "queued",
        mode: "direct",
        prompt: "Run request",
        system_prompt: null,
        run_result_status: null,
        run_result_messages: null,
        run_result_changes: null,
        run_error: null,
        created_at: FIXTURE_NOW,
        started_at: null,
        completed_at: null,
      });
      dbMock.setQueryHandler(async (text) => {
        if (text.includes("FROM threads t")) {
          return dbMock.queryRows(threadAccessRow("Editor"));
        }
        if (text.includes("UPDATE agent_runs")) {
          return dbMock.queryRows({
            id: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
            thread_id: THREAD_SUMMARY.id,
            status: "cancelled",
            mode: "direct",
            prompt: "Run request",
            system_prompt: null,
            run_result_status: "failed",
            run_result_messages: [],
            run_error: "Cancelled by user",
            created_at: FIXTURE_NOW,
            started_at: null,
            completed_at: FIXTURE_NOW,
            run_result_changes: null,
          });
        }
        return dbMock.queryNoRows();
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "POST",
        url: "/v1/assistant-runs/eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee/cancel",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json().runId).toBe("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee");
      expect(response.json().status).toBe("cancelled");
      expect(response.json().runError).toBe("Cancelled by user");
    });
  });
});
