import type { MockAuthUser } from "./authMock.js";

export const FIXTURE_NOW = "2025-01-01T12:00:00.000Z";

export const ACTIVE_USER: MockAuthUser = {
  id: "11111111-1111-4111-8a11-111111111111",
  auth0Id: "auth0|owner-user",
  email: "owner@example.com",
  name: "Owner User",
  picture: null,
  handle: "owner-user",
  githubHandle: "owner-github",
  orgId: "11111111-aaaaaaaa-aaaa-aaaa-aaaaaaaaaaaa",
  scope: null,
  createdAt: new Date("2025-01-01T00:00:00.000Z"),
  updatedAt: new Date("2025-01-02T00:00:00.000Z"),
};

export const VIEWER_USER: MockAuthUser = {
  id: "22222222-2222-4222-8b22-222222222222",
  auth0Id: "auth0|viewer-user",
  email: "viewer@example.com",
  name: "Viewer User",
  picture: null,
  handle: "viewer",
  githubHandle: null,
  orgId: "11111111-aaaaaaaa-aaaa-aaaa-aaaaaaaaaaaa",
  scope: "read",
  createdAt: new Date("2025-02-01T00:00:00.000Z"),
  updatedAt: new Date("2025-02-02T00:00:00.000Z"),
};

export const EDITOR_USER: MockAuthUser = {
  id: "33333333-3333-4333-8c33-333333333333",
  auth0Id: "auth0|editor-user",
  email: "editor@example.com",
  name: "Editor User",
  picture: null,
  handle: "editor",
  githubHandle: null,
  orgId: "11111111-aaaaaaaa-aaaa-aaaa-aaaaaaaaaaaa",
  scope: "edit",
  createdAt: new Date("2025-03-01T00:00:00.000Z"),
  updatedAt: new Date("2025-03-02T00:00:00.000Z"),
};

export const TEAM_OWNER_USER: MockAuthUser = {
  id: "44444444-4444-4444-8d44-444444444444",
  auth0Id: "auth0|team-owner",
  email: "teamowner@example.com",
  name: "Team Owner",
  picture: null,
  handle: "teamowner",
  githubHandle: null,
  orgId: "11111111-aaaaaaaa-aaaa-aaaa-aaaaaaaaaaaa",
  scope: "admin",
  createdAt: new Date("2025-04-01T00:00:00.000Z"),
  updatedAt: new Date("2025-04-02T00:00:00.000Z"),
};

export const PUBLIC_PROJECT = {
  id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  name: "Public Project",
  description: "Public project for tests",
  visibility: "public" as const,
  owner_id: TEAM_OWNER_USER.id,
  owner_handle: TEAM_OWNER_USER.handle,
  access_role: "Owner" as const,
  created_at: new Date("2025-05-01T00:00:00.000Z"),
  thread_count: "3",
};

export const PRIVATE_PROJECT = {
  id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
  name: "Private Project",
  description: "Private project for tests",
  visibility: "private" as const,
  owner_id: TEAM_OWNER_USER.id,
  owner_handle: TEAM_OWNER_USER.handle,
  access_role: "Owner" as const,
  created_at: new Date("2025-05-02T00:00:00.000Z"),
  thread_count: "1",
};

export const THREAD_SUMMARY = {
  id: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
  title: "Thread summary",
  description: "Thread description",
  status: "open" as const,
  project_id: PUBLIC_PROJECT.id,
  project_name: PUBLIC_PROJECT.name,
  source_thread_id: null,
  created_by_handle: TEAM_OWNER_USER.handle,
  owner_handle: TEAM_OWNER_USER.handle,
  created_at: new Date("2025-06-01T00:00:00.000Z"),
  updated_at: new Date("2025-06-02T00:00:00.000Z"),
  access_role: "Owner" as const,
};

export const RUN_ROW = {
  id: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
  thread_id: THREAD_SUMMARY.id,
  status: "queued" as const,
  mode: "direct" as const,
  prompt: "Run request",
  system_prompt: null as string | null,
  run_result_status: null,
  run_result_messages: null,
  run_result_changes: null,
  run_error: null,
  created_at: FIXTURE_NOW,
  started_at: null as string | null,
  completed_at: null as string | null,
};

export const SSE_EVENT = {
  id: "evt-001",
  type: "assistant.run.started",
  aggregateType: "assistant-run",
  aggregateId: "run-event-1",
  occurredAt: FIXTURE_NOW,
  orgId: "11111111-aaaaaaaa-aaaa-aaaa-aaaaaaaaaaaa",
  traceId: THREAD_SUMMARY.id,
  payload: { threadId: THREAD_SUMMARY.id },
  version: 1,
};

export function makeMatrixCell(nodeId = "s.root", concern = "content") {
  return {
    nodeId,
    concern,
    docs: [{
      hash: `${nodeId}-${concern}-hash`,
      title: `Document ${nodeId}`,
      kind: "Document",
      language: "md",
      sourceType: "local",
      sourceUrl: null,
      sourceExternalId: null,
    }],
    artifacts: [],
  };
}
