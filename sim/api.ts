import { createHash } from "node:crypto";
import { randomInt } from "node:crypto";
import type { Pool, PoolClient, QueryResultRow } from "pg";
import pg from "pg";

export interface SimActorContext {
  actorId: string;
  handle: string;
  orgId: string | null;
}

export type AccessRole = "Owner" | "Editor" | "Viewer";
export type ProjectVisibility = "public" | "private";
export type ThreadStatus = "open" | "closed" | "committed";
export type AssistantType = "direct" | "plan";
export type AssistantRunStatus = "queued" | "running" | "success" | "failed" | "cancelled";
export type AssistantRunResultStatus = "success" | "failed";
export type StaffXEventType =
  | "chat.session.finished"
  | "assistant.run.started"
  | "assistant.run.progress"
  | "assistant.run.waiting_input"
  | "assistant.run.completed"
  | "assistant.run.failed"
  | "assistant.run.cancelled"
  | "thread.matrix.changed";

export interface ProjectRow {
  id: string;
  name: string;
  description: string | null;
  visibility: ProjectVisibility;
  accessRole: AccessRole;
  ownerHandle: string;
  createdAt: string;
  threadCount: number;
}

export interface ThreadRow {
  id: string;
  projectThreadId: number;
  title: string | null;
  description: string | null;
  status: ThreadStatus;
  sourceThreadId: string | null;
  projectId: string;
  createdByHandle: string;
  ownerHandle: string;
  projectName: string;
  createdAt: string;
  updatedAt: string;
  accessRole: AccessRole;
}

export interface MatrixCellDoc {
  hash: string;
  title: string;
  kind: string;
  language: string;
  sourceType: string;
  sourceUrl: string | null;
  sourceExternalId: string | null;
  sourceMetadata: Record<string, unknown> | null;
  sourceConnectedUserId: string | null;
}

export interface MatrixCellArtifact {
  id: string;
  concern: string;
  type: string;
  language: string;
  text: string | null;
}

export interface MatrixCell {
  nodeId: string;
  concern: string;
  docs: MatrixCellDoc[];
  artifacts: MatrixCellArtifact[];
}

export interface TopologyNode {
  id: string;
  name: string;
  kind: string;
  parentId: string | null;
  metadata: Record<string, unknown>;
}

export interface MatrixSnapshot {
  threadId: string;
  systemId: string;
  topology: {
    nodes: TopologyNode[];
    edges: Array<{
      id: string;
      type: string;
      fromNodeId: string;
      toNodeId: string;
      protocol?: string | null;
    }>;
  };
  concerns: Array<{ name: string; position: number }>;
  documents: MatrixCellDoc[];
  cells: MatrixCell[];
}

export interface ChatMessage {
  id: string;
  actionId: string;
  actionType: string;
  actionPosition: number;
  role: "User" | "Assistant" | "System";
  content: string;
  senderName?: string;
  createdAt: string;
}

export interface AssistantRun {
  runId: string;
  threadId: string;
  model: string;
  status: AssistantRunStatus;
  mode: AssistantType;
  prompt: string;
  systemPrompt: string | null;
  runResultStatus: AssistantRunResultStatus | null;
  runResultMessages: string[];
  runResultChanges: Array<Record<string, unknown>>;
  runError: string | null;
  createdAt: string;
  startedAt: string | null;
  completedAt: string | null;
  projectId: string;
  requestedByUserId: string | null;
  runnerId: string | null;
  planActionId: string | null;
  chatMessageId: string | null;
}

export interface StaffXEvent {
  id: string;
  type: StaffXEventType;
  aggregateType: string;
  aggregateId: string;
  occurredAt: string;
  orgId: string | null;
  traceId: string | null;
  payload: Record<string, unknown>;
  version: number;
}

export interface RunChangeRow {
  target_table: string;
  operation: "Create" | "Update" | "Delete";
  target_id: Record<string, unknown>;
  previous: Record<string, unknown> | null;
  current: Record<string, unknown> | null;
}

export interface SimApiOptions {
  databaseUrl?: string;
  existingPool?: Pool;
}

interface DbProjectAccessRow {
  project_id: string;
  owner_id: string;
  visibility: ProjectVisibility;
  owner_handle: string;
  is_archived: boolean;
  access_role: AccessRole | null;
  name: string;
  description: string | null;
  created_at: Date;
  thread_count: string;
}

interface DbThreadAccessRow {
  id: string;
  project_thread_id: number;
  title: string | null;
  description: string | null;
  status: ThreadStatus;
  source_thread_id: string | null;
  project_id: string;
  created_at: Date;
  updated_at: Date;
  created_by_handle: string;
  owner_handle: string;
  project_name: string;
  access_role: AccessRole;
}

interface DbTopologyNodeRow {
  id: string;
  name: string;
  kind: string;
  parent_id: string | null;
  metadata: Record<string, unknown>;
}

interface DbTopologyEdgeRow {
  id: string;
  from_node_id: string;
  to_node_id: string;
  type: string;
  metadata: Record<string, unknown>;
}

interface DbMessageRow {
  id: string;
  action_id: string;
  action_type: string;
  action_position: number;
  role: "User" | "Assistant" | "System";
  content: string;
  sender_model: string | null;
  created_at: Date;
}

interface DbMatrixDocRefRow {
  node_id: string;
  concern: string;
  hash: string;
  title: string;
  kind: string;
  language: string;
  source_type: string;
  source_url: string | null;
  source_external_id: string | null;
  source_metadata: Record<string, unknown> | null;
  source_connected_user_id: string | null;
}

interface DbArtifactRow {
  id: string;
  node_id: string;
  concern: string;
  type: string;
  language: string;
  text: string | null;
}

interface DbConcernRow {
  name: string;
  position: number;
}

interface DbRunRow {
  id: string;
  thread_id: string;
  model: string;
  status: AssistantRunStatus;
  mode: AssistantType;
  prompt: string;
  system_prompt: string | null;
  run_result_status: AssistantRunResultStatus | null;
  run_result_messages: string[] | null;
  run_result_changes: RunChangeRow[] | null;
  run_error: string | null;
  created_at: Date;
  started_at: Date | null;
  completed_at: Date | null;
  project_id: string;
  requested_by_user_id: string | null;
  runner_id: string | null;
  plan_action_id: string | null;
  chat_message_id: string | null;
}

interface DbEventRow {
  id: string;
  type: StaffXEventType;
  aggregate_type: string;
  aggregate_id: string;
  occurred_at: Date;
  org_id: string | null;
  trace_id: string | null;
  payload: Record<string, unknown>;
  version: number;
}

interface DbIdRow {
  id: string;
}

interface QueryEventsInput {
  orgId?: string | null;
  aggregateType?: string;
  aggregateId?: string;
  since?: string;
  limit?: number;
}

interface PublishEventInput {
  type: StaffXEventType;
  aggregateType: string;
  aggregateId: string;
  orgId: string | null;
  traceId?: string | null;
  payload: Record<string, unknown>;
  version?: number;
  occurredAt?: Date;
}

export class SimError extends Error {
  constructor(
    public status: number,
    message: string,
    public title = "Error",
  ) {
    super(message);
    this.name = "SimError";
  }
}

function randomSleepJitter() {
  return randomInt(0, 15);
}

function toISO(value: Date | string): string {
  return value instanceof Date ? value.toISOString() : new Date(value).toISOString();
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parsePositiveInt(raw: unknown, fallback: number, min = 1, max = 1_000): number {
  const candidate = typeof raw === "number" || typeof raw === "string" ? Number(raw) : Number.NaN;
  if (!Number.isFinite(candidate)) return fallback;
  const normalized = Math.trunc(candidate);
  if (normalized < min) return fallback;
  if (normalized > max) return max;
  return normalized;
}

function parseBoolean(raw: unknown, fallback: boolean) {
  if (raw === undefined || raw === null || raw === "") return fallback;
  const normalized = String(raw).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off", "n"].includes(normalized)) return false;
  return fallback;
}

function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function canEdit(role: AccessRole | null): role is Exclude<AccessRole, "Viewer"> {
  return role === "Owner" || role === "Editor";
}

function hashDigest(seed: string): string {
  return createHash("sha256").update(seed).digest("hex");
}

export function stableId(prefix: string, seed: string): string {
  return deterministicUuid(`${prefix}|${seed}`);
}

function deterministicUuid(seed: string): string {
  const digest = hashDigest(seed);
  const cleanDigest = digest.padEnd(32, "0").slice(0, 32);
  return `${cleanDigest.slice(0, 8)}-${cleanDigest.slice(8, 12)}-5${cleanDigest.slice(13, 16)}-a${cleanDigest.slice(17, 20)}-${cleanDigest.slice(20, 32)}`;
}

function normalizeModel(raw: string | undefined): string | null {
  const model = raw?.trim();
  if (!model) return "claude-opus-4-6";
  if (model === "gpt-5.3-codex") return "codex-5.3";
  if (
    model === "claude-opus-4-6" ||
    model === "claude-sonnet-4-6" ||
    model === "codex-5.3"
  ) {
    return model;
  }
  return null;
}

export function encodeEventCursor(event: StaffXEvent): string {
  return `${encodeURIComponent(event.occurredAt)}|${encodeURIComponent(event.id)}`;
}

function parseEventCursor(raw: string): { occurredAt: string; id: string } | null {
  const parts = raw.split("|");
  if (parts.length !== 2) return null;
  const occurredAt = decodeURIComponent(parts[0] ?? "");
  const id = decodeURIComponent(parts[1] ?? "");
  if (!occurredAt || !id) return null;
  const parsedDate = new Date(occurredAt);
  if (Number.isNaN(parsedDate.getTime())) return null;
  return { occurredAt: parsedDate.toISOString(), id };
}

function parseThreadRouteId(raw: string): { kind: "uuid"; id: string } | { kind: "project"; projectThreadId: number } | null {
  const trimmed = raw.trim();
  if (isUuid(trimmed)) return { kind: "uuid", id: trimmed };
  if (!/^\d+$/.test(trimmed)) return null;
  const value = Number(trimmed);
  if (!Number.isSafeInteger(value) || value <= 0) return null;
  return { kind: "project", projectThreadId: value };
}

function normalizeRole(input?: string | null): string | null {
  if (input !== "Owner" && input !== "Editor" && input !== "Viewer") return null;
  return input;
}

function normalizeRunPrompt(prompt?: string | null): string {
  const trimmed = prompt?.trim();
  return trimmed?.length ? trimmed : "Run this request.";
}

function sanitizeRoleForDb(value: string | undefined): "User" | "Assistant" | "System" {
  if (value === "Assistant" || value === "System") return value;
  return "User";
}

function parseEventSince(since?: string): {
  occurredAt: string;
  id: string;
} | {
  occurredAt: string;
  asCursor: false;
} | null {
  if (!since) return null;
  const cursor = parseEventCursor(since);
  if (cursor) return { ...cursor };
  const parsedDate = new Date(since);
  if (Number.isNaN(parsedDate.getTime())) return null;
  return { occurredAt: parsedDate.toISOString(), asCursor: false };
}

export class SimApi {
  private pool: {
    query: <T extends QueryResultRow = QueryResultRow>(
      text: string,
      params?: unknown[],
    ) => Promise<{ rows: T[]; rowCount: number | null }>;
    connect: () => Promise<{
      query: <T extends QueryResultRow = QueryResultRow>(
        text: string,
        params?: unknown[],
      ) => Promise<{ rows: T[]; rowCount: number | null }>;
      release: () => void;
    }>;
    end: () => Promise<void>;
  };
  private readonly defaultSystemPrompt =
    "Deterministic StaffX simulation run without external calls. " +
    "Perform a concrete, deterministic edit to the current thread system and record the result.";
  private readonly usePgMem: boolean;
  private readonly poolReady: Promise<void>;

  constructor(options: SimApiOptions = {}) {
    if (options.existingPool) {
      this.pool = options.existingPool as unknown as {
        query: <T extends QueryResultRow = QueryResultRow>(
          text: string,
          params?: unknown[],
        ) => Promise<{ rows: T[]; rowCount: number | null }>;
        connect: () => Promise<{
          query: <T extends QueryResultRow = QueryResultRow>(
            text: string,
            params?: unknown[],
          ) => Promise<{ rows: T[]; rowCount: number | null }>;
          release: () => void;
        }>;
        end: () => Promise<void>;
      };
      this.usePgMem = false;
      this.poolReady = Promise.resolve();
      return;
    }

    const databaseUrl = options.databaseUrl ?? process.env.DATABASE_URL;
    if (!databaseUrl) {
      throw new Error("DATABASE_URL is required when creating SimApi");
    }
    this.usePgMem = databaseUrl.startsWith("pgmem://");
    this.poolReady = this.usePgMem
      ? this.initializePgMemPool(databaseUrl)
      : this.initializePgPool(databaseUrl);
  }

  private static readonly defaultEventLimit = 100;

  async close(): Promise<void> {
    await this.poolReady;
    await this.pool.end();
  }

  get isUsingPgMem(): boolean {
    return this.usePgMem;
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    text: string,
    params: unknown[] = [],
  ): Promise<{ rows: T[]; rowCount: number | null }> {
    await this.poolReady;
    const result = await this.pool.query<T>(text, params);
    return { rows: result.rows, rowCount: result.rowCount };
  }

  async withTx<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
    await this.poolReady;
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const value = await fn(client as unknown as PoolClient);
      await client.query("COMMIT");
      return value;
    } catch (error) {
      try {
        await client.query("ROLLBACK");
      } catch {
        // ignored
      }
      throw error;
    } finally {
      client.release();
    }
  }

  private async initializePgPool(databaseUrl: string): Promise<void> {
    this.pool = new pg.Pool({ connectionString: databaseUrl });
  }

  private async initializePgMemPool(_databaseUrl: string): Promise<void> {
    let createDb: (options?: unknown) => any;
    try {
      ({ newDb: createDb } = (await import("pg-mem")) as { newDb: (options?: unknown) => any });
    } catch {
      throw new SimError(500, "pg-mem is required for SIM_USE_PG_MEM mode. Install it in sim package.");
    }

    const db = createDb({ autoCreateForeignKeyIndices: true });
    const pgAdapter = db.adapters.createPg();
    const pool = new (pgAdapter.Pool)();

    await pool.query(`
      CREATE TYPE node_kind AS ENUM ('Root', 'Host', 'Container', 'Process', 'Library');
    `);
    await pool.query("CREATE TYPE edge_type AS ENUM ('Runtime', 'Dataflow', 'Dependency');");
    await pool.query("CREATE TYPE doc_kind AS ENUM ('Document', 'Skill', 'Prompt');");
    await pool.query("CREATE TYPE ref_type AS ENUM ('Document', 'Skill', 'Prompt');");
    await pool.query("CREATE TYPE provider AS ENUM ('notion', 'google');");
    await pool.query("CREATE TYPE doc_source_type AS ENUM ('local', 'notion', 'google_doc');");
    await pool.query("CREATE TYPE artifact_type AS ENUM ('Summary', 'Code', 'Docs');");
    await pool.query("CREATE TYPE collaborator_role AS ENUM ('Editor', 'Viewer');");
    await pool.query("CREATE TYPE project_visibility AS ENUM ('public', 'private');");
    await pool.query("CREATE TYPE action_type AS ENUM ('Chat', 'Edit', 'Import', 'Plan', 'PlanResponse', 'Execute', 'ExecuteResponse', 'Update');");
    await pool.query("CREATE TYPE message_role AS ENUM ('User', 'Assistant', 'System');");
    await pool.query("CREATE TYPE change_operation AS ENUM ('Create', 'Update', 'Delete');");
    await pool.query("CREATE TYPE staffx_event_type AS ENUM ('chat.session.finished', 'assistant.run.started', 'assistant.run.progress', 'assistant.run.waiting_input', 'assistant.run.completed', 'assistant.run.failed', 'assistant.run.cancelled', 'thread.matrix.changed');");

    await pool.query(`
      CREATE TABLE users (
        id uuid PRIMARY KEY,
        auth0_id text UNIQUE NOT NULL,
        email text,
        name text,
        picture text,
        handle text UNIQUE NOT NULL,
        github_handle text,
        created_at timestamptz NOT NULL DEFAULT NOW(),
        updated_at timestamptz NOT NULL DEFAULT NOW()
      );
    `);
    await pool.query(`
      CREATE TABLE projects (
        id text PRIMARY KEY,
        name text NOT NULL,
        description text,
        visibility project_visibility NOT NULL DEFAULT 'private',
        owner_id uuid NOT NULL REFERENCES users(id),
        is_archived boolean NOT NULL DEFAULT false,
        created_at timestamptz NOT NULL DEFAULT NOW(),
        updated_at timestamptz NOT NULL DEFAULT NOW()
      );
    `);
    await pool.query(`
      CREATE TABLE project_collaborators (
        project_id text NOT NULL REFERENCES projects(id),
        user_id uuid NOT NULL REFERENCES users(id),
        role collaborator_role NOT NULL DEFAULT 'Editor'::collaborator_role,
        created_at timestamptz NOT NULL DEFAULT NOW(),
        PRIMARY KEY (project_id, user_id)
      );
    `);
    await pool.query(`
      CREATE TABLE systems (
        id text PRIMARY KEY,
        name text NOT NULL,
        spec_version text NOT NULL DEFAULT 'openship/v1',
        root_node_id text NOT NULL,
        metadata jsonb NOT NULL DEFAULT '{}',
        created_at timestamptz NOT NULL DEFAULT NOW(),
        updated_at timestamptz NOT NULL DEFAULT NOW()
      );
    `);
    await pool.query(`
      CREATE TABLE nodes (
        id text NOT NULL,
        system_id text NOT NULL REFERENCES systems(id) ON DELETE CASCADE,
        kind node_kind NOT NULL,
        name text NOT NULL,
        parent_id text,
        metadata jsonb NOT NULL DEFAULT '{}',
        created_at timestamptz NOT NULL DEFAULT NOW(),
        updated_at timestamptz NOT NULL DEFAULT NOW(),
        PRIMARY KEY (system_id, id),
        FOREIGN KEY (system_id, parent_id) REFERENCES nodes(system_id, id) ON DELETE SET NULL
      );
    `);
    await pool.query(`
      CREATE TABLE concerns (
        system_id text NOT NULL REFERENCES systems(id) ON DELETE CASCADE,
        name text NOT NULL,
        position int NOT NULL,
        is_baseline bool NOT NULL DEFAULT false,
        scope text,
        PRIMARY KEY (system_id, name)
      );
    `);
    await pool.query(`
      CREATE TABLE edges (
        id text NOT NULL,
        system_id text NOT NULL REFERENCES systems(id) ON DELETE CASCADE,
        type edge_type NOT NULL,
        from_node_id text NOT NULL,
        to_node_id text NOT NULL,
        metadata jsonb NOT NULL DEFAULT '{}',
        created_at timestamptz NOT NULL DEFAULT NOW(),
        updated_at timestamptz NOT NULL DEFAULT NOW(),
        PRIMARY KEY (system_id, id),
        FOREIGN KEY (system_id, from_node_id) REFERENCES nodes(system_id, id) ON DELETE CASCADE,
        FOREIGN KEY (system_id, to_node_id) REFERENCES nodes(system_id, id) ON DELETE CASCADE
      );
    `);
    await pool.query(`
      CREATE TABLE documents (
        hash text NOT NULL,
        system_id text NOT NULL REFERENCES systems(id) ON DELETE CASCADE,
        kind doc_kind NOT NULL,
        title text NOT NULL,
        language text NOT NULL DEFAULT 'en',
        source_type doc_source_type NOT NULL DEFAULT 'local',
        text text NOT NULL,
        source_url text,
        source_external_id text,
        source_metadata jsonb,
        source_connected_user_id uuid REFERENCES users(id) ON DELETE SET NULL,
        supersedes text,
        created_at timestamptz NOT NULL DEFAULT NOW(),
        PRIMARY KEY (system_id, hash)
      );
    `);
    await pool.query(`
      CREATE TABLE matrix_refs (
        system_id text NOT NULL,
        node_id text NOT NULL,
        concern text NOT NULL,
        concern_hash text NOT NULL,
        ref_type ref_type NOT NULL,
        doc_hash text NOT NULL,
        PRIMARY KEY (system_id, node_id, concern_hash, ref_type, doc_hash),
        FOREIGN KEY (system_id, node_id) REFERENCES nodes(system_id, id) ON DELETE CASCADE,
        FOREIGN KEY (system_id, concern) REFERENCES concerns(system_id, name) ON DELETE CASCADE,
        FOREIGN KEY (system_id, doc_hash) REFERENCES documents(system_id, hash) ON DELETE CASCADE
      );
    `);
    await pool.query(`
      CREATE TABLE artifacts (
        id text NOT NULL,
        system_id text NOT NULL,
        node_id text NOT NULL,
        concern text NOT NULL,
        type artifact_type NOT NULL,
        language text NOT NULL DEFAULT 'en',
        text text,
        created_at timestamptz NOT NULL DEFAULT NOW(),
        updated_at timestamptz NOT NULL DEFAULT NOW(),
        PRIMARY KEY (system_id, id),
        FOREIGN KEY (system_id, node_id) REFERENCES nodes(system_id, id) ON DELETE CASCADE,
        FOREIGN KEY (system_id, concern) REFERENCES concerns(system_id, name) ON DELETE CASCADE
      );
    `);
    await pool.query(`
      CREATE TABLE threads (
        id text PRIMARY KEY,
        title text,
        description text,
        project_id text NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
        created_by uuid NOT NULL REFERENCES users(id),
        seed_system_id text NOT NULL REFERENCES systems(id),
        project_thread_id integer NOT NULL,
        source_thread_id text REFERENCES threads(id),
        status text NOT NULL DEFAULT 'open',
        created_at timestamptz NOT NULL DEFAULT NOW(),
        updated_at timestamptz NOT NULL DEFAULT NOW()
      );
    `);
    await pool.query(`
      CREATE TABLE actions (
        id text NOT NULL,
        thread_id text NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
        position int NOT NULL,
        type action_type NOT NULL,
        title text,
        output_system_id text REFERENCES systems(id),
        created_at timestamptz NOT NULL DEFAULT NOW(),
        PRIMARY KEY (thread_id, id),
        UNIQUE(thread_id, position)
      );
    `);
    await pool.query(`
      CREATE TABLE messages (
        id text NOT NULL,
        thread_id text NOT NULL,
        action_id text NOT NULL,
        role message_role NOT NULL,
        content text NOT NULL,
        position int NOT NULL,
        created_at timestamptz NOT NULL DEFAULT NOW(),
        PRIMARY KEY (thread_id, action_id, id),
        FOREIGN KEY (thread_id, action_id) REFERENCES actions(thread_id, id) ON DELETE CASCADE
      );
    `);
    await pool.query(`
      CREATE TABLE changes (
        id text NOT NULL,
        thread_id text NOT NULL,
        action_id text NOT NULL,
        target_table text NOT NULL,
        operation change_operation NOT NULL,
        target_id jsonb NOT NULL,
        previous jsonb,
        current jsonb,
        created_at timestamptz NOT NULL DEFAULT NOW(),
        PRIMARY KEY (thread_id, action_id, id),
        FOREIGN KEY (thread_id, action_id) REFERENCES actions(thread_id, id) ON DELETE CASCADE
      );
    `);
    await pool.query(`
      CREATE TABLE agent_runs (
        id uuid PRIMARY KEY,
        thread_id text NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
        project_id text NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
        requested_by_user_id uuid REFERENCES users(id) ON DELETE SET NULL,
        model text NOT NULL,
        mode text NOT NULL,
        plan_action_id text,
        chat_message_id text,
        prompt text NOT NULL,
        system_prompt text,
        status text NOT NULL DEFAULT 'queued',
        runner_id text,
        run_result_status text,
        run_result_messages text[],
        run_result_changes jsonb,
        run_error text,
        created_at timestamptz NOT NULL DEFAULT NOW(),
        started_at timestamptz,
        completed_at timestamptz,
        updated_at timestamptz NOT NULL DEFAULT NOW()
      );
    `);
    await pool.query(`
      CREATE TABLE staffx_events (
        id uuid PRIMARY KEY,
        type staffx_event_type NOT NULL,
        aggregate_type text NOT NULL,
        aggregate_id text NOT NULL,
        org_id text,
        trace_id text,
        payload jsonb NOT NULL DEFAULT '{}',
        version int NOT NULL DEFAULT 1,
        occurred_at timestamptz NOT NULL DEFAULT NOW(),
        created_at timestamptz NOT NULL DEFAULT NOW()
      );
    `);
    this.pool = pool;
  }

  async ensureUser(input: { handle: string; orgId?: string | null }): Promise<SimActorContext> {
    const handle = input.handle.trim();
    if (!handle) {
      throw new SimError(400, "handle is required");
    }

    const result = await this.query<DbIdRow>(
      `INSERT INTO users (id, auth0_id, handle, updated_at)
       VALUES ($1, $2, $3, NOW())
       ON CONFLICT (handle) DO UPDATE
       SET auth0_id = EXCLUDED.auth0_id, updated_at = NOW()
       RETURNING id`,
      [stableId("usr", handle), `sim://${handle}`, handle],
    );

    if (!result.rows[0]) {
      throw new SimError(500, "Failed to bootstrap user", "USER_BOOTSTRAP_FAILED");
    }

    const actorId = result.rows[0]!.id;
    return { actorId, handle, orgId: input.orgId ?? null };
  }

  private mapProjectRow(row: DbProjectAccessRow): ProjectRow {
    return {
      id: row.project_id,
      name: row.name,
      description: row.description,
      visibility: row.visibility,
      accessRole: row.access_role ?? "Viewer",
      ownerHandle: row.owner_handle,
      createdAt: toISO(row.created_at),
      threadCount: Number.parseInt(row.thread_count ?? "0", 10),
    };
  }

  private mapThreadRow(row: DbThreadAccessRow): ThreadRow {
    return {
      id: row.id,
      projectThreadId: row.project_thread_id,
      title: row.title,
      description: row.description,
      status: row.status,
      sourceThreadId: row.source_thread_id,
      projectId: row.project_id,
      createdByHandle: row.created_by_handle,
      ownerHandle: row.owner_handle,
      projectName: row.project_name,
      createdAt: toISO(row.created_at),
      updatedAt: toISO(row.updated_at),
      accessRole: row.access_role,
    };
  }

  private mapAssistantRun(row: DbRunRow): AssistantRun {
    const normalizedMessages = Array.isArray(row.run_result_messages)
      ? row.run_result_messages
      : typeof row.run_result_messages === "string"
        ? (() => {
          try {
            return JSON.parse(row.run_result_messages) as string[];
          } catch {
            return [];
          }
        })()
        : [];
    const normalizedChanges = row.run_result_changes
      ? (typeof row.run_result_changes === "string"
        ? (() => {
          try {
            return JSON.parse(row.run_result_changes as unknown as string) as RunChangeRow[];
          } catch {
            return [];
          }
        })()
        : row.run_result_changes)
      : [];
    return {
      runId: row.id,
      threadId: row.thread_id,
      model: row.model,
      status: row.status,
      mode: row.mode,
      prompt: row.prompt,
      systemPrompt: row.system_prompt,
      runResultStatus: row.run_result_status,
      runResultMessages: normalizedMessages,
      runResultChanges: normalizedChanges,
      runError: row.run_error,
      createdAt: toISO(row.created_at),
      startedAt: row.started_at ? toISO(row.started_at) : null,
      completedAt: row.completed_at ? toISO(row.completed_at) : null,
      projectId: row.project_id,
      requestedByUserId: row.requested_by_user_id,
      runnerId: row.runner_id,
      planActionId: row.plan_action_id,
      chatMessageId: row.chat_message_id,
    };
  }

  private mapMatrixCellMap(
    docs: Array<DbMatrixDocRefRow>,
    artifacts: Array<DbArtifactRow>,
  ): Map<string, MatrixCell> {
    const cellMap = new Map<string, MatrixCell>();
    const cellKey = (nodeId: string, concern: string) => `${nodeId}|${concern}`;

    for (const doc of docs) {
      const key = cellKey(doc.node_id, doc.concern);
      const existing = cellMap.get(key);
      const mappedDoc: MatrixCellDoc = {
        hash: doc.hash,
        title: doc.title,
        kind: doc.kind,
        language: doc.language,
        sourceType: doc.source_type,
        sourceUrl: doc.source_url,
        sourceExternalId: doc.source_external_id,
        sourceMetadata: doc.source_metadata,
        sourceConnectedUserId: doc.source_connected_user_id,
      };

      if (existing) {
        existing.docs.push(mappedDoc);
      } else {
        cellMap.set(key, {
          nodeId: doc.node_id,
          concern: doc.concern,
          docs: [mappedDoc],
          artifacts: [],
        });
      }
    }

    for (const artifact of artifacts) {
      const key = cellKey(artifact.node_id, artifact.concern);
      const mappedArtifact: MatrixCellArtifact = {
        id: artifact.id,
        concern: artifact.concern,
        type: artifact.type,
        language: artifact.language,
        text: artifact.text,
      };
      const existing = cellMap.get(key);
      if (existing) {
        existing.artifacts.push(mappedArtifact);
      } else {
        cellMap.set(key, {
          nodeId: artifact.node_id,
          concern: artifact.concern,
          docs: [],
          artifacts: [mappedArtifact],
        });
      }
    }

    return cellMap;
  }

  private async buildNodeMetadataWithLayout(
    systemId: string,
    nodeId: string,
    x: number,
    y: number,
  ): Promise<string> {
    const current = await this.query<{ metadata: unknown }>(
      "SELECT metadata FROM nodes WHERE system_id = $1 AND id = $2",
      [systemId, nodeId],
    );
    const row = current.rows[0];
    const existing = row?.metadata;
    const base = typeof existing === "object" && existing !== null && !Array.isArray(existing)
      ? (existing as Record<string, unknown>)
      : {};
    const layout = typeof base.layout === "object" && base.layout !== null && !Array.isArray(base.layout)
      ? (base.layout as Record<string, unknown>)
      : {};
    const merged = { ...base, layout: { ...layout, x, y } };
    return JSON.stringify(merged);
  }

  private mapEventRow(row: DbEventRow): StaffXEvent {
    const normalizedPayload = typeof row.payload === "string"
      ? (() => {
        try {
          return JSON.parse(row.payload);
        } catch {
          return {};
        }
      })()
      : row.payload;
    return {
      id: row.id,
      type: row.type,
      aggregateType: row.aggregate_type,
      aggregateId: row.aggregate_id,
      occurredAt: toISO(row.occurred_at),
      orgId: row.org_id,
      traceId: row.trace_id,
      payload: normalizedPayload,
      version: row.version,
    };
  }

  private async resolveProjectAccess(
    projectId: string,
    actor: SimActorContext,
  ): Promise<DbProjectAccessRow | null> {
    const result = await this.query<DbProjectAccessRow>(
      `SELECT
         p.id AS project_id,
         p.owner_id,
         p.visibility::text AS visibility,
         owner_u.handle AS owner_handle,
         p.is_archived,
         COALESCE(
           CASE WHEN pc.role::text = '' THEN NULL ELSE pc.role::text END,
           CASE
             WHEN p.visibility = 'public' THEN 'Viewer'::text
             WHEN p.owner_id = $2 THEN 'Owner'::text
             ELSE NULL
           END
         )::text AS access_role,
         p.name,
         p.description,
         p.created_at,
         COALESCE(tc.thread_count, '0') AS thread_count
       FROM projects p
       JOIN users owner_u ON owner_u.id = p.owner_id
       LEFT JOIN project_collaborators pc
         ON pc.project_id = p.id
        AND pc.user_id = $2
       LEFT JOIN (
         SELECT project_id, COUNT(*)::text AS thread_count
         FROM threads
         GROUP BY project_id
       ) tc ON tc.project_id = p.id
       WHERE p.id = $1`,
      [projectId, actor.actorId],
    );

    const row = result.rows[0];
    if (!row || row.is_archived) {
      return null;
    }

    return { ...row, visibility: row.visibility, access_role: normalizeRole(row.access_role as unknown as string | null) };
  }

  private async resolveThreadAccess(
    threadIdInput: string,
    actor: SimActorContext,
  ): Promise<DbThreadAccessRow | null> {
    const parsed = parseThreadRouteId(threadIdInput);
    if (!parsed) return null;

    const result = parsed.kind === "uuid"
      ? await this.query<DbThreadAccessRow>(
        `SELECT
           t.id,
           t.project_thread_id,
           t.title,
           t.description,
           t.status::text AS status,
           t.source_thread_id,
           t.project_id,
           t.created_at,
           t.updated_at,
           u.handle AS created_by_handle,
           owner_u.handle AS owner_handle,
           p.name AS project_name,
           COALESCE(
             CASE WHEN pc.role::text = '' THEN NULL ELSE pc.role::text END,
             CASE
               WHEN p.owner_id = $2 THEN 'Owner'::text
               WHEN p.visibility = 'public' THEN 'Viewer'::text
               ELSE NULL
             END
           )::text AS access_role
         FROM threads t
         JOIN projects p ON p.id = t.project_id
         JOIN users u ON u.id = t.created_by
         JOIN users owner_u ON owner_u.id = p.owner_id
         LEFT JOIN project_collaborators pc
           ON pc.project_id = p.id
          AND pc.user_id = $2
         WHERE t.id = $1
           AND p.is_archived = false`,
        [parsed.id, actor.actorId],
      )
      : await this.query<DbThreadAccessRow>(
        `SELECT
           t.id,
           t.project_thread_id,
           t.title,
           t.description,
           t.status::text AS status,
           t.source_thread_id,
           t.project_id,
           t.created_at,
           t.updated_at,
           u.handle AS created_by_handle,
           owner_u.handle AS owner_handle,
           p.name AS project_name,
           COALESCE(
             CASE WHEN pc.role::text = '' THEN NULL ELSE pc.role::text END,
             CASE
               WHEN p.owner_id = $2 THEN 'Owner'::text
               WHEN p.visibility = 'public' THEN 'Viewer'::text
               ELSE NULL
             END
           )::text AS access_role
         FROM threads t
         JOIN projects p ON p.id = t.project_id
         JOIN users u ON u.id = t.created_by
         JOIN users owner_u ON owner_u.id = p.owner_id
         LEFT JOIN project_collaborators pc
           ON pc.project_id = p.id
          AND pc.user_id = $2
         WHERE t.project_thread_id = $1
           AND p.is_archived = false
         LIMIT 1`,
        [parsed.projectThreadId, actor.actorId],
      );

    const row = result.rows[0];
    if (!row || !row.access_role) return null;
    return {
      ...row,
      access_role: normalizeRole(row.access_role as unknown as string) ?? "Viewer",
    };
  }

  async getThreadSystemId(threadId: string): Promise<string | null> {
    const output = await this.query<{ output_system_id: string }>(
      `SELECT output_system_id
       FROM actions
       WHERE thread_id = $1
         AND output_system_id IS NOT NULL
       ORDER BY position DESC
       LIMIT 1`,
      [threadId],
    );

    const topOutputSystem = output.rows[0]?.output_system_id;
    if (topOutputSystem && isUuid(topOutputSystem)) {
      return topOutputSystem;
    }

    const seed = await this.query<{ seed_system_id: string }>(
      `SELECT seed_system_id FROM threads WHERE id = $1`,
      [threadId],
    );
    const seedSystem = seed.rows[0]?.seed_system_id;
    return isUuid(seedSystem) ? seedSystem : null;
  }

  async threadCurrentSystem(threadId: string): Promise<string | null> {
    return this.getThreadSystemId(threadId);
  }

  async listProjects(
    actor: SimActorContext,
    input: { name?: string; page?: number; pageSize?: number } = {},
  ): Promise<{ items: ProjectRow[]; page: number; pageSize: number; nextCursor: string | null }> {
    const page = parsePositiveInt(input.page, 1, 1, 10_000);
    const pageSize = parsePositiveInt(input.pageSize, 50, 1, 200);
    const offset = (page - 1) * pageSize;
    const params: unknown[] = [actor.actorId];
    const nameFilter = input.name?.trim();
    const nameFilterValue = nameFilter ? nameFilter.toLowerCase() : null;
    const whereName = nameFilter ? `AND LOWER(p.name) LIKE $${params.length + 1}` : "";
    if (nameFilterValue) params.push(`%${nameFilterValue}%`);
    const projectQueryParams = this.usePgMem
      ? params
      : [...params, pageSize + 1, offset];
    const projectLimit = this.usePgMem ? `${pageSize + 1}` : `$${params.length + 1}`;
    const projectOffset = this.usePgMem ? `${offset}` : `$${params.length + 2}`;

    const result = await this.query<DbProjectAccessRow>(
      `SELECT
         p.id AS project_id,
         p.name,
         p.description,
         p.visibility::text AS visibility,
         p.owner_id,
         owner_u.handle AS owner_handle,
         p.is_archived,
         COALESCE(
           CASE WHEN pc.role::text = '' THEN NULL ELSE pc.role::text END,
           CASE
             WHEN p.owner_id = $1 THEN 'Owner'::text
             WHEN p.visibility = 'public' THEN 'Viewer'::text
             ELSE NULL
           END
         )::text AS access_role,
         p.created_at,
         COALESCE(tc.thread_count, '0') AS thread_count
       FROM projects p
       JOIN users owner_u ON owner_u.id = p.owner_id
       LEFT JOIN project_collaborators pc
         ON pc.project_id = p.id
        AND pc.user_id = $1
       LEFT JOIN (
         SELECT project_id, COUNT(*)::text AS thread_count
         FROM threads
         GROUP BY project_id
       ) tc ON tc.project_id = p.id
       WHERE p.is_archived = false
         AND (p.visibility = 'public' OR p.owner_id = $1 OR pc.user_id IS NOT NULL)
         ${whereName}
       ORDER BY p.created_at DESC
       LIMIT ${projectLimit} OFFSET ${projectOffset}`,
      projectQueryParams,
    );

    const hasMore = result.rows.length > pageSize;
    const rows = hasMore ? result.rows.slice(0, pageSize) : result.rows;

    return {
      page,
      pageSize,
      nextCursor: hasMore ? String(page + 1) : null,
      items: rows.map((row) => this.mapProjectRow(row)),
    };
  }

  async checkProjectName(actor: SimActorContext, name: string): Promise<{ available: boolean }> {
    const normalized = name.trim();
    if (!normalized) {
      throw new SimError(400, "Invalid name");
    }
    const result = await this.query<DbIdRow>(
      "SELECT 1 FROM projects WHERE owner_id = $1 AND name = $2",
      [actor.actorId, normalized],
    );
    return { available: result.rowCount === 0 };
  }

  async createProject(
    actor: SimActorContext,
    input: {
      name: string;
      description?: string | null;
      visibility?: ProjectVisibility;
    },
  ): Promise<ProjectRow & { threadId: string }> {
    const name = input.name.trim();
    if (!name) {
      throw new SimError(400, "Invalid project name", "Name is required.");
    }

    if (!/^[a-zA-Z0-9]([a-zA-Z0-9 _-]{0,78}[a-zA-Z0-9])?$/.test(name)) {
      throw new SimError(400, "Invalid project name");
    }

    const description = input.description?.trim() ?? null;
    const visibility = input.visibility === "public" ? "public" : "private";

    const exists = await this.query<DbIdRow>(
      "SELECT 1 FROM projects WHERE owner_id = $1 AND name = $2",
      [actor.actorId, name],
    );
    if (exists.rowCount && exists.rowCount > 0) {
      throw new SimError(409, "Project already exists");
    }

    const projectId = stableId("prj", `${actor.actorId}|${name}`);
    const threadId = stableId("th", `${projectId}|seed`);
    const systemId = stableId("sys", `${projectId}|seed`);
    const rootNodeId = "root";
    const rootConcern = "General";
    const nextProjectThreadIdResult = await this.query<{ next_project_thread_id: number }>(
      `SELECT COALESCE(MAX(project_thread_id), 0) + 1 AS next_project_thread_id FROM threads`,
      [],
    );
    const nextProjectThreadIdSource = nextProjectThreadIdResult.rows[0]?.next_project_thread_id;
    const nextProjectThreadId = Number.isInteger(Number(nextProjectThreadIdSource))
      ? Number(nextProjectThreadIdSource)
      : 1;

    await this.withTx(async (client) => {
      await client.query(
        `INSERT INTO projects (id, name, description, visibility, owner_id)
         VALUES ($1, $2, $3, $4::project_visibility, $5)`,
        [projectId, name, description, visibility, actor.actorId],
      );

      await client.query(
        `INSERT INTO systems (id, name, root_node_id, metadata, spec_version)
         VALUES ($1, $2, $3, '{}'::jsonb, 'openship/v1')`,
        [systemId, `${name} system`, rootNodeId],
      );

      await client.query(
        `INSERT INTO nodes (id, system_id, kind, name, parent_id, metadata)
         VALUES ($1, $2, 'Root'::node_kind, $3, NULL, '{}'::jsonb)`,
        [rootNodeId, systemId, name],
      );

      await client.query(
        `INSERT INTO concerns (system_id, name, position, is_baseline, scope)
         VALUES ($1, $2, 0, TRUE, 'system')`,
        [systemId, rootConcern],
      );

      await client.query(
        `INSERT INTO threads (id, title, description, project_id, created_by, seed_system_id, project_thread_id, status)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'open')`,
        [
          threadId,
          `${name} Thread`,
          description,
          projectId,
          actor.actorId,
          systemId,
          nextProjectThreadId,
        ],
      );
    });

    const created = await this.query<DbProjectAccessRow>(
      `SELECT
         p.id AS project_id,
         p.name,
         p.description,
         p.visibility::text AS visibility,
         owner_u.handle AS owner_handle,
         p.created_at,
         1::text AS thread_count,
         p.owner_id,
         p.is_archived,
         'Owner'::text AS access_role
       FROM projects p
       JOIN users owner_u ON owner_u.id = p.owner_id
       WHERE p.id = $1`,
      [projectId],
    );

    const row = created.rows[0];
    if (!row) {
      throw new SimError(500, "Project creation failed");
    }

    return {
      ...this.mapProjectRow(row),
      threadId,
      accessRole: "Owner",
    };
  }

  async addProjectCollaborator(
    actor: SimActorContext,
    projectId: string,
    collaboratorUserId: string,
    role: Exclude<AccessRole, "Owner">,
  ): Promise<void> {
    const access = await this.resolveProjectAccess(projectId, actor);
    if (!access || access.access_role !== "Owner") {
      throw new SimError(403, "Forbidden");
    }

    if (!isUuid(collaboratorUserId)) {
      throw new SimError(400, "Invalid user identifier");
    }

    await this.query(
      `INSERT INTO project_collaborators (project_id, user_id, role)
       VALUES ($1, $2, $3::collaborator_role)
       ON CONFLICT (project_id, user_id)
       DO UPDATE SET role = EXCLUDED.role`,
      [projectId, collaboratorUserId, role],
    );
  }

  async listThreads(
    actor: SimActorContext,
    input: { projectId?: string; page?: number; pageSize?: number } = {},
  ): Promise<{ items: ThreadRow[]; page: number; pageSize: number; nextCursor: string | null }> {
    const page = parsePositiveInt(input.page, 1, 1, 10_000);
    const pageSize = parsePositiveInt(input.pageSize, 50, 1, 200);
    const offset = (page - 1) * pageSize;

    const params: unknown[] = [actor.actorId];
    const projectClause = input.projectId ? `AND t.project_id = $${params.length + 1}` : "";
    if (input.projectId) params.push(input.projectId);
    const threadQueryParams = this.usePgMem
      ? params
      : [...params, pageSize + 1, offset];
    const threadLimit = this.usePgMem ? `${pageSize + 1}` : `$${params.length + 1}`;
    const threadOffset = this.usePgMem ? `${offset}` : `$${params.length + 2}`;

    const result = await this.query<DbThreadAccessRow>(
      `SELECT
         t.id,
         t.project_thread_id,
         t.title,
         t.description,
         t.status::text AS status,
         t.source_thread_id,
         t.project_id,
         t.created_at,
         t.updated_at,
         u.handle AS created_by_handle,
         owner_u.handle AS owner_handle,
         p.name AS project_name,
         COALESCE(
           CASE WHEN pc.role::text = '' THEN NULL ELSE pc.role::text END,
           CASE
             WHEN p.owner_id = $1 THEN 'Owner'::text
             ELSE 'Viewer'::text
           END
         )::text AS access_role
       FROM threads t
       JOIN projects p ON p.id = t.project_id
       JOIN users u ON u.id = t.created_by
       JOIN users owner_u ON owner_u.id = p.owner_id
       LEFT JOIN project_collaborators pc
         ON pc.project_id = p.id
        AND pc.user_id = $1
       WHERE p.is_archived = false
         AND (p.visibility = 'public' OR p.owner_id = $1 OR pc.user_id IS NOT NULL)
         ${projectClause}
       ORDER BY t.updated_at DESC
       LIMIT ${threadLimit} OFFSET ${threadOffset}`,
      threadQueryParams,
    );

    const hasMore = result.rows.length > pageSize;
    const rows = hasMore ? result.rows.slice(0, pageSize) : result.rows;
    return {
      items: rows.map((row) => this.mapThreadRow(row)),
      page,
      pageSize,
      nextCursor: hasMore ? String(page + 1) : null,
    };
  }

  async createThread(
    actor: SimActorContext,
    input: { projectId: string; title?: string; description?: string | null; sourceThreadId?: string | null },
  ): Promise<ThreadRow> {
    const projectId = input.projectId.trim();
    if (!projectId || !isUuid(projectId)) {
      throw new SimError(400, "Invalid projectId");
    }

    const project = await this.resolveProjectAccess(projectId, actor);
    if (!project) {
      throw new SimError(403, "Project not found or access denied");
    }
    if (!canEdit(project.access_role)) {
      throw new SimError(403, "Forbidden");
    }

    const sourceInput = input.sourceThreadId?.trim();
    if (sourceInput && !isUuid(sourceInput)) {
      throw new SimError(400, "Invalid sourceThreadId");
    }

    const source = sourceInput
      ? await this.query<DbIdRow>("SELECT id FROM threads WHERE id = $1 AND project_id = $2", [sourceInput, projectId])
      : await this.query<DbIdRow>(
        `SELECT id FROM threads
         WHERE project_id = $1
         ORDER BY updated_at DESC LIMIT 1`,
        [projectId],
      );

    if (!source.rows[0]) {
      throw new SimError(400, "No source thread");
    }

    const nextIndexResult = await this.query<{ next_project_thread_id: number }>(
      `SELECT COALESCE(MAX(project_thread_id), 0) + 1 AS next_project_thread_id
       FROM threads`,
      [],
    );
    const nextProjectThreadIdSource = nextIndexResult.rows[0]?.next_project_thread_id;
    const nextProjectThreadId = Number.isInteger(Number(nextProjectThreadIdSource))
      ? Number(nextProjectThreadIdSource)
      : 1;

    const resolvedTitle = (input.title?.trim()) ?? `Thread ${nextProjectThreadId}`;
    const resolvedDescription = input.description?.trim() ?? null;
    const threadId = stableId("th", `${projectId}|${source.rows[0]!.id}|${nextProjectThreadId}|${resolvedTitle}`);

    const sourceSystem = await this.getThreadSystemId(source.rows[0]!.id);
    if (!sourceSystem) {
      throw new SimError(500, "Source thread has no current system");
    }

    await this.query(
      `INSERT INTO threads (id, title, description, project_id, created_by, seed_system_id, project_thread_id, source_thread_id, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'open')`,
      [
        threadId,
        resolvedTitle,
        resolvedDescription,
        projectId,
        actor.actorId,
        sourceSystem,
        nextProjectThreadId,
        source.rows[0]!.id,
      ],
    );

    const created = await this.query<DbThreadAccessRow>(
      `SELECT
         t.id,
         t.project_thread_id,
         t.title,
         t.description,
         t.status::text AS status,
         t.source_thread_id,
         t.project_id,
         t.created_at,
         t.updated_at,
         u.handle AS created_by_handle,
         owner_u.handle AS owner_handle,
         p.name AS project_name,
         'Owner'::text AS access_role
       FROM threads t
       JOIN projects p ON p.id = t.project_id
       JOIN users u ON u.id = t.created_by
       JOIN users owner_u ON owner_u.id = p.owner_id
       WHERE t.id = $1`,
      [threadId],
    );

    const row = created.rows[0];
    if (!row) {
      throw new SimError(500, "Thread creation failed");
    }

    return this.mapThreadRow(row);
  }

  async getThread(actor: SimActorContext, threadIdInput: string): Promise<ThreadRow> {
    const thread = await this.resolveThreadAccess(threadIdInput, actor);
    if (!thread) {
      if (!parseThreadRouteId(threadIdInput)) {
        throw new SimError(400, "Invalid threadId");
      }
      throw new SimError(404, "Thread not found");
    }
    return this.mapThreadRow(thread);
  }

  async patchThread(
    actor: SimActorContext,
    threadIdInput: string,
    input: { title?: string; description?: string | null; status?: ThreadStatus },
  ): Promise<ThreadRow> {
    const thread = await this.resolveThreadAccess(threadIdInput, actor);
    if (!thread) {
      if (!parseThreadRouteId(threadIdInput)) {
        throw new SimError(400, "Invalid threadId");
      }
      throw new SimError(404, "Thread not found");
    }
    if (!canEdit(thread.access_role)) {
      throw new SimError(403, "Forbidden");
    }

    const updates: string[] = [];
    const params: unknown[] = [];

    if (input.title !== undefined) {
      const title = input.title?.trim();
      if (!title) {
        throw new SimError(400, "Invalid title");
      }
      updates.push(`title = $${updates.length + 1}`);
      params.push(title);
    }

    if (input.description !== undefined) {
      updates.push(`description = $${updates.length + 1}`);
      params.push(input.description?.trim() ?? null);
    }

    if (input.status !== undefined) {
      if (input.status !== "open" && input.status !== "closed" && input.status !== "committed") {
        throw new SimError(400, "Invalid status");
      }
      updates.push(`status = $${updates.length + 1}`);
      params.push(input.status);
    }

    if (updates.length === 0) {
      throw new SimError(400, "No changes");
    }

    const result = await this.query<DbThreadAccessRow>(
      `WITH updated_thread AS (
         UPDATE threads
         SET ${updates.join(", ")}
         WHERE id = $${updates.length + 1}
         RETURNING
           id,
           project_thread_id,
           title,
           description,
           status::text AS status,
           source_thread_id,
           project_id,
           created_at,
           updated_at,
           created_by AS created_by_id
      )
      SELECT
        updated_thread.id,
        updated_thread.project_thread_id,
        updated_thread.title,
        updated_thread.description,
        updated_thread.status::text AS status,
        updated_thread.source_thread_id,
        updated_thread.project_id,
        updated_thread.created_at,
        updated_thread.updated_at,
        u.handle AS created_by_handle,
        owner_u.handle AS owner_handle,
        p.name AS project_name,
        'Owner'::text AS access_role
      FROM updated_thread
      JOIN users u ON u.id = updated_thread.created_by_id
      JOIN projects p ON p.id = updated_thread.project_id
      JOIN users owner_u ON owner_u.id = p.owner_id
      WHERE updated_thread.id = $${updates.length + 1}`
,
      [...params, thread.id],
    );

    if (!result.rows[0]) {
      throw new SimError(404, "Thread not found");
    }

    const row = result.rows[0];
    return {
      ...this.mapThreadRow({
        ...row,
        access_role: "Owner",
      }),
    };
  }

  async deleteThread(actor: SimActorContext, threadIdInput: string): Promise<void> {
    const thread = await this.resolveThreadAccess(threadIdInput, actor);
    if (!thread) {
      if (!parseThreadRouteId(threadIdInput)) {
        throw new SimError(400, "Invalid threadId");
      }
      throw new SimError(404, "Thread not found");
    }
    if (!canEdit(thread.access_role)) {
      throw new SimError(403, "Forbidden");
    }
    await this.query("DELETE FROM threads WHERE id = $1", [thread.id]);
  }

  async listMessages(actor: SimActorContext, threadIdInput: string): Promise<ChatMessage[]> {
    const thread = await this.resolveThreadAccess(threadIdInput, actor);
    if (!thread) {
      if (!parseThreadRouteId(threadIdInput)) {
        throw new SimError(400, "Invalid threadId");
      }
      throw new SimError(404, "Thread not found");
    }

    const result = await this.query<DbMessageRow>(
      `SELECT m.id, m.action_id, a.type::text AS action_type, a.position AS action_position, m.role, m.content, m.created_at, null::text AS sender_model
       FROM messages m
       JOIN actions a ON a.thread_id = m.thread_id AND a.id = m.action_id
       WHERE m.thread_id = $1
       ORDER BY a.position, m.created_at`,
      [thread.id],
    );

    return result.rows.map((row) => ({
      id: row.id,
      actionId: row.action_id,
      actionType: row.action_type,
      actionPosition: row.action_position,
      role: row.role,
      content: row.content,
      senderName: row.sender_model ? String(row.sender_model) : undefined,
      createdAt: toISO(row.created_at),
    }));
  }

  async getMatrix(actor: SimActorContext, threadIdInput: string): Promise<MatrixSnapshot> {
    const thread = await this.resolveThreadAccess(threadIdInput, actor);
    if (!thread) {
      if (!parseThreadRouteId(threadIdInput)) {
        throw new SimError(400, "Invalid threadId");
      }
      throw new SimError(404, "Thread not found");
    }

    const systemId = await this.getThreadSystemId(thread.id);
    if (!systemId) {
      throw new SimError(500, "Thread has no current system");
    }

    const [nodesResult, edgesResult, concernRowsResult, matrixDocRows, artifactRows] = await Promise.all([
      this.query<DbTopologyNodeRow>(
        `SELECT id, name, kind::text AS kind, parent_id, metadata
         FROM nodes
         WHERE system_id = $1
         ORDER BY id`,
        [systemId],
      ),
      this.query<DbTopologyEdgeRow>(
        `SELECT id, from_node_id, to_node_id, type::text AS type, metadata
         FROM edges
         WHERE system_id = $1
         ORDER BY id`,
        [systemId],
      ),
      this.query<DbConcernRow>(
        `SELECT name, position FROM concerns WHERE system_id = $1 ORDER BY position`,
        [systemId],
      ),
      this.query<DbMatrixDocRefRow>(
        `SELECT
           mr.node_id, mr.concern, d.hash, d.title, d.kind::text AS kind,
           d.language, d.source_type::text AS source_type, d.source_url, d.source_external_id,
           d.source_metadata, d.source_connected_user_id
         FROM matrix_refs mr
         JOIN documents d ON d.system_id = mr.system_id AND d.hash = mr.doc_hash
         WHERE mr.system_id = $1 AND mr.ref_type IN ('Document'::ref_type, 'Skill'::ref_type)
         ORDER BY mr.node_id, mr.concern`,
        [systemId],
      ),
      this.query<DbArtifactRow>(
        `SELECT id, node_id, concern, type::text AS type, language, text
         FROM artifacts
         WHERE system_id = $1`,
        [systemId],
      ),
    ]);

    const topologyNodes: TopologyNode[] = nodesResult.rows.map((node) => ({
      id: node.id,
      name: node.name,
      kind: node.kind,
      parentId: node.parent_id,
      metadata: node.metadata,
    }));

    const topologyEdges = edgesResult.rows.map((edge) => ({
      id: edge.id,
      type: edge.type,
      fromNodeId: edge.from_node_id,
      toNodeId: edge.to_node_id,
      protocol: (edge.metadata as { protocol?: string }).protocol ?? null,
    }));

    const documents: MatrixCellDoc[] = matrixDocRows.rows.map((row) => ({
      hash: row.hash,
      title: row.title,
      kind: row.kind,
      language: row.language,
      sourceType: row.source_type,
      sourceUrl: row.source_url,
      sourceExternalId: row.source_external_id,
      sourceMetadata: row.source_metadata,
      sourceConnectedUserId: row.source_connected_user_id,
    }));

    const cellMap = this.mapMatrixCellMap(matrixDocRows.rows, artifactRows.rows);
    return {
      threadId: thread.id,
      systemId,
      topology: {
        nodes: topologyNodes,
        edges: topologyEdges,
      },
      concerns: concernRowsResult.rows.map((concern) => ({
        name: concern.name,
        position: concern.position,
      })),
      documents,
      cells: Array.from(cellMap.values()),
    };
  }

  async patchMatrixLayout(
    actor: SimActorContext,
    threadIdInput: string,
    payload: Array<{ nodeId: string; x: number; y: number }>,
  ): Promise<MatrixSnapshot> {
    const thread = await this.resolveThreadAccess(threadIdInput, actor);
    if (!thread) {
      if (!parseThreadRouteId(threadIdInput)) {
        throw new SimError(400, "Invalid threadId");
      }
      throw new SimError(404, "Thread not found");
    }
    if (!canEdit(thread.access_role)) {
      throw new SimError(403, "Forbidden");
    }

    const updates = Array.isArray(payload) ? payload : [];
    const normalized = updates
      .map((entry) => {
        if (!entry || typeof entry.nodeId !== "string") return null;
        const nodeId = entry.nodeId.trim();
        const x = typeof entry.x === "number" ? entry.x : Number(entry.x);
        const y = typeof entry.y === "number" ? entry.y : Number(entry.y);
        if (!nodeId || !Number.isFinite(x) || !Number.isFinite(y)) return null;
        return { nodeId, x, y };
      })
      .filter((entry): entry is { nodeId: string; x: number; y: number } => entry !== null);

    if (normalized.length === 0) {
      throw new SimError(400, "No valid layout entries");
    }

    const systemId = await this.getThreadSystemId(thread.id);
    if (!systemId) {
      throw new SimError(500, "Thread has no current system");
    }

    let changed = 0;
    for (const next of normalized) {
      const result = this.usePgMem
        ? await this.query<DbIdRow>(
          `UPDATE nodes
           SET metadata = $3
           WHERE system_id = $1 AND id = $2`,
          [
            systemId,
            next.nodeId,
            await this.buildNodeMetadataWithLayout(systemId, next.nodeId, next.x, next.y),
          ],
        )
        : await this.query<DbIdRow>(
          `UPDATE nodes
           SET metadata = jsonb_set(
                 COALESCE(metadata, '{}'::jsonb),
                 '{layout}',
                 jsonb_build_object('x', $3::double precision, 'y', $4::double precision),
                 true
               )
           WHERE system_id = $1 AND id = $2`,
          [systemId, next.nodeId, next.x, next.y],
        );
      changed += result.rowCount ?? 0;
    }

    if (changed === 0) {
      throw new SimError(404, "No nodes updated");
    }

    await this.publishEvent({
      type: "thread.matrix.changed",
      aggregateType: "thread",
      aggregateId: thread.id,
      orgId: actor.orgId,
      traceId: thread.id,
      payload: { threadId: thread.id, changed },
    });

    const matrix = await this.getMatrix(actor, thread.id);
    return matrix;
  }

  async appendChatMessage(
    actor: SimActorContext,
    threadIdInput: string,
    input: { content: string; role?: "User" | "Assistant" | "System" },
  ): Promise<ChatMessage> {
    const thread = await this.resolveThreadAccess(threadIdInput, actor);
    if (!thread) {
      if (!parseThreadRouteId(threadIdInput)) {
        throw new SimError(400, "Invalid threadId");
      }
      throw new SimError(404, "Thread not found");
    }
    if (!canEdit(thread.access_role)) {
      throw new SimError(403, "Forbidden");
    }

    const content = input.content?.trim();
    if (!content) {
      throw new SimError(400, "Message content required");
    }

    const role = sanitizeRoleForDb(input.role);
    const actionId = stableId("chat", `${thread.id}|${content}|${Date.now()}`);
    const messageId = stableId("msg", `${thread.id}|${actionId}|${content}`);

    const inserted = await this.withTx(async (client) => {
      const positionResult = await client.query<{ position: number }>(
        `SELECT COALESCE(MAX(position), 0) + 1 AS position FROM actions WHERE thread_id = $1`,
        [thread.id],
      );
      const actionPosition = positionResult.rows[0]?.position ?? 1;

      await client.query(
        `INSERT INTO actions (id, thread_id, position, type, title, output_system_id)
         VALUES ($1, $2, $3, 'Chat'::action_type, $4, NULL)`,
        [actionId, thread.id, actionPosition, "Chat message"],
      );

      const messageResult = await client.query<{
        id: string;
        action_id: string;
        role: "User" | "Assistant" | "System";
        action_type: string;
        content: string;
        created_at: Date;
      }>(
        `INSERT INTO messages (id, thread_id, action_id, role, content, position)
         VALUES ($1, $2, $3, $4::message_role, $5, $6)
         RETURNING id, action_id, role, content, created_at`,
        [messageId, thread.id, actionId, role, content, actionPosition],
      );

      const row = messageResult.rows[0];
      if (!row) {
        throw new SimError(500, "Failed to append message");
      }

      return {
        id: row.id,
        actionId: row.action_id,
        actionType: "Chat",
        actionPosition,
        role: row.role,
        content: row.content,
        createdAt: toISO(row.created_at),
      };
    });

    return inserted;
  }

  private async resolveRunPrompt(
    threadId: string,
    chatMessageId: string | null,
    prompt?: string,
  ): Promise<string> {
    const explicitPrompt = normalizeRunPrompt(prompt);
    if (explicitPrompt !== "Run this request.") {
      return explicitPrompt;
    }

    if (!chatMessageId) {
      return explicitPrompt;
    }

    if (!isUuid(chatMessageId)) {
      throw new SimError(400, "Invalid chatMessageId");
    }

    const result = await this.query<{ content: string }>(
      `SELECT content FROM messages WHERE id = $1 AND thread_id = $2`,
      [chatMessageId, threadId],
    );
    return normalizeRunPrompt(result.rows[0]?.content);
  }

  async startRun(
    actor: SimActorContext,
    threadIdInput: string,
    input: {
      assistantType: AssistantType;
      prompt?: string;
      model?: string;
      chatMessageId?: string;
    },
  ): Promise<AssistantRun> {
    const thread = await this.resolveThreadAccess(threadIdInput, actor);
    if (!thread) {
      if (!parseThreadRouteId(threadIdInput)) {
        throw new SimError(400, "Invalid threadId");
      }
      throw new SimError(404, "Thread not found");
    }

    if (!canEdit(thread.access_role)) {
      throw new SimError(403, "Forbidden");
    }

    const model = normalizeModel(input.model);
    if (!model) {
      throw new SimError(400, "Invalid model");
    }
    if (input.assistantType !== "direct" && input.assistantType !== "plan") {
      throw new SimError(400, "Invalid assistant type");
    }

    const prompt = await this.resolveRunPrompt(thread.id, input.chatMessageId?.trim() ?? null, input.prompt);
    const runCountResult = await this.query<{ next_run_id: number }>(
      `SELECT COALESCE(COUNT(*)::int, 0) + 1 AS next_run_id
       FROM agent_runs
       WHERE thread_id = $1`,
      [thread.id],
    );
    const sequence = runCountResult.rows[0]?.next_run_id ?? 1;
    const runId = deterministicUuid(`run|${thread.id}|${actor.actorId}|${input.assistantType}|${prompt}|${sequence}`);
    const chatMessageId = input.chatMessageId?.trim() || null;
    if (chatMessageId && !isUuid(chatMessageId)) {
      throw new SimError(400, "Invalid chatMessageId");
    }

    await this.query(
      `INSERT INTO agent_runs (
         id, thread_id, project_id, requested_by_user_id, model, mode,
         plan_action_id, chat_message_id, prompt, system_prompt
       )
       VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, $8, $9)`,
      [
        runId,
        thread.id,
        thread.project_id,
        actor.actorId,
        model,
        input.assistantType,
        chatMessageId,
        prompt,
        this.defaultSystemPrompt,
      ],
    );

    await this.publishEvent({
      type: "assistant.run.started",
      aggregateType: "assistant-run",
      aggregateId: runId,
      orgId: actor.orgId,
      traceId: thread.id,
      payload: { threadId: thread.id, status: "queued", mode: input.assistantType },
    });

    await this.publishEvent({
      type: "assistant.run.waiting_input",
      aggregateType: "assistant-run",
      aggregateId: runId,
      orgId: actor.orgId,
      traceId: thread.id,
      payload: { threadId: thread.id, status: "waiting_input", mode: input.assistantType },
    });

    return this.getRun(actor, runId);
  }

  async getRun(actor: SimActorContext, runId: string): Promise<AssistantRun> {
    if (!isUuid(runId)) {
      throw new SimError(400, "Invalid run id");
    }
    const result = await this.query<DbRunRow>(
      `SELECT
         id, thread_id, model, status::text AS status, mode::text AS mode, prompt,
         system_prompt, run_result_status, run_result_messages, run_result_changes, run_error,
         created_at, started_at, completed_at, project_id, requested_by_user_id,
         runner_id, plan_action_id, chat_message_id
       FROM agent_runs
       WHERE id = $1`,
      [runId],
    );

    const run = result.rows[0];
    if (!run) {
      throw new SimError(404, "Run not found");
    }

    const thread = await this.resolveThreadAccess(run.thread_id, actor);
    if (!thread) {
      throw new SimError(403, "Forbidden");
    }

    return this.mapAssistantRun(run);
  }

  async claimRun(actor: SimActorContext, runId: string, runnerId: string): Promise<AssistantRun> {
    if (!isUuid(runId)) {
      throw new SimError(400, "Invalid run id");
    }

    const runRow = await this.query<{ thread_id: string }>(`SELECT thread_id FROM agent_runs WHERE id = $1`, [runId]);
    if (!runRow.rows[0]) {
      throw new SimError(404, "Run not found");
    }
    const thread = await this.resolveThreadAccess(runRow.rows[0]!.thread_id, actor);
    if (!thread) {
      throw new SimError(403, "Forbidden");
    }

    try {
      const state = await this.query<{ status: string; runner_id: string | null }>(
        `SELECT status, runner_id FROM agent_runs WHERE id = $1`,
        [runId],
      );
      const currentState = state.rows[0];
      if (!currentState) {
        throw new SimError(404, "Run not found");
      }
      if (currentState.status !== "queued" && currentState.status !== "running") {
        throw new SimError(409, "Run unavailable");
      }
      if (currentState.status === "running" && currentState.runner_id !== runnerId) {
        throw new SimError(409, "Run unavailable");
      }
      if (currentState.status === "running" && currentState.runner_id === runnerId) {
        const existing = await this.getRun(actor, runId);
        await this.publishEvent({
          type: "assistant.run.progress",
          aggregateType: "assistant-run",
          aggregateId: runId,
          orgId: actor.orgId,
          traceId: thread.id,
          payload: {
            status: currentState.status,
            runId,
            threadId: thread.id,
            runnerId,
          },
        });
        return existing;
      }

      const result = await this.query<DbRunRow>(
        `UPDATE agent_runs
         SET status = 'running',
             runner_id = $2,
             started_at = NOW(),
             updated_at = NOW(),
             run_error = NULL
         WHERE id = $1
           AND status = 'queued'
         RETURNING
           id, thread_id, model, status::text AS status, mode::text AS mode,
           prompt, system_prompt, run_result_status, run_result_messages,
           run_result_changes, run_error, created_at, started_at, completed_at,
           project_id, requested_by_user_id, runner_id, plan_action_id, chat_message_id`,
        [runId, runnerId],
      );

      const row = result.rows[0];
      if (!row) {
        throw new SimError(409, "Run unavailable");
      }

      await this.publishEvent({
        type: "assistant.run.progress",
        aggregateType: "assistant-run",
        aggregateId: row.id,
        orgId: actor.orgId,
        traceId: row.thread_id,
        payload: {
          status: row.status,
          runId: row.id,
          threadId: row.thread_id,
          runnerId,
        },
      });

      return this.mapAssistantRun(row);
    } catch (error: unknown) {
      if (error instanceof SimError) throw error;
      if (error && typeof error === "object" && (error as { code?: string }).code === "23505") {
        throw new SimError(409, "Run unavailable");
      }
      throw error;
    }
  }

  async claimQueuedRun(runnerId: string): Promise<AssistantRun | null> {
    if (this.usePgMem) {
      const claimed = await this.withTx(async (client) => {
        const queuedRuns = await client.query<{ id: string; thread_id: string }>(
          `SELECT id, thread_id
           FROM agent_runs
           WHERE status = 'queued'
           ORDER BY created_at`,
        );

        for (const nextRun of queuedRuns.rows) {
          const hasRunning = await client.query<{ exists: string }>(
            `SELECT id
             FROM agent_runs
             WHERE thread_id = $1
               AND status = 'running'`,
            [nextRun.thread_id],
          );
          if (hasRunning.rows.length > 0) {
            continue;
          }

          const updated = await client.query<DbRunRow>(
            `UPDATE agent_runs
             SET status = 'running',
                 runner_id = $1,
                 started_at = NOW(),
                 updated_at = NOW(),
                 run_error = NULL
             WHERE id = $2
               AND status = 'queued'
             RETURNING
               id, thread_id, model, status::text AS status, mode::text AS mode,
               prompt, system_prompt, run_result_status, run_result_messages,
               run_result_changes, run_error, created_at, started_at, completed_at,
               project_id, requested_by_user_id, runner_id, plan_action_id, chat_message_id`,
            [runnerId, nextRun.id],
          );

          if (updated.rows[0]) {
            return updated.rows[0];
          }
        }

        return null;
      });

      return claimed ? this.mapAssistantRun(claimed) : null;
    }

    const claimed = await this.withTx(async (client) => {
      const result = await client.query<DbRunRow>(
        `WITH next_run AS (
          SELECT ar.id
          FROM agent_runs ar
          JOIN threads t ON t.id = ar.thread_id
          WHERE ar.status = 'queued'
            AND NOT EXISTS (
              SELECT 1
              FROM agent_runs ar2
              WHERE ar2.thread_id = ar.thread_id
                AND ar2.status = 'running'
            )
          ORDER BY ar.created_at
          FOR UPDATE OF ar, t
          SKIP LOCKED
          LIMIT 1
        )
        UPDATE agent_runs ar
        SET status = 'running',
            runner_id = $1,
            started_at = NOW(),
            updated_at = NOW(),
            run_error = NULL
        FROM next_run
        WHERE ar.id = next_run.id
        RETURNING
          ar.id, ar.thread_id, ar.model, ar.status::text AS status, ar.mode::text AS mode,
          ar.prompt, ar.system_prompt, ar.run_result_status, ar.run_result_messages,
          ar.run_result_changes, ar.run_error, ar.created_at, ar.started_at, ar.completed_at,
          ar.project_id, ar.requested_by_user_id, ar.runner_id, ar.plan_action_id, ar.chat_message_id`,
        [runnerId],
      );

      return result.rows[0] ?? null;
    });

    return claimed ? this.mapAssistantRun(claimed) : null;
  }

  private parseChanges(input: Array<unknown>): RunChangeRow[] {
    const normalized: RunChangeRow[] = [];
    for (const entry of input) {
      if (!entry || typeof entry !== "object" || Array.isArray(entry)) continue;
      const candidate = entry as {
        target_table?: unknown;
        operation?: unknown;
        target_id?: unknown;
        previous?: unknown;
        current?: unknown;
      };
      if (typeof candidate.target_table !== "string") continue;
      if (candidate.operation !== "Create" && candidate.operation !== "Update" && candidate.operation !== "Delete") {
        continue;
      }
      if (!candidate.target_id || typeof candidate.target_id !== "object" || Array.isArray(candidate.target_id)) continue;
      const previous =
        candidate.previous && typeof candidate.previous === "object" && !Array.isArray(candidate.previous)
          ? (candidate.previous as Record<string, unknown>)
          : null;
      const current =
        candidate.current && typeof candidate.current === "object" && !Array.isArray(candidate.current)
          ? (candidate.current as Record<string, unknown>)
          : null;
      normalized.push({
        target_table: candidate.target_table,
        operation: candidate.operation as "Create" | "Update" | "Delete",
        target_id: candidate.target_id as Record<string, unknown>,
        previous,
        current,
      });
    }
    return normalized;
  }

  async completeRun(
    actor: SimActorContext,
    runId: string,
    input: {
      status: "success" | "failed";
      messages: string[];
      changes?: Array<unknown>;
      error?: string | null;
    },
  ): Promise<AssistantRun> {
    if (!isUuid(runId)) {
      throw new SimError(400, "Invalid run id");
    }

    const run = await this.query<DbRunRow>(`SELECT thread_id, status FROM agent_runs WHERE id = $1`, [runId]);
    if (!run.rows[0]) {
      throw new SimError(404, "Run not found");
    }
    const thread = await this.resolveThreadAccess(run.rows[0].thread_id, actor);
    if (!thread) {
      throw new SimError(403, "Forbidden");
    }

    const messages = (input.messages ?? [])
      .map((entry) => String(entry).trim())
      .filter((entry) => entry.length > 0);
    if (messages.length === 0) {
      throw new SimError(400, "No completion messages");
    }

    const changes = this.parseChanges(input.changes ?? []);
    const runResultStatus = input.status;
    const completionMessage = messages.join(" | ");
    const responseActionId = stableId("assistant-response-action", `${runId}|${completionMessage}`);

    const updated = await this.withTx(async (client) => {
      const update = await client.query<DbRunRow>(
        `UPDATE agent_runs
         SET status = $2,
             run_result_status = $3,
             run_result_messages = $4,
             run_result_changes = $5::jsonb,
             run_error = COALESCE($6, run_error),
             completed_at = NOW(),
             updated_at = NOW()
         WHERE id = $1
           AND status IN ('queued', 'running')
         RETURNING
           id, thread_id, model, status::text AS status, mode::text AS mode, prompt,
           system_prompt, run_result_status, run_result_messages, run_result_changes,
           run_error, created_at, started_at, completed_at, project_id, requested_by_user_id,
           runner_id, plan_action_id, chat_message_id`,
        [
          runId,
          runResultStatus,
          runResultStatus,
          messages,
          JSON.stringify(changes),
          input.error ?? null,
        ],
      );

      const updatedRun = update.rows[0];
      if (!updatedRun) {
        throw new SimError(409, "Run already finalized");
      }

      const messagePositionResult = await client.query<{ position: number }>(
        `SELECT COALESCE(MAX(position), 0) + 1 AS position FROM actions WHERE thread_id = $1`,
        [updatedRun.thread_id],
      );
      const responseActionPosition = messagePositionResult.rows[0]?.position ?? 1;

      await client.query(
        `INSERT INTO actions (id, thread_id, position, type, title, output_system_id)
         VALUES ($1, $2, $3, 'ExecuteResponse'::action_type, $4, NULL)`,
        [responseActionId, updatedRun.thread_id, responseActionPosition, "Assistant execution response"],
      );
      const completionMessageRows = messages.map((entry) => {
        const actionMessageId = stableId("assistant-response-msg", `${responseActionId}|${entry}`);
        return {
          id: actionMessageId,
          actionType: "ExecuteResponse",
          content: entry,
        };
      });

      for (const [messageIndex, message] of completionMessageRows.entries()) {
        await client.query(
          `INSERT INTO messages (id, thread_id, action_id, role, content, position)
           VALUES ($1, $2, $3, 'Assistant'::message_role, $4, $5)`,
          [message.id, updatedRun.thread_id, responseActionId, message.content, messageIndex + 1],
        );
      }

      return updatedRun;
    });

    await this.publishEvent({
      type: runResultStatus === "success" ? "assistant.run.completed" : "assistant.run.failed",
      aggregateType: "assistant-run",
      aggregateId: runId,
      orgId: actor.orgId,
      traceId: updated.thread_id,
      payload: { runId, status: runResultStatus, messages },
    });

    await this.publishEvent({
      type: "chat.session.finished",
      aggregateType: "thread",
      aggregateId: updated.thread_id,
      orgId: actor.orgId,
      traceId: updated.thread_id,
      payload: {
        threadId: updated.thread_id,
        runId,
        status: runResultStatus,
      },
    });

    return this.mapAssistantRun(updated);
  }

  async cancelRun(actor: SimActorContext, runId: string): Promise<AssistantRun> {
    if (!isUuid(runId)) {
      throw new SimError(400, "Invalid run id");
    }
    const run = await this.query<DbRunRow>(`SELECT thread_id FROM agent_runs WHERE id = $1`, [runId]);
    if (!run.rows[0]) {
      throw new SimError(404, "Run not found");
    }
    const thread = await this.resolveThreadAccess(run.rows[0].thread_id, actor);
    if (!thread) {
      throw new SimError(403, "Forbidden");
    }

    const canceled = await this.query<DbRunRow>(
      `UPDATE agent_runs
       SET status = 'cancelled',
           run_result_status = 'failed',
           run_error = COALESCE($2, run_error),
           completed_at = NOW(),
           updated_at = NOW()
       WHERE id = $1 AND status IN ('queued', 'running')
       RETURNING
         id, thread_id, model, status::text AS status, mode::text AS mode, prompt,
         system_prompt, run_result_status, run_result_messages, run_result_changes,
         run_error, created_at, started_at, completed_at, project_id, requested_by_user_id,
         runner_id, plan_action_id, chat_message_id`,
      [runId, "Cancelled by user"],
    );

    if (!canceled.rows[0]) {
      throw new SimError(409, "Run cannot be cancelled");
    }

    const row = canceled.rows[0];
    await this.publishEvent({
      type: "assistant.run.cancelled",
      aggregateType: "assistant-run",
      aggregateId: runId,
      orgId: actor.orgId,
      traceId: row.thread_id,
      payload: { threadId: row.thread_id, status: "cancelled", runId },
    });

    await this.publishEvent({
      type: "chat.session.finished",
      aggregateType: "thread",
      aggregateId: row.thread_id,
      orgId: actor.orgId,
      traceId: row.thread_id,
      payload: { threadId: row.thread_id, runId, status: "cancelled" },
    });

    return this.mapAssistantRun(row);
  }

  async publishEvent(input: PublishEventInput): Promise<StaffXEvent> {
    const eventId = stableId("evt", `${input.type}|${input.aggregateType}|${input.aggregateId}|${input.traceId ?? ""}|${Date.now()}`);
    const row = await this.query<DbEventRow>(
      `INSERT INTO staffx_events (
         id,
         type,
         aggregate_type,
         aggregate_id,
         org_id,
         trace_id,
        payload,
        version,
        occurred_at
       ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)
       RETURNING
         id, type, aggregate_type, aggregate_id, occurred_at, org_id, trace_id, payload, version`,
      [
        eventId,
        input.type,
        input.aggregateType,
        input.aggregateId,
        input.orgId ?? null,
        input.traceId ?? null,
        JSON.stringify(input.payload),
        input.version ?? 1,
        input.occurredAt ?? new Date(),
      ],
    );
    if (!row.rows[0]) {
      throw new Error("Unable to persist event");
    }
    return this.mapEventRow(row.rows[0]);
  }

  async queryEvents(input: QueryEventsInput): Promise<{ items: StaffXEvent[]; nextCursor: string | null }> {
    const limit = parsePositiveInt(input.limit, SimApi.defaultEventLimit, 1, 500);
    const params: unknown[] = [];
    let whereClause = "1 = 1";

    if (input.orgId !== undefined) {
      if (input.orgId === null) {
        whereClause += " AND org_id IS NULL";
      } else {
        params.push(input.orgId);
        whereClause += ` AND org_id = $${params.length}`;
      }
    }

    if (input.aggregateType) {
      params.push(input.aggregateType);
      whereClause += ` AND aggregate_type = $${params.length}`;
    }

    if (input.aggregateId) {
      params.push(input.aggregateId);
      whereClause += ` AND aggregate_id = $${params.length}`;
    }

    if (input.since) {
      const parsed = parseEventSince(input.since);
      if (!parsed) {
        throw new SimError(400, "Invalid cursor");
      }
      if (parsed.asCursor === false) {
        params.push(parsed.occurredAt);
        whereClause += ` AND occurred_at > $${params.length}`;
      } else {
        const cursor = parsed;
        params.push(cursor.occurredAt, cursor.id);
        whereClause += ` AND (occurred_at > $${params.length - 1} OR (occurred_at = $${params.length - 1} AND id > $${params.length}))`;
      }
    }

    const result = await this.query<DbEventRow>(
      `SELECT id, type, aggregate_type, aggregate_id, org_id, occurred_at, trace_id, payload, version
       FROM staffx_events
       WHERE ${whereClause}
       ORDER BY occurred_at ASC, id ASC
       LIMIT $${params.length + 1}`,
      [...params, limit + 1],
    );

    const rows = result.rows;
    const events = rows.slice(0, limit).map((row) => this.mapEventRow(row));
    const nextCursor = rows.length > limit && events[events.length - 1]
      ? encodeEventCursor(events[events.length - 1]!)
      : null;

    return {
      items: events,
      nextCursor,
    };
  }

  async getArtifactsForSystem(systemId: string): Promise<Array<{
    id: string;
    nodeId: string;
    concern: string;
    type: string;
    language: string;
    text: string | null;
    systemId: string;
  }>> {
    const result = await this.query<DbArtifactRow>(
      `SELECT id, node_id, concern, type::text AS type, language, text
       FROM artifacts
       WHERE system_id = $1`,
      [systemId],
    );

    return result.rows.map((row) => ({
      id: row.id,
      nodeId: row.node_id,
      concern: row.concern,
      type: row.type,
      language: row.language,
      text: row.text,
      systemId,
    }));
  }

  async cleanupSimProjects(prefix: string, likePattern?: string, usePgMem = this.usePgMem): Promise<{ removed: number }> {
    const pattern = likePattern ?? `${prefix}%`;
    const escapedLikePattern = pattern;

    return this.withTx(async (client) => {
      const projects = usePgMem
        ? await client.query<DbIdRow>(
            `SELECT id FROM projects WHERE name LIKE $1`,
            [escapedLikePattern],
          )
        : await client.query<DbIdRow>(
            `SELECT id FROM projects WHERE name LIKE $1 ESCAPE '\\'`,
            [escapedLikePattern],
          );
      if (projects.rows.length === 0) {
        return { removed: 0 };
      }

      const projectIds = projects.rows.map((row) => row.id);
      const projectIdPlaceholders = projectIds.map((_, index) => `$${index + 1}`).join(", ");
      const threadIdParams: string[] = [...projectIds];
      const threads = usePgMem
        ? await client.query<{ id: string; seed_system_id: string }>(
          `SELECT id, seed_system_id FROM threads WHERE project_id IN (${projectIdPlaceholders})`,
          threadIdParams,
        )
        : await client.query<{ id: string; seed_system_id: string }>(
          `SELECT id, seed_system_id FROM threads WHERE project_id = ANY($1::text[])`,
          [projectIds],
        );

      const threadIds = threads.rows.map((row) => row.id);
      const systemIds = new Set<string>(threads.rows.map((row) => row.seed_system_id));

      if (threadIds.length > 0) {
        const threadPlaceholders = threadIds.map((_, index) => `$${index + 1}`).join(", ");
        await client.query(
          usePgMem
            ? `UPDATE threads
               SET source_thread_id = NULL
               WHERE id IN (${threadPlaceholders})`
            : `UPDATE threads
               SET source_thread_id = NULL
               WHERE id = ANY($1::text[])`,
          usePgMem ? threadIds : [threadIds],
        );

        const threadIdPlaceholders = threadIds.map((_, index) => `$${index + 1}`).join(", ");
        const actionSystems = await client.query<{ output_system_id: string }>(
          usePgMem
            ? `SELECT DISTINCT output_system_id
               FROM actions
               WHERE thread_id IN (${threadIdPlaceholders})
                 AND output_system_id IS NOT NULL`
            : `SELECT DISTINCT output_system_id
               FROM actions
               WHERE thread_id = ANY($1::text[])
                 AND output_system_id IS NOT NULL`,
          usePgMem ? threadIds : [threadIds],
        );
        for (const row of actionSystems.rows) {
          systemIds.add(row.output_system_id);
        }

        const runIdPlaceholders = threadIds.map((_, index) => `$${index + 1}`).join(", ");
        const agentRuns = await client.query<{ id: string }>(
          usePgMem
            ? `SELECT id FROM agent_runs WHERE thread_id IN (${threadIdPlaceholders})`
            : `SELECT id FROM agent_runs WHERE thread_id = ANY($1::text[])`,
          usePgMem ? threadIds : [threadIds],
        );
        const runIds = agentRuns.rows.map((row) => row.id);
        if (runIds.length > 0) {
          const runPlaceholders = runIds.map((_, index) => `$${index + 1}`).join(", ");
          await client.query(
            usePgMem
              ? `DELETE FROM staffx_events
                 WHERE aggregate_type = 'assistant-run'
                   AND aggregate_id IN (${runPlaceholders})`
              : `DELETE FROM staffx_events
                 WHERE aggregate_type = 'assistant-run'
                   AND aggregate_id = ANY($1::text[])`,
            usePgMem ? runIds : [runIds],
          );
        }
      }

      if (threadIds.length > 0) {
        const threadPlaceholders = threadIds.map((_, index) => `$${index + 1}`).join(", ");
        await client.query(
          usePgMem
            ? `DELETE FROM staffx_events
               WHERE aggregate_type = 'thread'
                 AND aggregate_id IN (${threadPlaceholders})`
            : `DELETE FROM staffx_events
               WHERE aggregate_type = 'thread'
                 AND aggregate_id = ANY($1::text[])`,
          usePgMem ? threadIds : [threadIds],
        );
      }

      const deleted = await client.query<DbIdRow>(
        usePgMem
          ? `DELETE FROM projects WHERE id IN (${projectIdPlaceholders}) RETURNING id`
          : `DELETE FROM projects WHERE id = ANY($1::text[]) RETURNING id`,
        usePgMem ? projectIds : [projectIds],
      );

      const orderedSystemIds = Array.from(systemIds).filter((value) => value);
      if (orderedSystemIds.length > 0) {
        const systemPlaceholders = orderedSystemIds.map((_, index) => `$${index + 1}`).join(", ");
        await client.query(
          usePgMem
            ? `DELETE FROM systems WHERE id IN (${systemPlaceholders})`
            : `DELETE FROM systems WHERE id = ANY($1::text[])`,
          usePgMem ? orderedSystemIds : [orderedSystemIds],
        );
      }

      return { removed: deleted.rowCount ?? 0 };
    });
  }

  async runArtifactsForRun(runId: string): Promise<{
    artifactCount: number;
    matrixSystemId: string | null;
  }> {
    const runRows = await this.query<{ thread_id: string; status: string }>(
      "SELECT thread_id, status FROM agent_runs WHERE id = $1",
      [runId],
    );
    if (!runRows.rows[0]) {
      throw new SimError(404, "Run not found");
    }
    const systemId = await this.getThreadSystemId(runRows.rows[0]!.thread_id);
    if (!systemId) {
      return { artifactCount: 0, matrixSystemId: null };
    }
    const artifacts = await this.getArtifactsForSystem(systemId);
    return { artifactCount: artifacts.length, matrixSystemId: systemId };
  }
}
