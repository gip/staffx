import { randomUUID } from "node:crypto";
import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import type { FastifyInstance, FastifyReply, FastifyRequest } from "fastify";

import pool, { query } from "../db.js";
import { generateOpenShipFileBundle } from "../agent-runner.js";
import { verifyAuth, type AuthUser } from "../auth.js";
import {
  claimAgentRunById,
  enqueueAgentRunWithWait,
  getAgentRunById,
  updateAgentRunResult,
} from "../agent-queue.js";
import type { AgentRunPlanChange } from "@staffx/agent-runtime";
import {
  publishEvent,
  queryEvents,
  encodeCursor,
  parseCursor,
  type StaffXEvent,
} from "../events.js";

type AccessRole = "Owner" | "Editor" | "Viewer";

interface V1AuthRequest extends FastifyRequest {
  auth: AuthUser;
}

type CursorLike = string | undefined;

interface V1ProjectAccessRow {
  project_id: string;
  owner_id: string;
  visibility: "public" | "private";
  owner_handle: string;
  is_archived: boolean;
  access_role: AccessRole;
  name: string;
}

interface V1ThreadSummaryRow {
  id: string;
  title: string | null;
  description: string | null;
  source_thread_id: string | null;
  project_id: string;
  project_name: string;
  status: "open" | "closed" | "committed";
  created_by_handle: string;
  owner_handle: string;
  created_at: Date;
  updated_at: Date;
  access_role: AccessRole;
}

interface V1ThreadMatrixNodeCell {
  nodeId: string;
  concern: string;
  docs: Array<{
    hash: string;
    title: string;
    kind: string;
    language: string;
    sourceType: string;
    sourceUrl: string | null;
    sourceExternalId: string | null;
  }>;
  artifacts: Array<{
    path: string;
    type: string;
    metadata: Record<string, unknown>;
  }>;
}

interface V1TopologyNode {
  id: string;
  name: string;
  kind: string;
  parentId: string | null;
  layoutX?: number | null;
  layoutY?: number | null;
}

interface V1TopologyEdge {
  id: string;
  fromNodeId: string;
  toNodeId: string;
  type: string;
  protocol?: string | null;
}

interface V1RunChatMessage {
  id: string;
  actionId: string;
  actionType: string;
  actionPosition: number;
  role: "User" | "Assistant" | "System";
  content: string;
  createdAt: string;
}

interface V1RunFileChange {
  kind: "Create" | "Update" | "Delete";
  path: string;
  fromHash?: string;
  toHash?: string;
}

type V1RunResultStatus = "queued" | "running" | "success" | "failed" | "cancelled";

interface V1RunResponse {
  runId: string;
  status: V1RunResultStatus;
  mode: AssistantMode;
  threadId: string;
  systemId: string;
  filesChanged: V1RunFileChange[];
  summary: {
    status: "success" | "failed" | "cancelled" | "queued" | "running";
    messages: string[];
  };
  changesCount: number;
  messages: V1RunChatMessage[];
  threadState?: Record<string, unknown>;
}

interface V1OpenShipBundleFile {
  path: string;
  content: string;
}

interface V1OpenShipBundleDescriptor {
  threadId: string;
  systemId: string;
  generatedAt: string;
  files: V1OpenShipBundleFile[];
}

type V1IntegrationStatus = "connected" | "disconnected" | "expired" | "needs_reauth";
type V1ProjectVisibility = "public" | "private";
type V1ProjectRole = "Owner" | "Editor" | "Viewer";

interface V1ProjectListRow {
  id: string;
  name: string;
  description: string | null;
  visibility: V1ProjectVisibility;
  access_role: V1ProjectRole;
  owner_handle: string;
  created_at: Date;
  thread_count: string;
}

type AssistantMode = "direct" | "plan";
type AssistantRunStatus = "queued" | "running" | "success" | "failed" | "cancelled";

interface V1ThreadRow {
  id: string;
  project_id: string;
  title: string | null;
  description: string | null;
  status: "open" | "closed" | "committed";
  created_at: Date;
  updated_at: Date;
  source_thread_id: string | null;
  access_role: AccessRole;
}

interface V1ProjectThreadConcernRow {
  name: string;
  position: number;
}

interface V1ProjectThreadDocumentRow {
  hash: string;
  kind: string;
  title: string;
  language: string;
  source_type: string;
  source_url: string | null;
  source_external_id: string | null;
  source_metadata: Record<string, unknown> | null;
  source_connected_user_id: string | null;
  text: string;
}

interface V1AgentRunRow {
  id: string;
  thread_id: string;
  status: AssistantRunStatus;
  mode: AssistantMode;
  prompt: string;
  system_prompt: string | null;
  run_result_status: "success" | "failed" | null;
  run_result_messages: string[] | null;
  run_result_changes: AgentRunPlanChange[] | null;
  run_error: string | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
}

interface V1ChatMessageRequest {
  content?: string;
  role?: "User" | "Assistant" | "System";
}

interface V1RunStartBody {
  prompt?: string;
  chatMessageId?: string;
  wait?: boolean;
  status?: string;
  sourceThreadId?: string;
}

type V1ThreadStatus = "open" | "closed" | "committed";

interface V1ThreadPatchBody {
  title?: string;
  description?: string | null;
  status?: V1ThreadStatus;
}

interface V1RunClaimBody {
  runnerId?: string;
}

interface V1RunCompleteBody {
  status: "success" | "failed";
  messages: string[];
  changes?: Array<Record<string, unknown>>;
  error?: string;
  runnerId?: string;
}

interface V1MatrixPatchBody {
  layout?: Array<{ nodeId: string; x: number; y: number }>;
  nodes?: Array<{ nodeId: string; x: number; y: number }>;
}

interface V1ListCursor {
  page: number;
  pageSize: number;
  nextCursor: string | null;
}

function writeProblem(
  reply: FastifyReply,
  status: number,
  title: string,
  detail: string,
): void {
  reply.code(status).type("application/problem+json").send({
    type: "https://tools.ietf.org/html/rfc7807#section-3.1",
    title,
    status,
    detail,
    instance: reply.request.url,
  });
}

function forbiddenProblem(reply: FastifyReply, detail = "You do not have access to this resource."): void {
  writeProblem(reply, 403, "Forbidden", detail);
}

function notFoundProblem(reply: FastifyReply, title = "Not found"): void {
  writeProblem(reply, 404, title, title);
}

function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function readEventCursor(raw: CursorLike): CursorLike {
  if (!raw) return undefined;
  try {
    const decoded = decodeURIComponent(raw);
    if (parseCursor(decoded)) return decoded;
    const parsedDate = new Date(decoded);
    if (!Number.isNaN(parsedDate.getTime())) return decoded;
    return undefined;
  } catch {
    return undefined;
  }
}

function parsePositiveInt(raw: unknown, fallback: number, min = 1, max = 200): number {
  const asNumber = typeof raw === "number" || typeof raw === "string"
    ? Number(raw)
    : NaN;
  if (!Number.isFinite(asNumber)) return fallback;
  const value = Math.trunc(asNumber);
  if (value < min) return fallback;
  return value > max ? max : value;
}

function normalizeToplologyPositions(body: V1MatrixPatchBody): Array<{ nodeId: string; x: number; y: number }> {
  const list = body.layout ?? body.nodes;
  if (!Array.isArray(list)) return [];
  return list
    .filter((entry): entry is { nodeId: string; x: number; y: number } => {
      if (!entry || typeof entry !== "object") return false;
      return (
        typeof entry.nodeId === "string"
        && entry.nodeId.trim().length > 0
        && Number.isFinite(entry.x)
        && Number.isFinite(entry.y)
      );
    })
    .map((entry) => ({ nodeId: entry.nodeId.trim(), x: entry.x, y: entry.y }));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function isAgentRunPlanChange(value: unknown): value is AgentRunPlanChange {
  if (!isRecord(value)) return false;
  const candidate = value as {
    target_table?: unknown;
    operation?: unknown;
    target_id?: unknown;
    previous?: unknown;
    current?: unknown;
  };
  if (typeof candidate.target_table !== "string") return false;
  if (candidate.operation !== "Create" && candidate.operation !== "Update" && candidate.operation !== "Delete") return false;
  if (!isRecord(candidate.target_id)) return false;
  if (candidate.previous !== null && !isRecord(candidate.previous)) return false;
  if (candidate.current !== null && !isRecord(candidate.current)) return false;
  return true;
}

function parseRunPlanChanges(raw: unknown): AgentRunPlanChange[] {
  if (!Array.isArray(raw)) return [];
  return raw.filter(isAgentRunPlanChange);
}

function canEdit(role: AccessRole): boolean {
  return role === "Owner" || role === "Editor";
}

async function resolveProjectAccess(projectId: string, user: AuthUser): Promise<V1ProjectAccessRow | null> {
  const result = await query<V1ProjectAccessRow>(
    `SELECT
       p.id AS project_id,
       p.owner_id,
       p.visibility::text AS visibility,
       owner_u.handle AS owner_handle,
       p.is_archived,
       COALESCE(NULLIF(pc.role::text, ''),
         CASE
           WHEN p.visibility = 'public' THEN 'Viewer'
           WHEN p.owner_id = $2 THEN 'Owner'
           ELSE NULL
         END
       )::text AS access_role,
       p.name
     FROM projects p
     JOIN users owner_u ON owner_u.id = p.owner_id
     LEFT JOIN project_collaborators pc
       ON pc.project_id = p.id
      AND pc.user_id = $2
     WHERE p.id = $1`,
    [projectId, user.id],
  );

  if (result.rowCount === 0) return null;

  const row = result.rows[0];
  if (row.is_archived) return null;

  const accessRole = row.access_role as AccessRole | null;
  if (!accessRole) return null;

  return { ...row, access_role: accessRole };
}

async function resolveThreadAccess(threadId: string, user: AuthUser): Promise<V1ThreadRow | null> {
  const result = await query<V1ThreadRow>(
    `SELECT
       t.id,
       t.project_id,
       t.title,
       t.description,
       t.status,
       t.created_at,
       t.updated_at,
       t.source_thread_id,
       COALESCE(NULLIF(pc.role::text, ''),
         CASE
           WHEN p.owner_id = $2 THEN 'Owner'
           WHEN p.visibility = 'public' THEN 'Viewer'
           ELSE NULL
         END
       )::text AS access_role
     FROM threads t
     JOIN projects p ON p.id = t.project_id
     JOIN users owner_u ON owner_u.id = p.owner_id
     LEFT JOIN project_collaborators pc
       ON pc.project_id = p.id
      AND pc.user_id = $2
     WHERE t.id = $1
       AND p.is_archived = false`,
    [threadId, user.id],
  );

  if (result.rowCount === 0) return null;

  const row = result.rows[0];
  const accessRole = row.access_role as AccessRole | null;
  if (!accessRole) return null;

  return { ...row, access_role: accessRole };
}

function buildPaginationFromQuery(rawPage: unknown, rawPageSize: unknown): V1ListCursor {
  const page = parsePositiveInt(rawPage, 1, 1, Number.MAX_SAFE_INTEGER);
  const pageSize = parsePositiveInt(rawPageSize, 50, 1, 200);
  return { page, pageSize, nextCursor: String(page + 1) };
}

async function getThreadSystemId(threadId: string): Promise<string | null> {
  const result = await query<{ system_id: string }>(
    "SELECT thread_current_system($1) AS system_id",
    [threadId],
  );
  return result.rows[0]?.system_id ?? null;
}

async function loadThreadMatrix(systemId: string): Promise<V1ThreadMatrixNodeCell[]> {
  const result = await query<{
    node_id: string;
    concern: string;
    hash: string;
    title: string;
    kind: string;
    language: string;
    source_type: string;
    source_url: string | null;
    source_external_id: string | null;
  }>(
    `SELECT mr.node_id, mr.concern, d.hash, d.title, d.kind::text, d.language,
            d.source_type::text AS source_type, d.source_url, d.source_external_id
       FROM matrix_refs mr
       JOIN documents d ON d.system_id = mr.system_id AND d.hash = mr.doc_hash
      WHERE mr.system_id = $1`,
    [systemId],
  );

  const byCell = new Map<string, V1ThreadMatrixNodeCell>();
  for (const row of result.rows) {
    const key = `${row.node_id}|${row.concern}`;
    const existing = byCell.get(key);
    const doc = {
      hash: row.hash,
      title: row.title,
      kind: row.kind,
      language: row.language,
      sourceType: row.source_type,
      sourceUrl: row.source_url,
      sourceExternalId: row.source_external_id,
    };

    if (!existing) {
      byCell.set(key, {
      nodeId: row.node_id,
      concern: row.concern,
      docs: [doc],
      artifacts: [],
    });
      continue;
    }

    existing.docs.push(doc);
  }

  return Array.from(byCell.values());
}

function toTopology(nodes: Array<{ id: string; name: string; kind: string; parent_id: string | null; metadata: Record<string, unknown> }> ,
  edges: Array<{ id: string; from_node_id: string; to_node_id: string; type: string; metadata: Record<string, unknown> }>,
): { nodes: V1TopologyNode[]; edges: V1TopologyEdge[] } {
  const topoNodes = nodes.map((node) => {
    const layout = (node.metadata?.layout as Record<string, unknown>) ?? {};
    const layoutX = typeof layout.x === "number" ? layout.x : null;
    const layoutY = typeof layout.y === "number" ? layout.y : null;
    return {
      id: node.id,
      name: node.name,
      kind: node.kind,
      parentId: node.parent_id,
      layoutX,
      layoutY,
    };
  });

  const topoEdges = edges.map((edge) => ({
    id: edge.id,
    fromNodeId: edge.from_node_id,
    toNodeId: edge.to_node_id,
    type: edge.type,
    protocol: edge.metadata?.protocol ? String(edge.metadata.protocol) : null,
  }));

  return { nodes: topoNodes, edges: topoEdges };
}

async function collectOpenShipBundleFiles(bundleDir: string): Promise<V1OpenShipBundleFile[]> {
  const entries = await readdir(bundleDir, { withFileTypes: true });
  const files: V1OpenShipBundleFile[] = [];

  for (const entry of entries.sort((left, right) => left.name.localeCompare(right.name))) {
    const childPath = join(bundleDir, entry.name);
    if (entry.isDirectory()) {
      const nested = await collectOpenShipBundleFiles(childPath);
      for (const nestedFile of nested) {
        const nextPath = `${entry.name}/${nestedFile.path}`.replace(/\\+/g, "/");
        files.push({ path: nextPath, content: nestedFile.content });
      }
      continue;
    }

    if (!entry.isFile()) continue;

    const content = await readFile(childPath, "utf8");
    files.push({
      path: entry.name,
      content,
    });
  }

  return files;
}

function mapAssistantRunRow(run: V1AgentRunRow): {
  runId: string;
  threadId: string;
  status: AssistantRunStatus;
  mode: AssistantMode;
  prompt: string;
  systemPrompt: string | null;
  runResultStatus: "success" | "failed" | null;
  runResultMessages: string[];
  runResultChanges: unknown[];
  runError: string | null;
  createdAt: string;
  startedAt: string | null;
  completedAt: string | null;
} {
  return {
    runId: run.id,
    threadId: run.thread_id,
    status: run.status,
    mode: run.mode,
    prompt: run.prompt,
    systemPrompt: run.system_prompt,
    runResultStatus: run.run_result_status,
    runResultMessages: run.run_result_messages ?? [],
    runResultChanges: run.run_result_changes ?? [],
    runError: run.run_error,
    createdAt: new Date(run.created_at).toISOString(),
    startedAt: run.started_at ? new Date(run.started_at).toISOString() : null,
    completedAt: run.completed_at ? new Date(run.completed_at).toISOString() : null,
  };
}

async function publishThreadMatrixChanged(threadId: string, user: AuthUser, aggregateId: string): Promise<void> {
  await publishEvent({
    type: "thread.matrix.changed",
    aggregateType: "thread",
    aggregateId,
    orgId: user.orgId,
    traceId: threadId,
    payload: { threadId },
  }).catch(() => undefined);
}

export async function v1Routes(app: FastifyInstance) {
  app.addHook("preHandler", async (req, reply) => {
    await verifyAuth(req, reply);
    if (reply.sent) return;
  });

  app.get<{ Querystring: { page?: number; pageSize?: number; name?: string } }>(
    "/projects",
    async (req, reply) => {
      const user = (req as V1AuthRequest).auth;
      const page = parsePositiveInt(req.query.page, 1, 1, Number.MAX_SAFE_INTEGER);
      const pageSize = parsePositiveInt(req.query.pageSize, 50, 1, 200);
      const offset = (page - 1) * pageSize;

      const params: Array<unknown> = [user.id, pageSize + 1, offset];
      let nameClause = "";
      if (req.query.name?.trim()) {
        params.push(`%${req.query.name.trim()}%`);
        nameClause = `AND p.name ILIKE $${params.length}`;
      }

      const result = await query<V1ProjectListRow>(
        `SELECT
           p.id,
           p.name,
           p.description,
           p.visibility::text AS visibility,
           CASE
             WHEN p.owner_id = $1 THEN 'Owner'::text
             WHEN pc.role IS NOT NULL THEN pc.role::text
             ELSE 'Viewer'
           END AS access_role,
           owner_u.handle AS owner_handle,
           p.created_at,
           COALESCE((SELECT COUNT(*)::text FROM threads t WHERE t.project_id = p.id), '0') AS thread_count
         FROM projects p
         JOIN users owner_u ON owner_u.id = p.owner_id
         LEFT JOIN project_collaborators pc ON pc.project_id = p.id AND pc.user_id = $1
         WHERE p.is_archived = false
           AND (p.visibility = 'public' OR p.owner_id = $1 OR pc.user_id IS NOT NULL)
           ${nameClause}
         ORDER BY p.created_at DESC
         LIMIT $2
         OFFSET $3`,
        params,
      );

      const hasMore = result.rows.length > pageSize;
      const rows = hasMore ? result.rows.slice(0, pageSize) : result.rows;

      return {
        items: rows.map((row) => ({
          id: row.id,
          name: row.name,
          description: row.description,
          visibility: row.visibility,
          accessRole: row.access_role,
          ownerHandle: row.owner_handle,
          createdAt: row.created_at.toISOString(),
          threadCount: Number.parseInt(row.thread_count, 10),
        })),
        page,
        pageSize,
        nextCursor: hasMore ? String(page + 1) : null,
      };
    },
  );

  app.post<{ Body: { name?: string; description?: string; visibility?: V1ProjectVisibility } }>(
    "/projects",
    async (req, reply) => {
      const user = (req as V1AuthRequest).auth;
      const name = req.body?.name?.trim();
      const description = req.body?.description?.trim() || null;
      const visibility = req.body?.visibility ?? "private";

      if (!name) {
        return writeProblem(reply, 400, "Invalid name", "name is required.");
      }
      if (visibility !== "public" && visibility !== "private") {
        return writeProblem(reply, 400, "Invalid visibility", "visibility must be public or private.");
      }
      if (!/^[a-zA-Z0-9]([a-zA-Z0-9_-]*[a-zA-Z0-9])?$/.test(name) || name.length > 80) {
        return writeProblem(reply, 400, "Invalid project name", "Use a valid project name.");
      }

      const existing = await query<{ id: string }>(
        "SELECT 1 FROM projects WHERE owner_id = $1 AND name = $2",
        [user.id, name],
      );
      if ((existing.rowCount ?? 0) > 0) {
        return writeProblem(reply, 409, "Duplicate project", "A project with this name already exists.");
      }

      const projectId = randomUUID();
      const threadId = randomUUID();
      const systemId = randomUUID();
      const rootNodeId = "s.root";
      const now = new Date();

      const client = await pool.connect();
      let inTransaction = false;
      try {
        await client.query("BEGIN");
        inTransaction = true;

        await client.query(
          `INSERT INTO projects (id, name, description, visibility, owner_id)
           VALUES ($1, $2, $3, $4, $5)`,
          [projectId, name, description, visibility, user.id],
        );
        await client.query(
          `INSERT INTO systems (id, name, root_node_id, metadata, spec_version)
           VALUES ($1, $2, $3, '{}'::jsonb, 'openship/v1')`,
          [systemId, name, rootNodeId],
        );
        await client.query(
          `INSERT INTO nodes (id, system_id, kind, name, parent_id, metadata)
           VALUES ($1, $2, 'Root'::node_kind, $3, NULL, '{}'::jsonb)`,
          [rootNodeId, systemId, name],
        );
        await client.query(
          `INSERT INTO threads (id, title, description, project_id, created_by, seed_system_id, status)
           VALUES ($1, 'Project Creation', $2, $3, $4, $5, 'open')`,
          [threadId, description, projectId, user.id, systemId],
        );

        await client.query("COMMIT");
        inTransaction = false;
      } catch (err) {
        if (inTransaction) {
          await client.query("ROLLBACK").catch(() => {});
        }
        throw err;
      } finally {
        client.release();
      }

      return {
        id: projectId,
        name,
        description,
        visibility,
        accessRole: "Owner" as const,
        ownerHandle: user.handle,
        createdAt: now.toISOString(),
        threadCount: 1,
      };
    },
  );

  app.get<{ Querystring: { name?: string } }>(
    "/projects/check-name",
    async (req, reply) => {
      const user = (req as V1AuthRequest).auth;
      const name = req.query.name?.trim();
      if (!name) {
        return writeProblem(reply, 400, "Invalid name", "name is required.");
      }
      const exists = await query(
        "SELECT 1 FROM projects WHERE owner_id = $1 AND name = $2",
        [user.id, name],
      );
      return { available: exists.rowCount === 0 };
    },
  );

  app.get("/integrations", async (req) => {
    const user = (req as V1AuthRequest).auth;
    const result = await query<{
      provider: string;
      status: string;
      refresh_token_enc: string | null;
      token_expires_at: Date | null;
    }>(
      `SELECT provider::text AS provider, status, refresh_token_enc, token_expires_at
       FROM user_integrations
       WHERE user_id = $1`,
      [user.id],
    );

    const statusByProvider = new Map(result.rows.map((row) => [row.provider, row]));

    return {
      items: ["notion", "google"].map((provider) => {
        const providerRow = statusByProvider.get(provider);
        if (!providerRow) {
          return { provider, status: "disconnected" as V1IntegrationStatus };
        }

        const hasRefresh = providerRow.refresh_token_enc !== null;
        const status = providerRow.status as V1IntegrationStatus;
        if (
          status === "connected"
          && providerRow.token_expires_at
          && providerRow.token_expires_at.getTime() <= Date.now()
        ) {
          return {
            provider,
            status: hasRefresh ? ("needs_reauth" as V1IntegrationStatus) : ("expired" as V1IntegrationStatus),
          };
        }

        return { provider, status };
      }),
    };
  });

  app.get<{ Querystring: { projectId?: string; page?: number; pageSize?: number } }>(
    "/threads",
    async (req, reply) => {
      const user = (req as V1AuthRequest).auth;
      const page = parsePositiveInt(req.query.page, 1, 1, Number.MAX_SAFE_INTEGER);
      const pageSize = parsePositiveInt(req.query.pageSize, 50, 1, 200);
      const offset = (page - 1) * pageSize;

      const params: Array<unknown> = [user.id];
      const limitParam = params.length + 1;
      const offsetParam = params.length + 2;
      params.push(pageSize + 1, offset);

      const projectFilterClause = req.query.projectId
        ? `AND p.id = $${params.push(req.query.projectId)}`
        : "";

      const result = await query<V1ThreadSummaryRow>(
        `SELECT
           t.id,
           t.title,
           t.description,
           t.source_thread_id,
           p.name AS project_name,
           t.project_id,
           t.status,
           u.handle AS created_by_handle,
           owner_u.handle AS owner_handle,
           t.created_at,
           t.updated_at,
           COALESCE(NULLIF(pc.role::text, ''),
             CASE WHEN p.owner_id = $1 THEN 'Owner' ELSE 'Viewer' END
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
           ${projectFilterClause}
         ORDER BY t.updated_at DESC
         LIMIT $${limitParam}
         OFFSET $${offsetParam}`,
        params,
      );

      const hasMore = result.rows.length > pageSize;
      const rows = hasMore ? result.rows.slice(0, pageSize) : result.rows;

      return {
        items: rows.map((row) => ({
          id: row.id,
          projectId: row.project_id,
          sourceThreadId: row.source_thread_id,
          title: row.title,
          description: row.description,
          status: row.status,
          createdByHandle: row.created_by_handle,
          ownerHandle: row.owner_handle,
          projectName: row.project_name,
          createdAt: row.created_at.toISOString(),
          updatedAt: row.updated_at.toISOString(),
          accessRole: row.access_role,
        })),
        page,
        pageSize,
        nextCursor: hasMore ? String(page + 1) : null,
      } as V1ListCursor & { items: unknown[] };
    },
  );

  app.post<{ Body: { projectId?: string; title?: string; description?: string; sourceThreadId?: string } }>(
    "/threads",
    async (req, reply) => {
      const user = (req as V1AuthRequest).auth;
      const projectId = req.body?.projectId?.trim();
      const title = req.body?.title?.trim();
      const sourceThreadId = req.body?.sourceThreadId?.trim();
      if (typeof sourceThreadId === "string" && !isUuid(sourceThreadId)) {
        return writeProblem(reply, 400, "Invalid sourceThreadId", "sourceThreadId must be a UUID.");
      }
      const sourceValidThreadId = sourceThreadId && isUuid(sourceThreadId) ? sourceThreadId : null;

      if (!projectId || !isUuid(projectId)) {
        return writeProblem(reply, 400, "Invalid projectId", "projectId must be a UUID.");
      }

      const project = await resolveProjectAccess(projectId, user);
      if (!project) {
        return writeProblem(reply, 403, "Project not found", "Project not found or access denied.");
      }

      if (!canEdit(project.access_role)) {
        return forbiddenProblem(reply);
      }

      const sourceThread = sourceValidThreadId
        ? await query<{ id: string }>(`SELECT id FROM threads WHERE id = $1 AND project_id = $2`, [sourceValidThreadId, projectId])
        : await query<{ id: string }>(
        "SELECT id FROM threads WHERE project_id = $1 ORDER BY updated_at DESC LIMIT 1",
        [projectId],
        );
      if (sourceThread.rowCount === 0) {
        return writeProblem(reply, 400, "No source thread", "No source thread exists for project.");
      }

      const threadId = randomUUID();
      const resolvedTitle = title && title.length > 0 ? title : `Thread ${Date.now()}`;
      await query<{ id: string }>(
        "SELECT clone_thread($1, $2, $3, $4, $5, $6)",
        [
          threadId,
          sourceThread.rows[0]!.id,
          projectId,
          user.id,
          resolvedTitle,
          req.body?.description?.trim() || null,
        ],
      );

      const inserted = await query<V1ThreadSummaryRow>(
        `SELECT
           t.id,
           t.title,
           t.description,
           t.source_thread_id,
           p.name AS project_name,
           t.project_id,
           t.status,
           u.handle AS created_by_handle,
           owner_u.handle AS owner_handle,
           t.created_at,
           t.updated_at,
           'Owner'::text AS access_role
         FROM threads t
         JOIN projects p ON p.id = t.project_id
         JOIN users u ON u.id = t.created_by
         JOIN users owner_u ON owner_u.id = p.owner_id
         WHERE t.id = $1`,
        [threadId],
      );

      if (inserted.rowCount === 0) {
        return writeProblem(reply, 500, "Thread creation failed", "Failed to create thread.");
      }

      const created = inserted.rows[0];
      await publishEvent({
        type: "assistant.run.started",
        aggregateType: "thread",
        aggregateId: threadId,
        orgId: user.orgId,
        traceId: threadId,
        payload: {
          threadId,
          projectId,
          action: "create",
        },
      });

        return {
          id: created.id,
          projectId: created.project_id,
          sourceThreadId: created.source_thread_id,
          title: created.title,
        description: created.description,
        status: created.status,
        createdByHandle: created.created_by_handle,
        ownerHandle: created.owner_handle,
        projectName: created.project_name,
        createdAt: created.created_at.toISOString(),
        updatedAt: created.updated_at.toISOString(),
        accessRole: created.access_role,
      };
    },
  );

  app.get<{ Params: { threadId: string } }>(
    "/threads/:threadId",
    async (req, reply) => {
      const user = (req as V1AuthRequest).auth;
      const threadId = req.params.threadId;
      if (!isUuid(threadId)) {
        return writeProblem(reply, 400, "Invalid threadId", "threadId must be a UUID.");
      }

      const thread = await resolveThreadAccess(threadId, user);
      if (!thread) {
        return notFoundProblem(reply, "Thread not found");
      }

      const [projectRow, messagesResult, systemTopology, systemEdges, matrixCells, concernRows, matrixDocumentsRows] = await Promise.all([
        query<{ project_name: string; owner_handle: string; creator_handle: string }>(
          `SELECT p.name AS project_name, owner_u.handle AS owner_handle, u.handle AS creator_handle
           FROM threads t
           JOIN projects p ON p.id = t.project_id
           JOIN users owner_u ON owner_u.id = p.owner_id
           JOIN users u ON u.id = t.created_by
           WHERE t.id = $1`,
          [threadId],
        ),
        query<{
          id: string;
          action_id: string;
          action_type: string;
          action_position: number;
          role: "User" | "Assistant" | "System";
          content: string;
          created_at: Date;
        }>(
          `SELECT m.id, m.action_id, a.type::text AS action_type, a.position AS action_position,
                  m.role, m.content, m.created_at
             FROM messages m
             JOIN actions a ON a.thread_id = m.thread_id AND a.id = m.action_id
            WHERE m.thread_id = $1
            ORDER BY a.position, m.created_at`,
          [threadId],
        ),
        query<{ id: string; name: string; kind: string; parent_id: string | null; metadata: Record<string, unknown> }>(
          `SELECT n.id, n.name, n.kind::text AS kind, n.parent_id, n.metadata
             FROM nodes n
            WHERE n.system_id = (SELECT thread_current_system($1))
            ORDER BY n.id`,
          [threadId],
        ),
        query<{ id: string; from_node_id: string; to_node_id: string; type: string; metadata: Record<string, unknown> }>(
          `SELECT e.id, e.from_node_id, e.to_node_id, e.type::text AS type, e.metadata
           FROM edges e
           WHERE e.system_id = (SELECT thread_current_system($1))
           ORDER BY e.id`,
          [threadId],
        ),
        getThreadSystemId(threadId)
          .then((systemId) => systemId ? loadThreadMatrix(systemId) : Promise.resolve([] as V1ThreadMatrixNodeCell[])),
        getThreadSystemId(threadId).then(async (systemId) =>
          systemId
            ? query<V1ProjectThreadConcernRow>(
                `SELECT name, position FROM concerns WHERE system_id = $1 ORDER BY position`,
                [systemId],
              ).then((result) => result.rows)
            : Promise.resolve([] as V1ProjectThreadConcernRow[]),
        ),
        getThreadSystemId(threadId).then(async (systemId) =>
          systemId
            ? query<V1ProjectThreadDocumentRow>(
                `SELECT hash, kind::text AS kind, title, language, text, source_type::text AS source_type,
                        source_url, source_external_id, source_metadata, source_connected_user_id
                 FROM documents
                 WHERE system_id = $1
                 ORDER BY created_at, hash`,
                [systemId],
              ).then((result) => result.rows)
            : Promise.resolve([] as V1ProjectThreadDocumentRow[]),
        ),
      ]);

      const project = projectRow.rows[0];
      const topology = toTopology(systemTopology.rows, systemEdges.rows);
      const safeSystemId = await getThreadSystemId(threadId);

      return {
        thread: {
          id: thread.id,
          projectId: thread.project_id,
          title: thread.title,
          description: thread.description,
          status: thread.status,
          createdAt: thread.created_at.toISOString(),
          createdByHandle: project?.creator_handle ?? "unknown",
          ownerHandle: project?.owner_handle ?? "unknown",
          projectName: project?.project_name ?? "",
          accessRole: thread.access_role,
        },
        permissions: {
          canView: true,
          canEdit: canEdit(thread.access_role),
          canChat: canEdit(thread.access_role),
          canClose: canEdit(thread.access_role) && thread.status === "open",
          canCommit: canEdit(thread.access_role) && thread.status === "open",
        },
        topology,
        matrix: {
          nodes: matrixCells,
          concerns: concernRows.map((concern) => ({ name: concern.name, position: concern.position })),
          documents: matrixDocumentsRows.map((document) => ({
            hash: document.hash,
            kind: document.kind,
            title: document.title,
            language: document.language,
            text: document.text,
            sourceType: document.source_type,
            sourceUrl: document.source_url,
            sourceExternalId: document.source_external_id,
            sourceMetadata: document.source_metadata,
            sourceConnectedUserId: document.source_connected_user_id,
          })),
        },
        systemPrompt: null,
        systemPromptTitle: null,
        systemPrompts: [],
        chat: {
          messages: messagesResult.rows.map((message) => ({
            id: message.id,
            actionId: message.action_id,
            role: message.role,
            actionType: message.action_type,
            actionPosition: message.action_position,
            content: message.content,
            createdAt: message.created_at.toISOString(),
          })),
        },
      };
    },
  );

  app.patch<{ Params: { threadId: string }; Body: V1ThreadPatchBody }>(
    "/threads/:threadId",
    async (req, reply) => {
      const user = (req as V1AuthRequest).auth;
      const threadId = req.params.threadId;
      if (!isUuid(threadId)) {
        return writeProblem(reply, 400, "Invalid threadId", "threadId must be a UUID.");
      }

      const thread = await resolveThreadAccess(threadId, user);
      if (!thread) {
        return notFoundProblem(reply, "Thread not found");
      }

      if (!canEdit(thread.access_role)) {
        return forbiddenProblem(reply);
      }

      const updates: string[] = [];
      const params: Array<unknown> = [];
      if (typeof req.body?.title !== "undefined") {
        const nextTitle = req.body.title?.trim();
        if (!nextTitle) return writeProblem(reply, 400, "Invalid title", "title cannot be empty");
        updates.push(`title = $${updates.length + 1}`);
        params.push(nextTitle);
      }

      if (typeof req.body?.description !== "undefined") {
        const nextDescription = req.body.description === null ? null : req.body.description.trim();
        updates.push(`description = $${updates.length + 1}`);
        params.push(nextDescription);
      }

      if (typeof req.body?.status !== "undefined") {
        if (req.body.status !== "open" && req.body.status !== "closed" && req.body.status !== "committed") {
          return writeProblem(reply, 400, "Invalid status", "status must be open, closed, or committed");
        }
        updates.push(`status = $${updates.length + 1}`);
        params.push(req.body.status);
      }

      if (updates.length === 0) {
        return writeProblem(reply, 400, "No changes", "No updatable fields supplied.");
      }

      const result = await query<{ id: string; title: string; description: string | null; status: string }>(
        `UPDATE threads
            SET ${updates.join(", ")}
          WHERE id = $${updates.length + 1}
          RETURNING id, title, description, status`,
        [...params, threadId],
      );

      if (result.rowCount === 0) {
        return writeProblem(reply, 404, "Thread not found", "Thread not found");
      }

      const updated = result.rows[0];
      return {
        thread: {
          id: updated.id,
          title: updated.title,
          description: updated.description,
          status: updated.status,
        },
      };
    },
  );

  app.delete<{ Params: { threadId: string } }>(
    "/threads/:threadId",
    async (req, reply) => {
      const user = (req as V1AuthRequest).auth;
      const threadId = req.params.threadId;
      if (!isUuid(threadId)) {
        return writeProblem(reply, 400, "Invalid threadId", "threadId must be a UUID.");
      }

      const thread = await resolveThreadAccess(threadId, user);
      if (!thread) {
        return notFoundProblem(reply, "Thread not found");
      }

      if (!canEdit(thread.access_role)) {
        return forbiddenProblem(reply);
      }

      await query("DELETE FROM threads WHERE id = $1", [threadId]);
      reply.code(204).send();
    },
  );

  app.get<{ Params: { threadId: string } }>(
    "/threads/:threadId/matrix",
    async (req, reply) => {
      const user = (req as V1AuthRequest).auth;
      const threadId = req.params.threadId;
      if (!isUuid(threadId)) {
        return writeProblem(reply, 400, "Invalid threadId", "threadId must be a UUID.");
      }

      const thread = await resolveThreadAccess(threadId, user);
      if (!thread) {
        return notFoundProblem(reply, "Thread not found");
      }

      const systemId = await getThreadSystemId(threadId);
      if (!systemId) {
        return writeProblem(reply, 500, "System missing", "Thread has no current system");
      }

      const [nodeResult, edgeResult, cells, concernRows, documentRows] = await Promise.all([
        query<{ id: string; name: string; kind: string; parent_id: string | null; metadata: Record<string, unknown> }>(
          `SELECT id, name, kind::text AS kind, parent_id, metadata
           FROM nodes
           WHERE system_id = $1 ORDER BY id`,
          [systemId],
        ),
        query<{ id: string; from_node_id: string; to_node_id: string; type: string; metadata: Record<string, unknown> }>(
          `SELECT id, from_node_id, to_node_id, type::text AS type, metadata
           FROM edges
           WHERE system_id = $1 ORDER BY id`,
          [systemId],
        ),
        loadThreadMatrix(systemId),
        query<V1ProjectThreadConcernRow>(
          `SELECT name, position FROM concerns WHERE system_id = $1 ORDER BY position`,
          [systemId],
        ).then((result) => result.rows),
        query<V1ProjectThreadDocumentRow>(
          `SELECT hash, kind::text AS kind, title, language, text, source_type::text AS source_type,
                  source_url, source_external_id, source_metadata, source_connected_user_id
           FROM documents
           WHERE system_id = $1
           ORDER BY created_at, hash`,
          [systemId],
        ).then((result) => result.rows),
      ]);

      return {
        threadId,
        systemId,
        topology: toTopology(nodeResult.rows, edgeResult.rows),
        matrix: {
          nodes: cells,
          concerns: concernRows.map((concern) => ({ name: concern.name, position: concern.position })),
          documents: documentRows.map((document) => ({
            hash: document.hash,
            kind: document.kind,
            title: document.title,
            language: document.language,
            text: document.text,
            sourceType: document.source_type,
            sourceUrl: document.source_url,
            sourceExternalId: document.source_external_id,
            sourceMetadata: document.source_metadata,
            sourceConnectedUserId: document.source_connected_user_id,
          })),
        },
      };
    },
  );

  app.patch<{ Params: { threadId: string }; Body: V1MatrixPatchBody }>(
    "/threads/:threadId/matrix",
    async (req, reply) => {
      const user = (req as V1AuthRequest).auth;
      const threadId = req.params.threadId;
      if (!isUuid(threadId)) {
        return writeProblem(reply, 400, "Invalid threadId", "threadId must be a UUID.");
      }

      const thread = await resolveThreadAccess(threadId, user);
      if (!thread) {
        return notFoundProblem(reply, "Thread not found");
      }

      if (!canEdit(thread.access_role)) {
        return forbiddenProblem(reply);
      }

      const layout = normalizeToplologyPositions(req.body ?? {});
      if (layout.length === 0) {
        return writeProblem(reply, 400, "Invalid matrix payload", "No valid node layout entries provided.");
      }

      const systemId = await getThreadSystemId(threadId);
      if (!systemId) {
        return writeProblem(reply, 500, "System missing", "Thread has no current system");
      }

      let changed = 0;
      for (const next of layout) {
        const result = await query<{ changed: number }>(
          `UPDATE nodes
              SET metadata = jsonb_set(jsonb_set(metadata, '{layout}', jsonb_build_object('x', $3, 'y', $4), true), '{layout}', metadata->'layout', true)
            WHERE system_id = $1 AND id = $2`,
          [systemId, next.nodeId, next.x, next.y],
        );
        changed += result.rowCount ?? 0;
      }

      if (changed === 0) {
        return notFoundProblem(reply, "No nodes updated");
      }

      await publishThreadMatrixChanged(threadId, user, threadId);
      const cells = await loadThreadMatrix(systemId);
      return {
        threadId,
        systemId,
        changed,
        matrix: {
          nodes: cells,
        },
      };
    },
  );

  app.get<{ Params: { threadId: string } }>(
    "/threads/:threadId/openship/bundle",
    async (req, reply) => {
      const user = (req as V1AuthRequest).auth;
      const threadId = req.params.threadId;
      if (!isUuid(threadId)) {
        return writeProblem(reply, 400, "Invalid threadId", "threadId must be a UUID.");
      }

      const thread = await resolveThreadAccess(threadId, user);
      if (!thread) {
        return notFoundProblem(reply, "Thread not found");
      }

      if (!canEdit(thread.access_role)) {
        return forbiddenProblem(reply);
      }

      const systemId = await getThreadSystemId(threadId);
      if (!systemId) {
        return writeProblem(reply, 500, "System missing", "Thread has no current system");
      }

      const workspace = await mkdtemp(join(tmpdir(), "staffx-openship-bundle-"));
      try {
        const bundleDir = await generateOpenShipFileBundle(threadId, workspace);
        const files = await collectOpenShipBundleFiles(bundleDir);
        const descriptor: V1OpenShipBundleDescriptor = {
          threadId,
          systemId,
          generatedAt: new Date().toISOString(),
          files,
        };
        return descriptor;
      } finally {
        await rm(workspace, { recursive: true, force: true });
      }
    },
  );

  app.post<{ Params: { threadId: string }; Body: V1ChatMessageRequest }>(
    "/threads/:threadId/chat",
    async (req, reply) => {
      const user = (req as V1AuthRequest).auth;
      const threadId = req.params.threadId;
      if (!isUuid(threadId)) {
        return writeProblem(reply, 400, "Invalid threadId", "threadId must be a UUID.");
      }

      const thread = await resolveThreadAccess(threadId, user);
      if (!thread) {
        return notFoundProblem(reply, "Thread not found");
      }

      if (!canEdit(thread.access_role)) {
        return forbiddenProblem(reply);
      }

      const content = req.body?.content?.trim();
      if (!content) {
        return writeProblem(reply, 400, "Invalid message", "content is required");
      }

      const role = req.body?.role ?? "User";
      if (role !== "User" && role !== "Assistant" && role !== "System") {
        return writeProblem(reply, 400, "Invalid role", "role must be User, Assistant, or System");
      }

      const actionId = randomUUID();
      const messageId = randomUUID();
      const positionResult = await query<{ position: number }>(
        `SELECT COALESCE(MAX(position), 0) + 1 AS position
         FROM actions
         WHERE thread_id = $1`,
        [threadId],
      );
      const actionPosition = positionResult.rows[0]?.position ?? 1;

      await query<{ id: string }>(
        `INSERT INTO actions (id, thread_id, position, type, title)
         VALUES ($1, $2, $3, 'Chat'::action_type, 'Chat message')`,
        [actionId, threadId, actionPosition],
      );

      const messageResult = await query<{
        id: string;
        action_id: string;
        role: "User" | "Assistant" | "System";
        content: string;
        created_at: Date;
      }>(
        `INSERT INTO messages (id, thread_id, action_id, role, content, position)
         VALUES ($1, $2, $3, $4::message_role, $5, 1)
         RETURNING id, action_id, role, content, created_at`,
        [messageId, threadId, actionId, role, content],
      );

      if (messageResult.rowCount === 0) {
        return writeProblem(reply, 500, "Failed to append message", "Failed to append message.");
      }

      const inserted = messageResult.rows[0];
      return {
        messages: [{
          id: inserted.id,
          actionId: inserted.action_id,
          role: inserted.role,
          actionType: "Chat",
          actionPosition,
          content: inserted.content,
          createdAt: inserted.created_at.toISOString(),
        }],
      };
    },
  );

  app.post<{ Params: { threadId: string; assistantType: AssistantMode }; Body: V1RunStartBody }>(
    "/threads/:threadId/assistants/:assistantType/runs",
    async (req, reply) => {
      const user = (req as V1AuthRequest).auth;
      const threadId = req.params.threadId;
      const mode = req.params.assistantType;
      const chatMessageId = req.body?.chatMessageId?.trim() || null;
      const prompt = req.body?.prompt?.trim();

      if (!isUuid(threadId)) {
        return writeProblem(reply, 400, "Invalid threadId", "threadId must be a UUID.");
      }

      if (mode !== "direct" && mode !== "plan") {
        return writeProblem(reply, 400, "Invalid assistant type", "assistantType must be direct or plan");
      }

      const thread = await resolveThreadAccess(threadId, user);
      if (!thread) {
        return notFoundProblem(reply, "Thread not found");
      }

      if (!canEdit(thread.access_role)) {
        return forbiddenProblem(reply);
      }

      if (chatMessageId && !isUuid(chatMessageId)) {
        return writeProblem(reply, 400, "Invalid chatMessageId", "chatMessageId must be a UUID.");
      }

      const runId = await enqueueAgentRunWithWait({
        threadId,
        projectId: thread.project_id,
        requestedByUserId: user.id,
        mode,
        planActionId: null,
        chatMessageId,
        prompt: prompt ?? "Run this request.",
      });

      await publishEvent({
        type: "assistant.run.started",
        aggregateType: "assistant-run",
        aggregateId: runId,
        orgId: user.orgId,
        traceId: threadId,
        payload: {
          threadId,
          mode,
          status: "queued",
        },
      });
      await publishEvent({
        type: "assistant.run.waiting_input",
        aggregateType: "assistant-run",
        aggregateId: runId,
        orgId: user.orgId,
        traceId: threadId,
        payload: {
          threadId,
          mode,
          status: "waiting_input",
        },
      });

      return {
        runId,
        status: "queued" as AssistantRunStatus,
        mode,
        threadId,
      };
    },
  );

  app.get<{ Params: { runId: string } }>(
    "/assistant-runs/:runId",
    async (req, reply) => {
      const user = (req as V1AuthRequest).auth;
      const runId = req.params.runId;
      if (!isUuid(runId)) {
        return writeProblem(reply, 400, "Invalid run id", "runId must be a UUID.");
      }
      const run = await getAgentRunById(runId);
      if (!run) {
        return notFoundProblem(reply, "Run not found");
      }

      const thread = await resolveThreadAccess(run.thread_id, user);
      if (!thread) {
        return forbiddenProblem(reply);
      }

      return mapAssistantRunRow(run);
    },
  );

  app.post<{ Params: { runId: string }; Body: V1RunClaimBody }>(
    "/assistant-runs/:runId/claim",
    async (req, reply) => {
      const user = (req as V1AuthRequest).auth;
      const runId = req.params.runId;
      if (!isUuid(runId)) {
        return writeProblem(reply, 400, "Invalid run id", "runId must be a UUID.");
      }
      const run = await getAgentRunById(runId);
      if (!run) {
        return notFoundProblem(reply, "Run not found");
      }

      const thread = await resolveThreadAccess(run.thread_id, user);
      if (!thread) {
        return forbiddenProblem(reply, "Access denied for this run.");
      }

      const runnerId = req.body?.runnerId?.trim() || `desktop-${randomUUID()}`;
      const claimed = await claimAgentRunById(runId, runnerId);
      if (!claimed) {
        return writeProblem(reply, 409, "Run unavailable", "Run is not available for claiming");
      }

      await publishEvent({
        type: "assistant.run.progress",
        aggregateType: "assistant-run",
        aggregateId: runId,
        orgId: user.orgId,
        traceId: run.thread_id,
        payload: {
          status: claimed.status,
          runnerId,
          threadId: run.thread_id,
        },
      });

      const completedRun = await getAgentRunById(runId);
      if (!completedRun) return notFoundProblem(reply, "Run not found");
      return mapAssistantRunRow(completedRun);
    },
  );

  app.post<{ Params: { runId: string }; Body: V1RunCompleteBody }>(
    "/assistant-runs/:runId/complete",
    async (req, reply) => {
      const user = (req as V1AuthRequest).auth;
      const runId = req.params.runId;
      if (!isUuid(runId)) {
        return writeProblem(reply, 400, "Invalid run id", "runId must be a UUID.");
      }
      if (req.body?.status !== "success" && req.body?.status !== "failed") {
        return writeProblem(reply, 400, "Invalid status", "status must be success or failed");
      }

      const run = await getAgentRunById(runId);
      if (!run) {
        return notFoundProblem(reply, "Run not found");
      }

      const thread = await resolveThreadAccess(run.thread_id, user);
      if (!thread) {
        return forbiddenProblem(reply, "Access denied for this run.");
      }

      const messages = Array.isArray(req.body?.messages)
        ? req.body.messages.filter((message) => typeof message === "string").map((message) => message.trim()).filter(Boolean)
        : [];

      if (messages.length === 0) {
        return writeProblem(reply, 400, "Invalid payload", "messages must be a non-empty list of strings");
      }

      const success = req.body.status === "success";
      const updated = await updateAgentRunResult(
        runId,
        success ? "success" : "failed",
        {
          status: success ? "success" : "failed",
          messages,
          changes: parseRunPlanChanges(req.body.changes),
          error: req.body.error,
        },
        req.body.error,
        req.body.runnerId,
      );

      if (!updated) {
        return writeProblem(reply, 409, "Run cannot be completed", "Run was already finalized");
      }

      await publishEvent({
        type: success ? "assistant.run.completed" : "assistant.run.failed",
        aggregateType: "assistant-run",
        aggregateId: runId,
        orgId: user.orgId,
        traceId: run.thread_id,
        payload: {
          threadId: run.thread_id,
          status: req.body.status,
          messages,
        },
      });
      await publishEvent({
        type: "chat.session.finished",
        aggregateType: "thread",
        aggregateId: run.thread_id,
        orgId: user.orgId,
        traceId: run.thread_id,
        payload: {
          threadId: run.thread_id,
          runId,
          status: req.body.status,
        },
      });

      const completedRun = await getAgentRunById(runId);
      if (!completedRun) return notFoundProblem(reply, "Run not found");
      return mapAssistantRunRow(completedRun);
    },
  );

  app.post<{ Params: { runId: string } }>(
    "/assistant-runs/:runId/cancel",
    async (req, reply) => {
      const user = (req as V1AuthRequest).auth;
      const runId = req.params.runId;
      if (!isUuid(runId)) {
        return writeProblem(reply, 400, "Invalid run id", "runId must be a UUID.");
      }
      const run = await getAgentRunById(runId);
      if (!run) {
        return notFoundProblem(reply, "Run not found");
      }

      const thread = await resolveThreadAccess(run.thread_id, user);
      if (!thread) {
        return forbiddenProblem(reply, "Access denied for this run.");
      }

      const canceled = await query<V1AgentRunRow>(
        `UPDATE agent_runs
            SET status = 'cancelled', run_result_status = 'failed', run_error = COALESCE($2, run_error),
                completed_at = NOW(), updated_at = NOW()
          WHERE id = $1 AND status IN ('queued', 'running')
          RETURNING id, thread_id, status, mode, prompt, system_prompt, run_result_status,
                    run_result_messages, run_result_changes, run_error, created_at, started_at, completed_at`,
        [runId, "Cancelled by user"],
      );

      if (canceled.rowCount === 0) {
        return writeProblem(reply, 409, "Run cannot be cancelled", "Run is already finalized");
      }

      await publishEvent({
        type: "assistant.run.cancelled",
        aggregateType: "assistant-run",
        aggregateId: runId,
        orgId: user.orgId,
        traceId: run.thread_id,
        payload: {
          threadId: run.thread_id,
          status: "cancelled",
        },
      });
      await publishEvent({
        type: "chat.session.finished",
        aggregateType: "thread",
        aggregateId: run.thread_id,
        orgId: user.orgId,
        traceId: run.thread_id,
        payload: {
          threadId: run.thread_id,
          runId,
          status: "cancelled",
        },
      });

      return mapAssistantRunRow(canceled.rows[0] as V1AgentRunRow);
    },
  );

  app.get<{ Querystring: { since?: string; limit?: number } }>(
    "/events",
    async (req, reply) => {
      const user = (req as V1AuthRequest).auth;
      const limit = parsePositiveInt(req.query.limit, 100, 1, 500);

      const cursor = readEventCursor(req.query.since);
      if (req.query.since && !cursor) {
        return writeProblem(
          reply,
          400,
          "Invalid cursor",
          "since must be RFC3339 timestamp or event cursor.",
        );
      }

      const items = await queryEvents({
        orgId: user.orgId,
        since: cursor,
        limit,
      });

      return {
        items: items.items,
        nextCursor: items.nextCursor,
        page: 1,
        pageSize: limit,
      };
    },
  );

  app.get<{ Querystring: { since?: string; limit?: number } }>(
    "/events/stream",
    async (req, reply) => {
      const user = (req as V1AuthRequest).auth;
      const limit = parsePositiveInt(req.query.limit, 100, 1, 500);

      let cursor: string | undefined;
      const lastEventId = req.headers["last-event-id"];
      const lastEventIdHeader = Array.isArray(lastEventId) ? lastEventId[0] : lastEventId;
      if (typeof lastEventIdHeader === "string" && lastEventIdHeader.trim()) {
        cursor = readEventCursor(lastEventIdHeader);
      } else if (req.query.since) {
        cursor = readEventCursor(req.query.since);
      }

      if (req.query.since && !cursor) {
        return writeProblem(
          reply,
          400,
          "Invalid cursor",
          "since must be RFC3339 timestamp or event cursor.",
        );
      }

      reply.raw.setHeader("Content-Type", "text/event-stream");
      reply.raw.setHeader("Cache-Control", "no-cache");
      reply.raw.setHeader("Connection", "keep-alive");
      reply.raw.write("retry: 3000\n\n");

      let closed = false;
      const heartbeat = setInterval(() => {
        if (closed) return;
        reply.raw.write(": heartbeat\n\n");
      }, 15000);

      reply.raw.on("close", () => {
        closed = true;
        clearInterval(heartbeat);
      });

      while (!closed) {
        const queryResult = await queryEvents({
          orgId: user.orgId,
          since: cursor,
          limit,
        });

        for (const item of queryResult.items) {
          const itemCursor = encodeCursor(item);
          reply.raw.write(`id: ${itemCursor}\n`);
          reply.raw.write(`event: ${item.type}\n`);
          reply.raw.write(`data: ${JSON.stringify(item)}\n\n`);
          cursor = itemCursor;
        }

        if (queryResult.items.length === 0) {
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }

      clearInterval(heartbeat);
      reply.raw.end();
    },
  );
}
