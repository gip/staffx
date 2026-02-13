import { createHash, randomUUID } from "node:crypto";
import type { FastifyInstance, FastifyReply } from "fastify";
import pool, { query } from "../db.js";
import { verifyAuth } from "../auth.js";

const EDIT_ROLES = new Set(["Owner", "Editor"]);
const PLACEHOLDER_ASSISTANT_MESSAGE = "Received. I captured your request in this thread.";

interface ThreadContextRow {
  thread_id: string;
  project_thread_id: number;
  title: string | null;
  description: string | null;
  status: string;
  created_at: Date;
  created_by_handle: string;
  project_name: string;
  owner_handle: string;
  access_role: string;
}

interface SystemRow {
  system_id: string;
}

interface TopologyNodeRow {
  id: string;
  name: string;
  kind: string;
  parent_id: string | null;
  layout_x: number | null;
  layout_y: number | null;
}

interface TopologyEdgeRow {
  id: string;
  type: string;
  from_node_id: string;
  to_node_id: string;
  protocol: string | null;
}

interface ConcernRow {
  name: string;
  position: number;
}

interface MatrixRefRow {
  node_id: string;
  concern: string;
  doc_hash: string;
  ref_type: "Feature" | "Spec" | "Skill";
  doc_title: string;
  doc_kind: "Feature" | "Spec" | "Skill";
  doc_language: string;
}

interface MatrixDocumentRow {
  hash: string;
  kind: "Feature" | "Spec" | "Skill";
  title: string;
  language: string;
  text: string;
}

interface ArtifactRow {
  id: string;
  node_id: string;
  concern: string;
  type: string;
  language: string;
  text: string | null;
}

interface ChatMessageRow {
  id: string;
  action_id: string;
  role: "User" | "Assistant" | "System";
  content: string;
  created_at: Date;
}

interface BeginActionRow {
  output_system_id: string | null;
}

interface UpsertThreadRow {
  id: string;
  project_thread_id: number;
  title: string;
  description: string | null;
  status: string;
}

interface ChangedRow {
  changed: number;
}

interface ThreadContext {
  threadId: string;
  projectThreadId: number;
  title: string;
  description: string | null;
  status: string;
  createdAt: Date;
  createdByHandle: string;
  projectName: string;
  ownerHandle: string;
  accessRole: string;
}

interface MatrixDoc {
  hash: string;
  title: string;
  kind: "Feature" | "Spec" | "Skill";
  language: string;
  refType: "Feature" | "Spec" | "Skill";
}

interface ArtifactRef {
  id: string;
  type: string;
  language: string;
  text: string | null;
}

interface MatrixCell {
  nodeId: string;
  concern: string;
  docs: MatrixDoc[];
  artifacts: ArtifactRef[];
}

interface MatrixRefBody {
  nodeId: string;
  concern: string;
  docHash: string;
  refType: "Feature" | "Spec" | "Skill";
}

type DocKind = "Feature" | "Spec" | "Skill";

interface MatrixDocumentCreateBody {
  title: string;
  kind: DocKind;
  language: string;
  name: string;
  description: string;
  body: string;
  attach?: {
    nodeId: string;
    concern: string;
    refType: DocKind;
  };
}

interface MatrixDocumentReplaceBody {
  title?: string;
  name?: string;
  description?: string;
  language?: string;
  body?: string;
}

interface ParsedDocumentText {
  name: string;
  description: string;
  body: string;
}

interface TopologyLayoutBody {
  positions: Array<{
    nodeId: string;
    x: number;
    y: number;
  }>;
}

interface ThreadPatchBody {
  title?: string;
  description?: string | null;
}

interface ChatMessageBody {
  content: string;
}

function parseThreadId(threadId: string): number | null {
  const parsed = Number(threadId);
  if (!Number.isInteger(parsed) || parsed <= 0) return null;
  return parsed;
}

function computeDocumentHash(document: Pick<MatrixDocumentCreateBody, "kind" | "title" | "language" | "body">) {
  const payload = [document.kind, document.title, document.language, document.body].join("\n");
  return `sha256:${createHash("sha256").update(payload, "utf8").digest("hex")}`;
}

function deriveDocumentName(title: string) {
  return title
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function isValidDocumentName(name: string) {
  return /^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(name);
}

function normalizeDocumentText(rawText: string) {
  return rawText ?? "";
}

function parseDocumentText(rawText: string): ParsedDocumentText {
  const text = normalizeDocumentText(rawText).replace(/\r\n/g, "\n");
  if (!text.startsWith("---\n")) {
    return { name: "", description: "", body: text.trim() };
  }

  const match = text.match(/^---\n([\s\S]*?)\n---\n?([\s\S]*)$/);
  if (!match) {
    return { name: "", description: "", body: text.trim() };
  }

  const frontMatter = match[1];
  const body = match[2].trim();
  const parsed: ParsedDocumentText = { name: "", description: "", body };

  for (const line of frontMatter.split("\n")) {
    const [key, rawValue] = line.split(":", 2);
    if (!key || rawValue === undefined) continue;
    const trimmedKey = key.trim();
    const trimmedValue = rawValue.trim();
    if (trimmedKey === "name") {
      parsed.name = trimmedValue;
    } else if (trimmedKey === "description") {
      parsed.description = trimmedValue;
    }
  }

  return parsed;
}

function buildDocumentText({ name, description, body }: { name: string; description: string; body: string }) {
  const normalizedDescription = description.trim().replace(/\r?\n/g, " ");
  const normalizedBody = body.trim();
  return [
    "---",
    `name: ${name}`,
    `description: ${normalizedDescription}`,
    "---",
    normalizedBody,
  ].join("\n");
}

function normalizeMatrixDocumentCreateBody(body: unknown): MatrixDocumentCreateBody | null {
  if (!body || typeof body !== "object") return null;
  const parsed = body as Partial<MatrixDocumentCreateBody>;

  if (
    typeof parsed.title !== "string" ||
    typeof parsed.kind !== "string" ||
    typeof parsed.language !== "string" ||
    typeof parsed.name !== "string" ||
    typeof parsed.description !== "string" ||
    typeof parsed.body !== "string"
  ) {
    return null;
  }

  const title = parsed.title.trim();
  const kind = parsed.kind;
  const language = parsed.language.trim() || "en";
  const name = parsed.name.trim();
  const description = parsed.description.trim();
  const bodyText = parsed.body;

  if (!title) return null;
  if (!["Feature", "Spec", "Skill"].includes(kind)) return null;
  if (!language) return null;
  if (!isValidDocumentName(name)) return null;

  let attach: { nodeId: string; concern: string; refType: DocKind } | undefined;
  if (parsed.attach) {
    if (typeof parsed.attach !== "object") return null;
    const attachParsed = parsed.attach as Partial<{ nodeId: string; concern: string; refType: string }>;
    if (
      typeof attachParsed.nodeId !== "string" ||
      typeof attachParsed.concern !== "string" ||
      typeof attachParsed.refType !== "string"
    ) {
      return null;
    }
    const nodeId = attachParsed.nodeId.trim();
    const concern = attachParsed.concern.trim();
    const refType = attachParsed.refType;
    if (!nodeId || !concern) return null;
    if (!["Feature", "Spec", "Skill"].includes(refType)) return null;
    attach = { nodeId, concern, refType: refType as DocKind };
  }

  return {
    title,
    kind: kind as DocKind,
    language,
    name,
    description,
    body: bodyText,
    attach,
  };
}

function normalizeMatrixDocumentReplaceBody(body: unknown): MatrixDocumentReplaceBody | null {
  if (!body || typeof body !== "object") return null;
  const parsed = body as Partial<MatrixDocumentReplaceBody>;
  const hasAny =
    typeof parsed.title !== "undefined" ||
    typeof parsed.name !== "undefined" ||
    typeof parsed.description !== "undefined" ||
    typeof parsed.language !== "undefined" ||
    typeof parsed.body !== "undefined";

  if (!hasAny) return null;

  const normalized: MatrixDocumentReplaceBody = {};
  if (typeof parsed.title !== "undefined") {
    if (typeof parsed.title !== "string") return null;
    const title = parsed.title.trim();
    if (!title) return null;
    normalized.title = title;
  }
  if (typeof parsed.name !== "undefined") {
    if (typeof parsed.name !== "string" || !isValidDocumentName(parsed.name.trim())) return null;
    normalized.name = parsed.name.trim();
  }
  if (typeof parsed.description !== "undefined") {
    if (typeof parsed.description !== "string") return null;
    normalized.description = parsed.description.trim();
  }
  if (typeof parsed.language !== "undefined") {
    if (typeof parsed.language !== "string" || !parsed.language.trim()) return null;
    normalized.language = parsed.language.trim();
  }
  if (typeof parsed.body !== "undefined") {
    if (typeof parsed.body !== "string") return null;
    normalized.body = parsed.body;
  }

  return normalized;
}

async function resolveThreadContext(
  userId: string,
  handle: string,
  projectName: string,
  projectThreadId: number,
): Promise<ThreadContext | null> {
  const result = await query<ThreadContextRow>(
    `SELECT
       t.id AS thread_id,
       t.project_thread_id,
       t.title,
       t.description,
       t.status,
       t.created_at,
       creator.handle AS created_by_handle,
       p.name AS project_name,
       owner.handle AS owner_handle,
       up.access_role
     FROM user_projects up
     JOIN projects p ON p.id = up.id
     JOIN users owner ON owner.id = p.owner_id
     JOIN threads t ON t.project_id = p.id
     JOIN users creator ON creator.id = t.created_by
     WHERE up.user_id = $1
       AND owner.handle = $2
       AND p.name = $3
       AND t.project_thread_id = $4
     LIMIT 1`,
    [userId, handle, projectName, projectThreadId],
  );

  if (result.rowCount === 0) return null;

  const row = result.rows[0];
  return {
    threadId: row.thread_id,
    projectThreadId: row.project_thread_id,
    title: row.title ?? "Untitled",
    description: row.description,
    status: row.status,
    createdAt: row.created_at,
    createdByHandle: row.created_by_handle,
    projectName: row.project_name,
    ownerHandle: row.owner_handle,
    accessRole: row.access_role,
  };
}

function buildThreadPayload(context: ThreadContext) {
  return {
    id: context.threadId,
    projectThreadId: context.projectThreadId,
    title: context.title,
    description: context.description,
    status: context.status,
    createdAt: context.createdAt,
    createdByHandle: context.createdByHandle,
    ownerHandle: context.ownerHandle,
    projectName: context.projectName,
    accessRole: context.accessRole,
  };
}

function canEdit(accessRole: string) {
  return EDIT_ROLES.has(accessRole);
}

function matrixCellKey(nodeId: string, concern: string) {
  return `${nodeId}::${concern}`;
}

function normalizeMatrixMutationBody(
  body: unknown,
): { nodeId: string; concern: string; docHash: string; refType: "Feature" | "Spec" | "Skill" } | null {
  if (!body || typeof body !== "object") return null;
  const parsed = body as Partial<MatrixRefBody>;

  if (
    typeof parsed.nodeId !== "string" ||
    typeof parsed.concern !== "string" ||
    typeof parsed.docHash !== "string" ||
    typeof parsed.refType !== "string"
  ) {
    return null;
  }

  const nodeId = parsed.nodeId.trim();
  const concern = parsed.concern.trim();
  const docHash = parsed.docHash.trim();
  const refType = parsed.refType;

  if (!nodeId || !concern || !docHash) return null;
  if (refType !== "Feature" && refType !== "Spec" && refType !== "Skill") return null;

  return { nodeId, concern, docHash, refType };
}

function normalizeTopologyLayoutBody(
  body: unknown,
): { positions: Array<{ nodeId: string; x: number; y: number }> } | null {
  if (!body || typeof body !== "object") return null;
  const parsed = body as Partial<TopologyLayoutBody>;
  if (!Array.isArray(parsed.positions) || parsed.positions.length === 0) return null;

  const seen = new Set<string>();
  const positions: Array<{ nodeId: string; x: number; y: number }> = [];
  for (const position of parsed.positions) {
    if (!position || typeof position !== "object") return null;
    const entry = position as Partial<TopologyLayoutBody["positions"][number]>;
    if (typeof entry.nodeId !== "string" || typeof entry.x !== "number" || typeof entry.y !== "number") {
      return null;
    }

    const nodeId = entry.nodeId.trim();
    if (!nodeId) return null;
    if (!Number.isFinite(entry.x) || !Number.isFinite(entry.y)) return null;
    if (seen.has(nodeId)) continue;
    seen.add(nodeId);

    positions.push({ nodeId, x: entry.x, y: entry.y });
  }

  if (positions.length === 0) return null;
  return { positions };
}

async function getThreadSystemId(threadId: string): Promise<string | null> {
  const result = await query<SystemRow>(
    "SELECT thread_current_system($1) AS system_id",
    [threadId],
  );
  return result.rows[0]?.system_id ?? null;
}

async function getMatrixCell(systemId: string, nodeId: string, concern: string): Promise<MatrixCell> {
  const [docsResult, artifactsResult] = await Promise.all([
    query<MatrixRefRow>(
      `SELECT
         mr.node_id,
         mr.concern,
         mr.doc_hash,
         mr.ref_type,
         d.title AS doc_title,
         d.kind AS doc_kind,
         d.language AS doc_language
       FROM matrix_refs mr
       JOIN documents d ON d.system_id = mr.system_id AND d.hash = mr.doc_hash
       WHERE mr.system_id = $1
         AND mr.node_id = $2
         AND mr.concern_hash = md5($3)
         AND mr.concern = $3
       ORDER BY mr.ref_type, d.title`,
      [systemId, nodeId, concern],
    ),
    query<ArtifactRow>(
      `SELECT id, node_id, concern, type, language, text
       FROM artifacts
       WHERE system_id = $1 AND node_id = $2 AND concern = $3
       ORDER BY created_at, id`,
      [systemId, nodeId, concern],
    ),
  ]);

  return {
    nodeId,
    concern,
    docs: docsResult.rows.map((row) => ({
      hash: row.doc_hash,
      title: row.doc_title,
      kind: row.doc_kind,
      language: row.doc_language,
      refType: row.ref_type,
    })),
    artifacts: artifactsResult.rows.map((row) => ({
      id: row.id,
      type: row.type,
      language: row.language,
      text: row.text,
    })),
  };
}

async function requireContext(
  reply: FastifyReply,
  userId: string,
  handle: string,
  projectName: string,
  threadId: string,
): Promise<ThreadContext | null> {
  const parsedThreadId = parseThreadId(threadId);
  if (!parsedThreadId) {
    await reply.code(400).send({ error: "Invalid thread id" });
    return null;
  }

  const context = await resolveThreadContext(userId, handle, projectName, parsedThreadId);
  if (!context) {
    await reply.code(404).send({ error: "Thread not found" });
    return null;
  }

  return context;
}

export async function threadRoutes(app: FastifyInstance) {
  app.addHook("preHandler", verifyAuth);

  app.get<{ Params: { handle: string; projectName: string; threadId: string } }>(
    "/projects/:handle/:projectName/thread/:threadId",
    async (req, reply) => {
      const { handle, projectName, threadId } = req.params;
      const context = await requireContext(reply, req.auth.id, handle, projectName, threadId);
      if (!context) return;

      const systemId = await getThreadSystemId(context.threadId);
      if (!systemId) {
        return reply.code(500).send({ error: "Unable to resolve thread system" });
      }

      const [nodesResult, edgesResult, concernsResult, matrixRefsResult, documentsResult, artifactsResult, messagesResult] =
        await Promise.all([
          query<TopologyNodeRow>(
            `SELECT
               id,
               name,
               kind,
               parent_id,
               (metadata->'layout'->>'x')::double precision AS layout_x,
               (metadata->'layout'->>'y')::double precision AS layout_y
             FROM nodes
             WHERE system_id = $1
             ORDER BY name, id`,
            [systemId],
          ),
          query<TopologyEdgeRow>(
            `SELECT id, type, from_node_id, to_node_id, metadata->>'protocol' AS protocol
             FROM edges
             WHERE system_id = $1
             ORDER BY id`,
            [systemId],
          ),
          query<ConcernRow>(
            `SELECT name, position
             FROM concerns
             WHERE system_id = $1
             ORDER BY position, name`,
            [systemId],
          ),
          query<MatrixRefRow>(
            `SELECT
               mr.node_id,
               mr.concern,
               mr.doc_hash,
               mr.ref_type,
               d.title AS doc_title,
               d.kind AS doc_kind,
               d.language AS doc_language
             FROM matrix_refs mr
             JOIN documents d ON d.system_id = mr.system_id AND d.hash = mr.doc_hash
             WHERE mr.system_id = $1
             ORDER BY mr.node_id, mr.concern, mr.ref_type, d.title`,
            [systemId],
          ),
          query<MatrixDocumentRow>(
            `SELECT hash, kind, title, language, text
             FROM documents
             WHERE system_id = $1
             ORDER BY kind, title, hash`,
            [systemId],
          ),
          query<ArtifactRow>(
            `SELECT id, node_id, concern, type, language, text
             FROM artifacts
             WHERE system_id = $1
             ORDER BY node_id, concern, created_at, id`,
            [systemId],
          ),
          query<ChatMessageRow>(
            `SELECT m.id, m.action_id, m.role, m.content, m.created_at
             FROM messages m
             JOIN actions a ON a.thread_id = m.thread_id AND a.id = m.action_id
             WHERE m.thread_id = $1
             ORDER BY a.position, m.position`,
            [context.threadId],
          ),
        ]);

      const nodes = nodesResult.rows.map((row) => ({
        id: row.id,
        name: row.name,
        kind: row.kind,
        parentId: row.parent_id,
        layoutX: row.layout_x,
        layoutY: row.layout_y,
      }));

      const concerns = concernsResult.rows.map((row) => ({
        name: row.name,
        position: row.position,
      }));

      const cellsByKey = new Map<string, MatrixCell>();
      for (const node of nodes) {
        for (const concern of concerns) {
          const key = matrixCellKey(node.id, concern.name);
          cellsByKey.set(key, {
            nodeId: node.id,
            concern: concern.name,
            docs: [],
            artifacts: [],
          });
        }
      }

      for (const row of matrixRefsResult.rows) {
        const key = matrixCellKey(row.node_id, row.concern);
        const existing = cellsByKey.get(key) ?? {
          nodeId: row.node_id,
          concern: row.concern,
          docs: [],
          artifacts: [],
        };
        existing.docs.push({
          hash: row.doc_hash,
          title: row.doc_title,
          kind: row.doc_kind,
          language: row.doc_language,
          refType: row.ref_type,
        });
        cellsByKey.set(key, existing);
      }

      for (const row of artifactsResult.rows) {
        const key = matrixCellKey(row.node_id, row.concern);
        const existing = cellsByKey.get(key) ?? {
          nodeId: row.node_id,
          concern: row.concern,
          docs: [],
          artifacts: [],
        };
        existing.artifacts.push({
          id: row.id,
          type: row.type,
          language: row.language,
          text: row.text,
        });
        cellsByKey.set(key, existing);
      }

      return {
        systemId,
        thread: buildThreadPayload(context),
        permissions: {
          canEdit: canEdit(context.accessRole),
          canChat: canEdit(context.accessRole),
        },
        topology: {
          nodes,
          edges: edgesResult.rows.map((row) => ({
            id: row.id,
            type: row.type,
            fromNodeId: row.from_node_id,
            toNodeId: row.to_node_id,
            protocol: row.protocol,
          })),
        },
        matrix: {
          concerns,
          nodes,
          cells: Array.from(cellsByKey.values()),
          documents: documentsResult.rows.map((row) => ({
            hash: row.hash,
            kind: row.kind,
            title: row.title,
            language: row.language,
            text: row.text,
          })),
        },
        chat: {
          messages: messagesResult.rows.map((row) => ({
            id: row.id,
            actionId: row.action_id,
            role: row.role,
            content: row.content,
            createdAt: row.created_at,
          })),
        },
      };
    },
  );

  app.patch<{ Params: { handle: string; projectName: string; threadId: string }; Body: ThreadPatchBody }>(
    "/projects/:handle/:projectName/thread/:threadId",
    async (req, reply) => {
      const { handle, projectName, threadId } = req.params;
      const context = await requireContext(reply, req.auth.id, handle, projectName, threadId);
      if (!context) return;

      if (!canEdit(context.accessRole)) {
        return reply.code(403).send({ error: "Forbidden" });
      }

      const { title, description } = req.body ?? {};
      if (typeof title === "undefined" && typeof description === "undefined") {
        return reply.code(400).send({ error: "No fields to update" });
      }

      const updates: string[] = [];
      const values: unknown[] = [];

      if (typeof title !== "undefined") {
        if (typeof title !== "string" || !title.trim()) {
          return reply.code(400).send({ error: "title cannot be blank" });
        }
        values.push(title.trim());
        updates.push(`title = $${values.length}`);
      }

      if (typeof description !== "undefined") {
        if (description !== null && typeof description !== "string") {
          return reply.code(400).send({ error: "description must be a string or null" });
        }
        const normalizedDescription = description === null ? null : (description.trim() || null);
        values.push(normalizedDescription);
        updates.push(`description = $${values.length}`);
      }

      values.push(context.threadId);

      const result = await query<UpsertThreadRow>(
        `UPDATE threads
         SET ${updates.join(", ")}
         WHERE id = $${values.length}
         RETURNING id, project_thread_id, title, description, status`,
        values,
      );

      if (result.rowCount === 0) {
        return reply.code(404).send({ error: "Thread not found" });
      }

      const updated = result.rows[0];
      return {
        thread: {
          id: updated.id,
          projectThreadId: updated.project_thread_id,
          title: updated.title,
          description: updated.description,
          status: updated.status,
          createdAt: context.createdAt,
          createdByHandle: context.createdByHandle,
          ownerHandle: context.ownerHandle,
          projectName: context.projectName,
          accessRole: context.accessRole,
        },
      };
    },
  );

  app.patch<{ Params: { handle: string; projectName: string; threadId: string }; Body: TopologyLayoutBody }>(
    "/projects/:handle/:projectName/thread/:threadId/topology/layout",
    async (req, reply) => {
      const { handle, projectName, threadId } = req.params;
      const context = await requireContext(reply, req.auth.id, handle, projectName, threadId);
      if (!context) return;

      if (!canEdit(context.accessRole)) {
        return reply.code(403).send({ error: "Forbidden" });
      }

      const payload = normalizeTopologyLayoutBody(req.body);
      if (!payload) {
        return reply.code(400).send({ error: "Invalid topology layout payload" });
      }

      const client = await pool.connect();
      let inTransaction = false;
      try {
        await client.query("BEGIN");
        inTransaction = true;

        const actionId = randomUUID();
        const beginResult = await client.query<BeginActionRow>(
          `SELECT begin_action($1, $2, $3::action_type, $4) AS output_system_id`,
          [context.threadId, actionId, "Edit", "Topology layout update"],
        );

        const outputSystemId = beginResult.rows[0]?.output_system_id;
        if (!outputSystemId) {
          throw new Error("Failed to create action output system");
        }

        const requestedNodeIds = payload.positions.map((position) => position.nodeId);
        const existingNodesResult = await client.query<{ id: string }>(
          `SELECT id
           FROM nodes
           WHERE system_id = $1
             AND id = ANY($2::text[])`,
          [outputSystemId, requestedNodeIds],
        );

        const existingNodeIds = new Set(existingNodesResult.rows.map((row) => row.id));
        const invalidNode = requestedNodeIds.find((nodeId) => !existingNodeIds.has(nodeId));
        if (invalidNode) {
          await client.query("ROLLBACK");
          inTransaction = false;
          return reply.code(400).send({ error: "Invalid node id in topology layout payload" });
        }

        let changedCount = 0;
        for (const position of payload.positions) {
          const updateResult = await client.query<ChangedRow>(
            `UPDATE nodes
             SET metadata = jsonb_set(
               coalesce(metadata, '{}'::jsonb),
               '{layout}',
               jsonb_build_object('x', $3, 'y', $4),
               true
             )
             WHERE system_id = $1
               AND id = $2
               AND (
                 (metadata->'layout'->>'x')::double precision IS DISTINCT FROM $3
                 OR (metadata->'layout'->>'y')::double precision IS DISTINCT FROM $4
               )
             RETURNING 1 AS changed`,
            [outputSystemId, position.nodeId, position.x, position.y],
          );
          changedCount += updateResult.rowCount ?? 0;
        }

        if (changedCount === 0) {
          await client.query("SELECT commit_action_empty($1, $2)", [context.threadId, actionId]);
        }

        await client.query("COMMIT");
        inTransaction = false;
      } catch (error) {
        if (inTransaction) {
          try {
            await client.query("ROLLBACK");
          } catch {
            // Best effort rollback.
          }
        }
        throw error;
      } finally {
        client.release();
      }

      const systemId = await getThreadSystemId(context.threadId);
      if (!systemId) {
        return reply.code(500).send({ error: "Unable to resolve thread system" });
      }

      return { systemId };
    },
  );

  app.post<{ Params: { handle: string; projectName: string; threadId: string }; Body: MatrixRefBody }>(
    "/projects/:handle/:projectName/thread/:threadId/matrix/refs",
    async (req, reply) => {
      const { handle, projectName, threadId } = req.params;
      const context = await requireContext(reply, req.auth.id, handle, projectName, threadId);
      if (!context) return;

      if (!canEdit(context.accessRole)) {
        return reply.code(403).send({ error: "Forbidden" });
      }

      const payload = normalizeMatrixMutationBody(req.body);
      if (!payload) {
        return reply.code(400).send({ error: "Invalid matrix reference payload" });
      }

      const client = await pool.connect();
      let inTransaction = false;

      try {
        await client.query("BEGIN");
        inTransaction = true;

        const actionId = randomUUID();
        const beginResult = await client.query<BeginActionRow>(
          `SELECT begin_action($1, $2, $3::action_type, $4) AS output_system_id`,
          [context.threadId, actionId, "Edit", "Matrix doc add"],
        );

        const outputSystemId = beginResult.rows[0]?.output_system_id;
        if (!outputSystemId) {
          throw new Error("Failed to create action output system");
        }

        const insertResult = await client.query<ChangedRow>(
          `INSERT INTO matrix_refs (system_id, node_id, concern, ref_type, doc_hash)
           VALUES ($1, $2, $3, $4::ref_type, $5)
           ON CONFLICT DO NOTHING
           RETURNING 1 AS changed`,
          [outputSystemId, payload.nodeId, payload.concern, payload.refType, payload.docHash],
        );

        if (insertResult.rowCount === 0) {
          await client.query("SELECT commit_action_empty($1, $2)", [context.threadId, actionId]);
        }

        await client.query("COMMIT");
        inTransaction = false;
      } catch (error: unknown) {
        if (inTransaction) {
          try {
            await client.query("ROLLBACK");
          } catch {
            // Best effort rollback.
          }
        }

        if (
          error instanceof Error &&
          "code" in error &&
          (error as { code?: string }).code === "23503"
        ) {
          return reply.code(400).send({ error: "Invalid node, concern, or document reference" });
        }

        throw error;
      } finally {
        client.release();
      }

      const systemId = await getThreadSystemId(context.threadId);
      if (!systemId) {
        return reply.code(500).send({ error: "Unable to resolve thread system" });
      }

      const cell = await getMatrixCell(systemId, payload.nodeId, payload.concern);
      return { systemId, cell };
    },
  );

  app.post<{ Params: { handle: string; projectName: string; threadId: string }; Body: MatrixDocumentCreateBody }>(
    "/projects/:handle/:projectName/thread/:threadId/matrix/documents",
    async (req, reply) => {
      const { handle, projectName, threadId } = req.params;
      const context = await requireContext(reply, req.auth.id, handle, projectName, threadId);
      if (!context) return;

      if (!canEdit(context.accessRole)) {
        return reply.code(403).send({ error: "Forbidden" });
      }

      const payload = normalizeMatrixDocumentCreateBody(req.body);
      if (!payload) {
        return reply.code(400).send({ error: "Invalid matrix document payload" });
      }

      const text = buildDocumentText({
        name: payload.name,
        description: payload.description,
        body: payload.body,
      });
      const hash = computeDocumentHash({
        kind: payload.kind,
        title: payload.title,
        language: payload.language,
        body: text,
      });

      const client = await pool.connect();
      let inTransaction = false;

      try {
        await client.query("BEGIN");
        inTransaction = true;

        const actionId = randomUUID();
        const beginResult = await client.query<BeginActionRow>(
          `SELECT begin_action($1, $2, $3::action_type, $4) AS output_system_id`,
          [context.threadId, actionId, "Edit", "Matrix doc create"],
        );
        const outputSystemId = beginResult.rows[0]?.output_system_id;
        if (!outputSystemId) {
          throw new Error("Failed to create action output system");
        }

        const insertResult = await client.query<MatrixDocumentRow>(
          `INSERT INTO documents (hash, system_id, kind, title, language, text)
           VALUES ($1, $2, $3::doc_kind, $4, $5, $6)
           ON CONFLICT (system_id, hash) DO NOTHING
           RETURNING hash, kind, title, language, text`,
          [hash, outputSystemId, payload.kind, payload.title, payload.language, text],
        );

        const shouldAttach = Boolean(payload.attach);
        if (payload.attach) {
          const insertRefResult = await client.query<ChangedRow>(
            `INSERT INTO matrix_refs (system_id, node_id, concern, ref_type, doc_hash)
             VALUES ($1, $2, $3, $4::ref_type, $5)
             ON CONFLICT DO NOTHING
             RETURNING 1 AS changed`,
            [outputSystemId, payload.attach.nodeId, payload.attach.concern, payload.attach.refType, hash],
          );

          if (insertResult.rowCount === 0 && insertRefResult.rowCount === 0) {
            await client.query("SELECT commit_action_empty($1, $2)", [context.threadId, actionId]);
          }

        } else if (insertResult.rowCount === 0) {
          await client.query("SELECT commit_action_empty($1, $2)", [context.threadId, actionId]);
        }

        await client.query("COMMIT");
        inTransaction = false;

        const systemId = await getThreadSystemId(context.threadId);
        if (!systemId) {
          return reply.code(500).send({ error: "Unable to resolve thread system" });
        }

        const nextDocument = insertResult.rows[0] ?? {
          hash,
          kind: payload.kind,
          title: payload.title,
          language: payload.language,
          text,
        };

        const response: { systemId: string; document: MatrixDocumentRow; cell?: MatrixCell } = {
          systemId,
          document: nextDocument,
        };
        if (shouldAttach && payload.attach) {
          response.cell = await getMatrixCell(systemId, payload.attach.nodeId, payload.attach.concern);
        }
        return response;
      } catch (error: unknown) {
        if (inTransaction) {
          try {
            await client.query("ROLLBACK");
          } catch {
            // Best effort rollback.
          }
        }
        if (
          error instanceof Error &&
          "code" in error &&
          (error as { code?: string }).code === "23503"
        ) {
          return reply.code(400).send({ error: "Invalid node, concern, or document reference" });
        }
        throw error;
      } finally {
        client.release();
      }
    },
  );

  app.patch<{
    Params: { handle: string; projectName: string; threadId: string; documentHash: string };
    Body: MatrixDocumentReplaceBody;
  }>(
    "/projects/:handle/:projectName/thread/:threadId/matrix/documents/:documentHash",
    async (req, reply) => {
      const { handle, projectName, threadId, documentHash } = req.params;
      const context = await requireContext(reply, req.auth.id, handle, projectName, threadId);
      if (!context) return;

      if (!canEdit(context.accessRole)) {
        return reply.code(403).send({ error: "Forbidden" });
      }

      const trimmedHash = documentHash.trim();
      const payload = normalizeMatrixDocumentReplaceBody(req.body);
      if (!trimmedHash || !payload) {
        return reply.code(400).send({ error: "Invalid matrix document patch payload" });
      }

      const client = await pool.connect();
      let inTransaction = false;

      try {
        await client.query("BEGIN");
        inTransaction = true;

        const actionId = randomUUID();
        const beginResult = await client.query<BeginActionRow>(
          `SELECT begin_action($1, $2, $3::action_type, $4) AS output_system_id`,
          [context.threadId, actionId, "Edit", "Matrix doc replace"],
        );
        const outputSystemId = beginResult.rows[0]?.output_system_id;
        if (!outputSystemId) {
          throw new Error("Failed to create action output system");
        }

        const existingResult = await client.query<MatrixDocumentRow>(
          `SELECT hash, kind, title, language, text
           FROM documents
           WHERE system_id = $1 AND hash = $2`,
          [outputSystemId, trimmedHash],
        );
        if (existingResult.rowCount === 0) {
          await client.query("ROLLBACK");
          inTransaction = false;
          return reply.code(404).send({ error: "Document not found" });
        }

        const existing = existingResult.rows[0];
        const parsedExisting = parseDocumentText(existing.text);
        const nextTitle = payload.title ?? existing.title;
        const nextLanguage = payload.language ?? existing.language;
        const existingName = parsedExisting.name;
        const sanitizedExistingName = existingName && isValidDocumentName(existingName) ? existingName : deriveDocumentName(nextTitle);
        const nextName = payload.name ?? sanitizedExistingName;
        const nextDescription = payload.description ?? parsedExisting.description;
        const nextBody = payload.body ?? parsedExisting.body;
        const nextText = buildDocumentText({
          name: nextName,
          description: nextDescription,
          body: nextBody,
        });
        const nextHash = computeDocumentHash({
          kind: existing.kind,
          title: nextTitle,
          language: nextLanguage,
          body: nextText,
        });

        const insertResult = await client.query<MatrixDocumentRow>(
          `INSERT INTO documents (hash, system_id, kind, title, language, text, supersedes)
           VALUES ($1, $2, $3::doc_kind, $4, $5, $6, $7)
           ON CONFLICT (system_id, hash) DO NOTHING
           RETURNING hash, kind, title, language, text`,
          [nextHash, outputSystemId, existing.kind, nextTitle, nextLanguage, nextText, existing.hash],
        );

        const updateRefsResult = await client.query<ChangedRow>(
          `UPDATE matrix_refs
           SET doc_hash = $3
           WHERE system_id = $1
             AND doc_hash = $2`,
          [outputSystemId, existing.hash, nextHash],
        );

        const changed = (insertResult.rowCount ?? 0) > 0 || (updateRefsResult.rowCount ?? 0) > 0;
        if (!changed) {
          await client.query("SELECT commit_action_empty($1, $2)", [context.threadId, actionId]);
        }

        await client.query("COMMIT");
        inTransaction = false;

        const nextDocument = insertResult.rows[0] ?? {
          hash: nextHash,
          kind: existing.kind,
          title: nextTitle,
          language: nextLanguage,
          text: nextText,
        };

        return {
          systemId: outputSystemId,
          oldHash: existing.hash,
          document: nextDocument,
          replacedRefs: updateRefsResult.rowCount ?? 0,
        };
      } catch (error) {
        if (inTransaction) {
          try {
            await client.query("ROLLBACK");
          } catch {
            // Best effort rollback;
          }
        }
        throw error;
      } finally {
        client.release();
      }
    },
  );

  app.delete<{ Params: { handle: string; projectName: string; threadId: string }; Body: MatrixRefBody }>(
    "/projects/:handle/:projectName/thread/:threadId/matrix/refs",
    async (req, reply) => {
      const { handle, projectName, threadId } = req.params;
      const context = await requireContext(reply, req.auth.id, handle, projectName, threadId);
      if (!context) return;

      if (!canEdit(context.accessRole)) {
        return reply.code(403).send({ error: "Forbidden" });
      }

      const payload = normalizeMatrixMutationBody(req.body);
      if (!payload) {
        return reply.code(400).send({ error: "Invalid matrix reference payload" });
      }

      const client = await pool.connect();
      let inTransaction = false;

      try {
        await client.query("BEGIN");
        inTransaction = true;

        const actionId = randomUUID();
        const beginResult = await client.query<BeginActionRow>(
          `SELECT begin_action($1, $2, $3::action_type, $4) AS output_system_id`,
          [context.threadId, actionId, "Edit", "Matrix doc remove"],
        );

        const outputSystemId = beginResult.rows[0]?.output_system_id;
        if (!outputSystemId) {
          throw new Error("Failed to create action output system");
        }

        const deleteResult = await client.query<ChangedRow>(
          `DELETE FROM matrix_refs
           WHERE system_id = $1
             AND node_id = $2
             AND concern_hash = md5($3)
             AND concern = $3
             AND ref_type = $4::ref_type
             AND doc_hash = $5
           RETURNING 1 AS changed`,
          [outputSystemId, payload.nodeId, payload.concern, payload.refType, payload.docHash],
        );

        if (deleteResult.rowCount === 0) {
          await client.query("SELECT commit_action_empty($1, $2)", [context.threadId, actionId]);
        }

        await client.query("COMMIT");
        inTransaction = false;
      } catch (error) {
        if (inTransaction) {
          try {
            await client.query("ROLLBACK");
          } catch {
            // Best effort rollback.
          }
        }
        throw error;
      } finally {
        client.release();
      }

      const systemId = await getThreadSystemId(context.threadId);
      if (!systemId) {
        return reply.code(500).send({ error: "Unable to resolve thread system" });
      }

      const cell = await getMatrixCell(systemId, payload.nodeId, payload.concern);
      return { systemId, cell };
    },
  );

  app.post<{ Params: { handle: string; projectName: string; threadId: string }; Body: ChatMessageBody }>(
    "/projects/:handle/:projectName/thread/:threadId/chat/messages",
    async (req, reply) => {
      const { handle, projectName, threadId } = req.params;
      const context = await requireContext(reply, req.auth.id, handle, projectName, threadId);
      if (!context) return;

      if (!canEdit(context.accessRole)) {
        return reply.code(403).send({ error: "Forbidden" });
      }

      if (typeof req.body?.content !== "string" || !req.body.content.trim()) {
        return reply.code(400).send({ error: "content is required" });
      }

      const content = req.body.content.trim();
      const actionId = randomUUID();
      const userMessageId = randomUUID();
      const assistantMessageId = randomUUID();

      const client = await pool.connect();
      let inTransaction = false;

      try {
        await client.query("BEGIN");
        inTransaction = true;

        await client.query(
          `SELECT begin_action($1, $2, $3::action_type, $4) AS output_system_id`,
          [context.threadId, actionId, "Chat", "Chat message"],
        );

        await client.query("SELECT commit_action_empty($1, $2)", [context.threadId, actionId]);

        await client.query(
          `INSERT INTO messages (id, thread_id, action_id, role, content, position)
           VALUES
             ($1, $2, $3, 'User'::message_role, $4, 1),
             ($5, $2, $3, 'Assistant'::message_role, $6, 2)`,
          [
            userMessageId,
            context.threadId,
            actionId,
            content,
            assistantMessageId,
            PLACEHOLDER_ASSISTANT_MESSAGE,
          ],
        );

        const messagesResult = await client.query<ChatMessageRow>(
          `SELECT id, action_id, role, content, created_at
           FROM messages
           WHERE thread_id = $1 AND action_id = $2
           ORDER BY position`,
          [context.threadId, actionId],
        );

        await client.query("COMMIT");
        inTransaction = false;

        return {
          messages: messagesResult.rows.map((row) => ({
            id: row.id,
            actionId: row.action_id,
            role: row.role,
            content: row.content,
            createdAt: row.created_at,
          })),
        };
      } catch (error) {
        if (inTransaction) {
          try {
            await client.query("ROLLBACK");
          } catch {
            // Best effort rollback.
          }
        }
        throw error;
      } finally {
        client.release();
      }
    },
  );
}
