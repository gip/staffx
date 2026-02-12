import type { FastifyInstance } from "fastify";
import { createHash, randomUUID } from "node:crypto";
import type { PoolClient } from "pg";
import { verifyAuth } from "../auth.js";
import pool, { query } from "../db.js";
import {
  BLANK_TEMPLATE_ID,
  getTemplateById,
  isKnownTemplateId,
  type TemplateDefinition,
  type TemplateDocument,
} from "../templates/index.js";

interface ProjectRow {
  id: string;
  name: string;
  description: string | null;
  access_role: string;
  created_at: Date;
}

interface ThreadRow {
  id: string;
  title: string | null;
  description: string | null;
  project_thread_id: number | null;
  status: string;
  created_by: string | null;
  created_at: Date;
  updated_at: Date;
}

function computeDocumentHash(document: Pick<TemplateDocument, "kind" | "title" | "language" | "text">) {
  const payload = [document.kind, document.title, document.language, document.text].join("\n");
  return `sha256:${createHash("sha256").update(payload, "utf8").digest("hex")}`;
}

async function seedSystemTemplate(
  client: PoolClient,
  systemId: string,
  rootNodeId: string,
  template: TemplateDefinition,
) {
  const nodeIds = new Map<string, string>([["root", rootNodeId]]);
  for (const node of template.nodes) {
    if (nodeIds.has(node.key)) {
      throw new Error(`Template "${template.id}" has duplicate node key "${node.key}"`);
    }
    nodeIds.set(node.key, randomUUID());
  }

  for (const node of template.nodes) {
    const nodeId = nodeIds.get(node.key);
    const parentKey = node.parentKey ?? "root";
    const parentNodeId = nodeIds.get(parentKey);
    if (!nodeId || !parentNodeId) {
      throw new Error(
        `Template "${template.id}" references unknown parent "${parentKey}" for node "${node.key}"`,
      );
    }

    await client.query(
      `INSERT INTO nodes (id, system_id, kind, name, parent_id)
       VALUES ($1, $2, $3::node_kind, $4, $5)`,
      [nodeId, systemId, node.kind, node.name, parentNodeId],
    );
  }

  for (const edge of template.edges) {
    const fromNodeId = nodeIds.get(edge.fromKey);
    const toNodeId = nodeIds.get(edge.toKey);
    if (!fromNodeId || !toNodeId) {
      throw new Error(
        `Template "${template.id}" edge references unknown nodes "${edge.fromKey}" -> "${edge.toKey}"`,
      );
    }

    const metadata = edge.protocol ? JSON.stringify({ protocol: edge.protocol }) : "{}";
    await client.query(
      `INSERT INTO edges (id, system_id, type, from_node_id, to_node_id, metadata)
       VALUES ($1, $2, $3::edge_type, $4, $5, $6::jsonb)`,
      [randomUUID(), systemId, edge.type, fromNodeId, toNodeId, metadata],
    );
  }

  const concernNames = new Set<string>();
  for (const concern of template.concerns) {
    concernNames.add(concern.name);
    await client.query(
      `INSERT INTO concerns (system_id, name, position, is_baseline, scope)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (system_id, name) DO NOTHING`,
      [systemId, concern.name, concern.position, concern.isBaseline, concern.scope ?? null],
    );
  }

  const documentHashes = new Map<string, string>();
  for (const document of template.documents) {
    if (documentHashes.has(document.key)) {
      throw new Error(`Template "${template.id}" has duplicate document key "${document.key}"`);
    }
    const hash = computeDocumentHash(document);
    documentHashes.set(document.key, hash);

    await client.query(
      `INSERT INTO documents (hash, system_id, kind, title, language, text)
       VALUES ($1, $2, $3::doc_kind, $4, $5, $6)
       ON CONFLICT (system_id, hash) DO NOTHING`,
      [hash, systemId, document.kind, document.title, document.language, document.text],
    );
  }

  for (const ref of template.matrixRefs) {
    const nodeId = nodeIds.get(ref.nodeKey);
    const docHash = documentHashes.get(ref.documentKey);
    if (!nodeId) {
      throw new Error(`Template "${template.id}" matrix ref references unknown node "${ref.nodeKey}"`);
    }
    if (!docHash) {
      throw new Error(
        `Template "${template.id}" matrix ref references unknown document "${ref.documentKey}"`,
      );
    }
    if (!concernNames.has(ref.concern)) {
      throw new Error(`Template "${template.id}" matrix ref references unknown concern "${ref.concern}"`);
    }

    await client.query(
      `INSERT INTO matrix_refs (system_id, node_id, concern, ref_type, doc_hash)
       VALUES ($1, $2, $3, $4::ref_type, $5)
       ON CONFLICT DO NOTHING`,
      [systemId, nodeId, ref.concern, ref.refType, docHash],
    );
  }
}

export async function projectRoutes(app: FastifyInstance) {
  app.addHook("preHandler", verifyAuth);

  app.get<{ Querystring: { name: string } }>(
    "/projects/check-name",
    async (req, reply) => {
      const { name } = req.query;
      if (!name) return reply.code(400).send({ error: "name is required" });

      const result = await query(
        "SELECT 1 FROM projects WHERE owner_id = $1 AND name = $2",
        [req.auth.id, name],
      );
      return { available: result.rowCount === 0 };
    },
  );

  app.get("/projects", async (req) => {
    const result = await query<ProjectRow & { owner_handle: string; threads: ThreadRow[] }>(
      `SELECT
         p.id,
         p.name,
         p.description,
         p.access_role,
         p.created_at,
         u.handle AS owner_handle,
         COALESCE(
            (SELECT jsonb_agg(t)
            FROM (
              SELECT t.id, t.title, t.description, t.project_thread_id, t.status, t.updated_at
              FROM threads t
              WHERE t.project_id = p.id
              ORDER BY t.updated_at DESC
              LIMIT 2
            ) t),
           '[]'::jsonb
         ) AS threads
       FROM user_projects p
       JOIN users u ON u.id = p.owner_id
       WHERE p.user_id = $1
       ORDER BY p.created_at DESC`,
      [req.auth.id],
    );

    return result.rows.map((row) => ({
      id: row.id,
      name: row.name,
      description: row.description,
      accessRole: row.access_role,
      ownerHandle: row.owner_handle,
      createdAt: row.created_at,
      threads: (row.threads as unknown as ThreadRow[]).map((t) => ({
        id: t.id,
        title: t.title,
        description: t.description,
        projectThreadId: t.project_thread_id,
        status: t.status,
        updatedAt: t.updated_at,
      })),
    }));
  });

  app.get<{ Params: { handle: string; projectName: string } }>(
    "/projects/:handle/:projectName",
    async (req, reply) => {
      const { handle, projectName } = req.params;

      const result = await query<ProjectRow & { owner_handle: string; threads: ThreadRow[] }>(
        `SELECT
           p.id,
           p.name,
           p.description,
           p.access_role,
           p.created_at,
           u.handle AS owner_handle,
           COALESCE(
              (SELECT jsonb_agg(t ORDER BY t.project_thread_id DESC)
              FROM (
                SELECT t.id, t.title, t.description, t.project_thread_id, t.status,
                       t.created_by, t.created_at, t.updated_at
                FROM threads t
                WHERE t.project_id = p.id
              ) t),
             '[]'::jsonb
           ) AS threads
         FROM user_projects p
         JOIN users u ON u.id = p.owner_id
         WHERE u.handle = $1 AND p.name = $2 AND p.user_id = $3`,
        [handle, projectName, req.auth.id],
      );

      if (result.rowCount === 0) {
        return reply.code(404).send({ error: "Project not found" });
      }

      const row = result.rows[0];
      return {
        id: row.id,
        name: row.name,
        description: row.description,
        accessRole: row.access_role,
        ownerHandle: row.owner_handle,
        createdAt: row.created_at,
        threads: (row.threads as unknown as ThreadRow[]).map((t) => ({
          id: t.id,
          title: t.title,
          description: t.description,
          projectThreadId: t.project_thread_id,
          status: t.status,
          createdBy: t.created_by,
          createdAt: t.created_at,
          updatedAt: t.updated_at,
        })),
      };
    },
  );

  app.post<{ Body: { name: string; description?: string; template?: string } }>(
    "/projects",
    async (req, reply) => {
      const { name, description, template } = req.body;

      if (!name || typeof name !== "string" || !name.trim()) {
        return reply.code(400).send({ error: "name is required" });
      }

      const trimmed = name.trim();
      if (trimmed !== name) {
        return reply.code(400).send({ error: "name must not have leading or trailing spaces" });
      }
      if (!/^[a-zA-Z0-9]([a-zA-Z0-9_-]*[a-zA-Z0-9])?$/.test(trimmed) || /[-_]{2}/.test(trimmed)) {
        return reply.code(400).send({ error: "name must start/end with a letter or number, no consecutive - or _" });
      }

      const selectedTemplateId = template ?? BLANK_TEMPLATE_ID;
      if (!isKnownTemplateId(selectedTemplateId)) {
        return reply.code(400).send({ error: "Invalid template" });
      }
      const selectedTemplate = getTemplateById(selectedTemplateId);

      const id = randomUUID();
      const client = await pool.connect();
      let inTransaction = false;
      try {
        await client.query("BEGIN");
        inTransaction = true;

        const result = await client.query<ProjectRow>(
          `INSERT INTO projects (id, name, description, owner_id)
           VALUES ($1, $2, $3, $4)
           RETURNING id, name, description, created_at`,
          [id, trimmed, description?.trim() || null, req.auth.id],
        );

        const systemId = randomUUID();
        const rootNodeId = randomUUID();
        await client.query(
          `INSERT INTO systems (id, name, root_node_id)
           VALUES ($1, $2, $3)`,
          [systemId, trimmed, rootNodeId],
        );
        await client.query(
          `INSERT INTO nodes (id, system_id, kind, name, parent_id)
           VALUES ($1, $2, 'Root'::node_kind, $3, NULL)`,
          [rootNodeId, systemId, trimmed],
        );

        if (selectedTemplate) {
          await seedSystemTemplate(client, systemId, rootNodeId, selectedTemplate);
        }

        const threadId = randomUUID();
        await client.query(
          "SELECT create_thread($1, $2, $3, $4, $5, $6)",
          [threadId, id, req.auth.id, systemId, "Creating the project", null],
        );

        const threadResult = await client.query<ThreadRow>(
          `SELECT t.id, t.title, t.description, t.project_thread_id, t.status, t.updated_at
           FROM threads t
           WHERE t.id = $1`,
          [threadId],
        );
        const createdThreads = threadResult.rows;

        const row = result.rows[0];
        const handleResult = await client.query<{ handle: string }>(
          "SELECT handle FROM users WHERE id = $1",
          [req.auth.id],
        );

        await client.query("COMMIT");
        inTransaction = false;

        return reply.code(201).send({
          id: row.id,
          name: row.name,
          description: row.description,
          accessRole: "Owner",
          ownerHandle: handleResult.rows[0].handle,
          createdAt: row.created_at,
          threads: createdThreads.map((t) => ({
            id: t.id,
            title: t.title,
            description: t.description,
            projectThreadId: t.project_thread_id,
            status: t.status,
            updatedAt: t.updated_at,
          })),
        });
      } catch (err: unknown) {
        if (inTransaction) {
          try {
            await client.query("ROLLBACK");
          } catch {
            // Best effort rollback; preserve the original error.
          }
        }
        if (
          err instanceof Error &&
          "code" in err &&
          (err as { code: string }).code === "23505" &&
          "constraint" in err &&
          (err as { constraint?: string }).constraint === "idx_projects_owner_name"
        ) {
          return reply.code(409).send({ error: "A project with this name already exists" });
        }
        throw err;
      } finally {
        client.release();
      }
    },
  );
}
