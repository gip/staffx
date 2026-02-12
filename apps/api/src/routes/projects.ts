import type { FastifyInstance } from "fastify";
import { randomUUID } from "node:crypto";
import { verifyAuth } from "../auth.js";
import { query } from "../db.js";

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
      const { name, description } = req.body;

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

      const id = randomUUID();
      let result;
      try {
        result = await query<ProjectRow>(
          `INSERT INTO projects (id, name, description, owner_id)
           VALUES ($1, $2, $3, $4)
           RETURNING id, name, description, created_at`,
          [id, name.trim(), description?.trim() || null, req.auth.id],
        );
      } catch (err: unknown) {
        if (err instanceof Error && "code" in err && (err as { code: string }).code === "23505") {
          return reply.code(409).send({ error: "A project with this name already exists" });
        }
        throw err;
      }

      const row = result.rows[0];
      const handleResult = await query<{ handle: string }>(
        "SELECT handle FROM users WHERE id = $1",
        [req.auth.id],
      );
      return reply.code(201).send({
        id: row.id,
        name: row.name,
        description: row.description,
        accessRole: "Owner",
        ownerHandle: handleResult.rows[0].handle,
        createdAt: row.created_at,
        threads: [],
      });
    },
  );
}
