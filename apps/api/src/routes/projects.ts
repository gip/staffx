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
  status: string;
  updated_at: Date;
}

export async function projectRoutes(app: FastifyInstance) {
  app.addHook("preHandler", verifyAuth);

  app.get("/projects", async (req) => {
    const result = await query<ProjectRow & { threads: ThreadRow[] }>(
      `SELECT
         p.id,
         p.name,
         p.description,
         p.access_role,
         p.created_at,
         COALESCE(
           (SELECT jsonb_agg(t)
            FROM (
              SELECT t.id, t.title, t.status, t.updated_at
              FROM threads t
              WHERE t.project_id = p.id
              ORDER BY t.updated_at DESC
              LIMIT 2
            ) t),
           '[]'::jsonb
         ) AS threads
       FROM user_projects p
       WHERE p.user_id = $1
       ORDER BY p.created_at DESC`,
      [req.auth.id],
    );

    return result.rows.map((row) => ({
      id: row.id,
      name: row.name,
      description: row.description,
      accessRole: row.access_role,
      createdAt: row.created_at,
      threads: (row.threads as unknown as ThreadRow[]).map((t) => ({
        id: t.id,
        title: t.title,
        status: t.status,
        updatedAt: t.updated_at,
      })),
    }));
  });

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
      } catch (err: any) {
        if (err.code === "23505") {
          return reply.code(409).send({ error: "A project with this name already exists" });
        }
        throw err;
      }

      const row = result.rows[0];
      return reply.code(201).send({
        id: row.id,
        name: row.name,
        description: row.description,
        accessRole: "Owner",
        createdAt: row.created_at,
        threads: [],
      });
    },
  );
}
