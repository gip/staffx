import type { FastifyInstance } from "fastify";
import { verifyAuth } from "../auth.js";
import { query } from "../db.js";

interface UserRow {
  id: string;
  handle: string;
  name: string | null;
  picture: string | null;
  github_handle: string | null;
  created_at: Date;
}

interface SharedProjectRow {
  name: string;
  description: string | null;
  owner_handle: string;
  role: string;
  created_at: Date;
}

export async function userRoutes(app: FastifyInstance) {
  app.addHook("preHandler", verifyAuth);

  app.get<{ Querystring: { q: string } }>(
    "/users/search",
    async (req, reply) => {
      const { q } = req.query;
      if (!q || typeof q !== "string" || !q.trim()) {
        return reply.code(400).send({ error: "q is required" });
      }

      const pattern = `%${q.trim()}%`;
      const result = await query<{ handle: string; name: string | null; picture: string | null }>(
        `SELECT handle, name, picture FROM users
         WHERE handle ILIKE $1 OR name ILIKE $1
         LIMIT 10`,
        [pattern],
      );

      return result.rows;
    },
  );

  app.get<{ Params: { handle: string } }>(
    "/users/:handle",
    async (req, reply) => {
      const { handle } = req.params;

      const userResult = await query<UserRow>(
        `SELECT id, handle, name, picture, github_handle, created_at
         FROM users WHERE handle = $1`,
        [handle],
      );

      if (userResult.rowCount === 0) {
        return reply.code(404).send({ error: "User not found" });
      }

      const target = userResult.rows[0];

      const projectResult = await query<SharedProjectRow>(
        `SELECT
           p.name,
           p.description,
           owner_u.handle AS owner_handle,
           target_up.access_role AS role,
           p.created_at
         FROM user_projects target_up
         JOIN projects p ON p.id = target_up.id
         JOIN users owner_u ON owner_u.id = p.owner_id
         JOIN user_projects viewer_up ON viewer_up.id = p.id AND viewer_up.user_id = $2
         WHERE target_up.user_id = $1
         ORDER BY p.created_at DESC`,
        [target.id, req.auth.id],
      );

      return {
        handle: target.handle,
        name: target.name,
        picture: target.picture,
        githubHandle: target.github_handle,
        memberSince: target.created_at,
        projects: projectResult.rows.map((row) => ({
          name: row.name,
          description: row.description,
          ownerHandle: row.owner_handle,
          role: row.role,
          createdAt: row.created_at,
        })),
      };
    },
  );
}
