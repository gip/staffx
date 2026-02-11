import type { FastifyInstance } from "fastify";
import { query } from "../db.js";

export async function healthRoutes(app: FastifyInstance) {
  app.get("/health", async () => {
    const result = await query("SELECT NOW() AS time");
    return { status: "ok", time: result.rows[0].time };
  });
}
