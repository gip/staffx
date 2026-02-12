import type { FastifyInstance } from "fastify";
import { verifyAuth } from "../auth.js";

export async function meRoutes(app: FastifyInstance) {
  app.addHook("preHandler", verifyAuth);

  app.get("/me", async (req) => {
    return req.auth;
  });
}
