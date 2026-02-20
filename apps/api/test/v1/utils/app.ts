import Fastify from "fastify";
import { healthRoutes } from "../../../src/routes/health.js";
import { meRoutes } from "../../../src/routes/me.js";
import { userRoutes } from "../../../src/routes/users.js";
import { integrationsRoutes } from "../../../src/routes/integrations.js";
import { v1Routes } from "../../../src/routes/v1.js";

export async function buildV1TestApp() {
  const app = Fastify({ logger: false });

  await app.register(async (sub) => {
    await sub.register(healthRoutes);
    await sub.register(meRoutes);
    await sub.register(userRoutes);
    await sub.register(integrationsRoutes);
    await sub.register(v1Routes);
  }, { prefix: "/v1" });

  return app;
}
