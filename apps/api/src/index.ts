import Fastify from "fastify";
import cors from "@fastify/cors";
import { healthRoutes } from "./routes/health.js";
import { meRoutes } from "./routes/me.js";
import { projectRoutes } from "./routes/projects.js";
import { threadRoutes } from "./routes/thread.js";
import { close } from "./db.js";

const port = Number(process.env.PORT ?? 3001);
const app = Fastify({ logger: true });

await app.register(cors, { origin: true });
await app.register(healthRoutes);
await app.register(meRoutes);
await app.register(projectRoutes);
await app.register(threadRoutes);

try {
  await app.listen({ port, host: "0.0.0.0" });
} catch (err) {
  app.log.error(err);
  await close();
  process.exit(1);
}

const shutdown = async () => {
  await app.close();
  await close();
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
