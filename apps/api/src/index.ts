import Fastify from "fastify";
import cors from "@fastify/cors";
import { healthRoutes } from "./routes/health.js";
import { meRoutes } from "./routes/me.js";
import { projectRoutes } from "./routes/projects.js";
import { threadRoutes } from "./routes/thread.js";
import { integrationsRoutes } from "./routes/integrations.js";
import { userRoutes } from "./routes/users.js";
import { close } from "./db.js";
import { startExternalDocumentSync } from "./documentSync.js";

const port = Number(process.env.PORT ?? 3001);
const app = Fastify({ logger: true });

await app.register(cors, { origin: true, methods: ["GET", "HEAD", "PUT", "PATCH", "POST", "DELETE"] });
await app.register(healthRoutes);
await app.register(meRoutes);
await app.register(projectRoutes);
await app.register(threadRoutes);
await app.register(integrationsRoutes);
await app.register(userRoutes);
let stopExternalDocumentSync: (() => Promise<void>) | null = null;

try {
  await app.listen({ port, host: "0.0.0.0" });
  stopExternalDocumentSync = startExternalDocumentSync({ logger: app.log });
} catch (err) {
  app.log.error(err);
  await close();
  process.exit(1);
}

const shutdown = async () => {
  if (stopExternalDocumentSync) {
    await stopExternalDocumentSync();
  }
  await app.close();
  await close();
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
