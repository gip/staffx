import Fastify from "fastify";
import cors from "@fastify/cors";
import { healthRoutes } from "./routes/health.js";
import { close } from "./db.js";

const port = Number(process.env.PORT ?? 3001);
const app = Fastify({ logger: true });

await app.register(cors, { origin: true });
await app.register(healthRoutes);

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
