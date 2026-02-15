import Fastify from "fastify";
import cors from "@fastify/cors";
import { healthRoutes } from "./routes/health.js";
import { meRoutes } from "./routes/me.js";
import { projectRoutes } from "./routes/projects.js";
import { threadRoutes } from "./routes/thread.js";
import { integrationsRoutes } from "./routes/integrations.js";
import { userRoutes } from "./routes/users.js";
import { close } from "./db.js";
import { startAgentRunner } from "./agent-runner.js";

const port = Number(process.env.PORT ?? 3001);
const app = Fastify({ logger: true });
const isClaudeAgentEnabled = process.env.STAFFX_ENABLE_CLAUDE_AGENT === "1";
const apiPollMsRaw = Number(process.env.STAFFX_AGENT_RUNNER_POLL_MS ?? "1000");
const apiPollMs = Number.isFinite(apiPollMsRaw) && apiPollMsRaw > 0 ? apiPollMsRaw : 1000;

await app.register(cors, { origin: true, methods: ["GET", "HEAD", "PUT", "PATCH", "POST", "DELETE"] });
await app.register(healthRoutes);
await app.register(meRoutes);
await app.register(projectRoutes);
await app.register(threadRoutes);
await app.register(integrationsRoutes);
await app.register(userRoutes);

const stopAgentRunner = isClaudeAgentEnabled
  ? startAgentRunner({
      pollIntervalMs: apiPollMs,
      runnerId: process.env.STAFFX_AGENT_RUNNER_ID,
    })
  : () => {};

app.log.info({
  agentRunnerEnabled: isClaudeAgentEnabled,
});

try {
  await app.listen({ port, host: "0.0.0.0" });
} catch (err) {
  app.log.error(err);
  stopAgentRunner();
  await close();
  process.exit(1);
}

const shutdown = async () => {
  stopAgentRunner();
  await app.close();
  await close();
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
