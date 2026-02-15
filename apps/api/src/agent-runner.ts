import { claimNextAgentRun, updateAgentRunResult } from "./agent-queue.js";
import { resolveThreadWorkspacePath, runClaudeAgent } from "@staffx/agent-runtime";

const DEFAULT_POLL_INTERVAL_MS = 1000;
const DEFAULT_RUNNER_ID = process.env.STAFFX_AGENT_RUNNER_ID || "api-worker";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

interface AgentRunnerOptions {
  pollIntervalMs?: number;
  runnerId?: string;
}

function runSummaryError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function startAgentRunner(options: AgentRunnerOptions = {}): () => void {
  const pollIntervalMs = options.pollIntervalMs && options.pollIntervalMs > 0
    ? options.pollIntervalMs
    : DEFAULT_POLL_INTERVAL_MS;
  const runnerId = options.runnerId?.trim() || DEFAULT_RUNNER_ID;

  let stopped = false;

  const processOnce = async () => {
    if (stopped) return;

    const run = await claimNextAgentRun(runnerId);
    if (!run) return;

    try {
      const workspace = resolveThreadWorkspacePath({
        projectId: run.project_id,
        threadId: run.thread_id,
        baseDir: process.env.STAFFX_PROJECTS_ROOT,
      });

      const result = await runClaudeAgent({
        prompt: run.prompt,
        cwd: workspace,
        systemPrompt: run.system_prompt ?? undefined,
        allowedTools: ["Read", "Grep", "Glob", "Bash", "Edit", "Write"],
      });

      await updateAgentRunResult(run.id, result.status === "failed" ? "failed" : "success", result);
      console.info("[agent-runner] completed", {
        runId: run.id,
        threadId: run.thread_id,
        status: result.status,
      });
    } catch (error: unknown) {
      const message = runSummaryError(error);
      console.error("[agent-runner] failed", { runId: run.id, threadId: run.thread_id, error: message });
      await updateAgentRunResult(
        run.id,
        "failed",
        {
          status: "failed",
          messages: [`Execution failed: ${message}`],
          changes: [],
          error: message,
        },
        message,
      );
    }
  };

  const runLoop = async () => {
    while (!stopped) {
      try {
        await processOnce();
      } catch (error: unknown) {
        console.error("[agent-runner] poller failed", { error: runSummaryError(error) });
      }
      if (stopped) break;
      await sleep(pollIntervalMs);
    }
    console.info("[agent-runner] stopped", { runnerId });
  };

  void runLoop();

  const stop = (): void => {
    stopped = true;
  };

  return stop;
}
