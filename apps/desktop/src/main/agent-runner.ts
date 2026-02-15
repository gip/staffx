import { Pool } from "pg";
import { resolveThreadWorkspacePath, runClaudeAgent } from "@staffx/agent-runtime";

const DEFAULT_POLL_INTERVAL_MS = 1000;
const DEFAULT_RUNNER_ID = `desktop-worker-${process.pid}`;

interface AgentRunRow {
  id: string;
  thread_id: string;
  project_id: string;
  prompt: string;
  system_prompt: string | null;
}

function getPollIntervalMs(): number {
  const raw = Number(process.env.STAFFX_AGENT_RUNNER_POLL_MS ?? "1000");
  return Number.isFinite(raw) && raw > 0 ? raw : DEFAULT_POLL_INTERVAL_MS;
}

function runSummaryError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function startAgentRunner(): () => Promise<void> {
  const dbUrl = process.env.DATABASE_URL ?? "postgresql://localhost:5432/staffx";
  const pollIntervalMs = getPollIntervalMs();
  const runnerId = process.env.STAFFX_AGENT_RUNNER_ID?.trim() || DEFAULT_RUNNER_ID;
  const pool = new Pool({ connectionString: dbUrl });

  let stopped = false;
  let processing = false;

  const claimNextAgentRun = async (): Promise<AgentRunRow | null> => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query<AgentRunRow>(
      `WITH next_run AS (
           SELECT ar.id
           FROM agent_runs ar
           JOIN threads t ON t.id = ar.thread_id
           WHERE ar.status = 'queued'
             AND NOT EXISTS (
               SELECT 1
               FROM agent_runs ar2
               WHERE ar2.thread_id = ar.thread_id
                 AND ar2.status = 'running'
             )
           ORDER BY ar.created_at
           FOR UPDATE OF ar, t
           SKIP LOCKED
           LIMIT 1
         )
         UPDATE agent_runs ar
         SET status = 'running',
             runner_id = $1,
             started_at = NOW(),
             updated_at = NOW(),
             run_error = NULL
         FROM next_run
         WHERE ar.id = next_run.id
         RETURNING
           ar.id,
           ar.thread_id,
           ar.project_id,
           ar.prompt,
           ar.system_prompt`,
        [runnerId],
      );
      await client.query("COMMIT");
      return result.rows[0] ?? null;
    } catch (error: unknown) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  };

  const updateResult = async (
    runId: string,
    status: "success" | "failed",
    result: { status: "success" | "failed"; messages: string[]; changes: unknown[]; error?: string },
    runnerError?: string,
  ) => {
    await pool.query(
      `UPDATE agent_runs
         SET status = $1,
             run_result_status = $2,
             run_result_messages = $3,
             run_result_changes = $4,
             run_error = COALESCE($5, run_error),
             completed_at = NOW(),
             updated_at = NOW()
       WHERE id = $6`,
      [
        status,
        result.status,
        result.messages,
        JSON.stringify(result.changes),
        runnerError ?? result.error ?? null,
        runId,
      ],
    );
  };

  const processNext = async () => {
    if (stopped || processing) return;
    processing = true;
    let run: AgentRunRow | null = null;

    try {
      run = await claimNextAgentRun();
      if (!run) return;

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

      await updateResult(run.id, result.status, {
        status: result.status,
        messages: result.messages,
        changes: result.changes,
      });

      console.info("[desktop-agent-runner] completed", {
        runId: run.id,
        threadId: run.thread_id,
        status: result.status,
      });
    } catch (error: unknown) {
      if (run) {
        const message = runSummaryError(error);
        await updateResult(
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
      const message = runSummaryError(error);
      console.error("[desktop-agent-runner] process error", { error: message });
    } finally {
      processing = false;
    }
  };

  const interval = setInterval(() => {
    void processNext().catch((error: unknown) => {
      console.error("[desktop-agent-runner] poll error", { error: runSummaryError(error) });
    });
  }, pollIntervalMs);

  void processNext();

  return async () => {
    stopped = true;
    clearInterval(interval);
    await pool.end();
  };
}
