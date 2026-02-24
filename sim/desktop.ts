import {
  AssistantRun,
  SimApi,
  SimActorContext,
  SimError,
  stableId,
} from "./api";

export interface SimDesktopOptions {
  actor: SimActorContext;
  runnerId?: string;
  pollIntervalMs?: number;
  runDelayMs?: number;
}

export interface DesktopExecutionResult {
  runId: string;
  threadId: string;
  artifactId: string;
  outputSystemId: string;
  artifactText: string;
  concern: string;
  nodeId: string;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isSimErrorLike(error: unknown): error is SimError {
  return error instanceof SimError;
}

export class SimDesktop {
  private readonly runnerId: string;
  private readonly pollIntervalMs: number;
  private readonly runDelayMs: number;
  private running = false;
  private loopActive = false;
  private loopPromise: Promise<void> | null = null;
  private processedRuns = 0;

  constructor(
    private readonly api: SimApi,
    private readonly options: SimDesktopOptions,
  ) {
    this.pollIntervalMs = Math.max(25, options.pollIntervalMs ?? 300);
    this.runDelayMs = Math.max(0, options.runDelayMs ?? 0);
    this.runnerId = options.runnerId ?? `sim-runner-${options.actor.handle}`;
  }

  start(): void {
    if (this.loopActive) return;
    this.loopActive = true;
    this.running = true;
    this.loopPromise = this.runLoop();
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.loopPromise) {
      await this.loopPromise;
    }
  }

  async tickOnce(): Promise<boolean> {
    const claimed = await this.api.claimQueuedRun(this.runnerId);
    if (!claimed) {
      return false;
    }

    await this.api.publishEvent({
      type: "assistant.run.progress",
      aggregateType: "assistant-run",
      aggregateId: claimed.runId,
      orgId: this.options.actor.orgId,
      traceId: claimed.threadId,
      payload: {
        status: "running",
        stage: "begin",
        runnerId: this.runnerId,
        threadId: claimed.threadId,
      },
    });

    try {
      await this.executeRun(claimed);
      this.processedRuns += 1;
      return true;
    } catch (error) {
      if (this.isCancellationOrFinalizedError(error)) {
        return true;
      }
      await this.markRunFailed(claimed, error);
      this.processedRuns += 1;
      return true;
    }
  }

  async stopAfterRuns(count: number): Promise<void> {
    if (count <= 0) return;
    while (this.running) {
      if (this.processedRuns >= count) {
        await this.stop();
        return;
      }
      await sleep(25);
    }
  }

  private async runLoop(): Promise<void> {
    try {
      while (this.running) {
        const didWork = await this.tickOnce();
        if (!didWork) {
          await sleep(this.pollIntervalMs);
        } else if (this.runDelayMs > 0) {
          await sleep(this.runDelayMs);
        }
      }
    } finally {
      this.loopActive = false;
    }
  }

  private async executeRun(run: AssistantRun): Promise<DesktopExecutionResult> {
    const threadId = run.threadId;
    const promptSeed = `${run.runId}|${run.prompt}|${run.model}|${run.mode}`;
    const actionId = stableId("desktop-exec", promptSeed);
    const artifactId = stableId("desktop-artifact", `${promptSeed}|artifact`);
    const changeId = stableId("desktop-change", `${promptSeed}|change`);

    const execution = await this.api.withTx(async (client) => {
      const statusCheck = await client.query<{ status: string }>(
        "SELECT status FROM agent_runs WHERE id = $1",
        [run.runId],
      );
      if (!statusCheck.rows[0] || statusCheck.rows[0]?.status !== "running") {
        throw new SimError(409, "Run unavailable");
      }

      const statusRow = await client.query<{ status: string }>(
        "SELECT status FROM threads WHERE id = $1",
        [threadId],
      );
      if (!statusRow.rows[0] || statusRow.rows[0]!.status !== "open") {
        throw new SimError(409, `Thread ${threadId} is not open`);
      }

      const actionPositionResult = await client.query<{ position: number }>(
        `SELECT COALESCE(MAX(position), 0) + 1 AS position FROM actions WHERE thread_id = $1`,
        [threadId],
      );
      const actionPosition = actionPositionResult.rows[0]?.position ?? 1;

      let outputSystemId: string | null = run.threadId;

      if (this.api.isUsingPgMem) {
        const latestOutputSystem = await client.query<{ output_system_id: string }>(
          `SELECT output_system_id
           FROM actions
           WHERE thread_id = $1
             AND output_system_id IS NOT NULL
           ORDER BY position DESC
           LIMIT 1`,
          [threadId],
        );
        if (latestOutputSystem.rows[0]?.output_system_id) {
          outputSystemId = latestOutputSystem.rows[0]!.output_system_id;
        } else {
          const sourceSystem = await client.query<{ seed_system_id: string }>(
            `SELECT seed_system_id FROM threads WHERE id = $1`,
            [threadId],
          );
          outputSystemId = sourceSystem.rows[0]?.seed_system_id ?? null;
        }
        if (!outputSystemId || !/^[0-9a-f]{8}-/.test(outputSystemId)) {
          throw new SimError(500, "Thread has no current system");
        }

        await client.query(
          `INSERT INTO actions (id, thread_id, position, type, title, output_system_id)
           VALUES ($1, $2, $3, 'Execute'::action_type, $4, $5)`,
          [actionId, threadId, actionPosition, `Deterministic execution for run ${run.runId}`, outputSystemId],
        );
      } else {
        const forkResult = await client.query<{ output_system_id: string }>(
          `SELECT begin_action($1, $2, 'Execute'::action_type, $3) AS output_system_id`,
          [threadId, actionId, `Deterministic execution for run ${run.runId}`],
        );
        outputSystemId = forkResult.rows[0]?.output_system_id;
      }

      if (!outputSystemId) {
        throw new SimError(500, "Failed to fork thread system");
      }

      const nodeResult = await client.query<{ id: string }>(
        `SELECT id FROM nodes WHERE system_id = $1 ORDER BY parent_id NULLS LAST, id LIMIT 1`,
        [outputSystemId],
      );
      const nodeId = nodeResult.rows[0]?.id ?? null;
      if (!nodeId) {
        throw new SimError(500, "No node available for artifact write");
      }

      const concernResult = await client.query<{ name: string }>(
        `SELECT name FROM concerns WHERE system_id = $1 ORDER BY position LIMIT 1`,
        [outputSystemId],
      );
      const concern = concernResult.rows[0]?.name ?? "General";

      const artifactText = `Deterministic artifact for run ${run.runId}`;

      await client.query(
        `INSERT INTO artifacts (id, system_id, node_id, concern, type, language, text)
         VALUES ($1, $2, $3, $4, 'Summary'::artifact_type, 'en', $5)`,
        [artifactId, outputSystemId, nodeId, concern, artifactText],
      );

      await client.query(
        `INSERT INTO changes (id, thread_id, action_id, target_table, operation, target_id, previous, current)
         VALUES ($1, $2, $3, 'artifacts', 'Create', $4, $5, $6)`,
        [
          changeId,
          threadId,
          actionId,
          JSON.stringify({ id: artifactId, concern, systemId: outputSystemId, nodeId, type: "Summary", language: "en" }),
          null,
          JSON.stringify({
            id: artifactId,
            concern,
            nodeId,
            type: "Summary",
            language: "en",
            systemId: outputSystemId,
            text: artifactText,
          }),
        ],
      );

      if (this.runDelayMs > 0) {
        await sleep(this.runDelayMs);
      }

      return {
        runId: run.runId,
        threadId,
        artifactId,
        outputSystemId,
        artifactText,
        concern,
        nodeId,
      };
    });

    const runMessage = `Run completed deterministically for run ${run.runId}`;
    const runChanges = [
      {
        target_table: "artifacts",
        operation: "Create" as const,
        target_id: {
          id: execution.artifactId,
          systemId: execution.outputSystemId,
          nodeId: execution.nodeId,
        },
        previous: null,
        current: {
          id: execution.artifactId,
          systemId: execution.outputSystemId,
          nodeId: execution.nodeId,
          concern: execution.concern,
          type: "Summary",
          language: "en",
          text: execution.artifactText,
        },
      },
    ];

    const currentRun = await this.api.getRun(this.options.actor, run.runId);
    if (currentRun.status !== "running") {
      return execution;
    }

    await this.api.completeRun(this.options.actor, run.runId, {
      status: "success",
      messages: [runMessage],
      changes: runChanges,
    });
    return execution;
  }

  private isCancellationOrFinalizedError(error: unknown): boolean {
    if (isSimErrorLike(error)) {
      return error.status === 403 || error.status === 404 || error.status === 409;
    }
    return false;
  }

  private async markRunFailed(run: AssistantRun, error: unknown): Promise<void> {
    const reason =
      error instanceof Error
        ? error.message
        : typeof error === "string"
          ? error
          : "Simulated desktop execution failed.";

    try {
      const current = await this.api.getRun(this.options.actor, run.runId);
      if (current.status !== "running") {
        return;
      }
      await this.api.completeRun(this.options.actor, run.runId, {
        status: "failed",
        messages: [`Execution failed: ${reason}`],
        changes: [],
        error: reason,
      });
    } catch {
      // ignore: runner has already been finalized elsewhere.
    }
  }
}
