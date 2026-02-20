import { vi } from "vitest";

interface V1RunRow {
  id: string;
  thread_id: string;
  status: "queued" | "running" | "success" | "failed" | "cancelled";
  mode: "direct" | "plan";
  prompt: string;
  system_prompt: string | null;
  run_result_status: "success" | "failed" | null;
  run_result_messages: string[] | null;
  run_result_changes: unknown[] | null;
  run_error: string | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
}

let enqueueRunId = "run-queued-001";
const runById = new Map<string, V1RunRow>();
const claimedBy = new Map<string, string>();

const enqueueAgentRunWithWaitMock = vi.fn(async (): Promise<string> => enqueueRunId);

const getAgentRunByIdMock = vi.fn(async (runId: string): Promise<V1RunRow | null> => {
  return runById.get(runId) ?? null;
});

const claimAgentRunByIdMock = vi.fn(async (runId: string, runnerId: string): Promise<V1RunRow | null> => {
  const run = runById.get(runId);
  if (!run) return null;
  if (run.status !== "queued") return null;

  const claimed = {
    ...run,
    status: "running" as const,
    started_at: new Date().toISOString(),
  };
  runById.set(runId, claimed);
  claimedBy.set(runId, runnerId);
  return claimed;
});

const updateAgentRunResultMock = vi.fn(async (
  runId: string,
  status: "success" | "failed" | "cancelled" | "queued" | "running",
  result: { status: "success" | "failed"; messages: string[]; changes: unknown[]; error: string | undefined },
  _runnerError?: string,
  _runnerId?: string,
): Promise<boolean> => {
  const run = runById.get(runId);
  if (!run) return false;
  if (run.status === "success" || run.status === "failed" || run.status === "cancelled") return false;
  runById.set(runId, {
    ...run,
    status,
    run_result_status: result.status,
    run_result_messages: result.messages,
    run_result_changes: result.changes,
    run_error: result.error ?? null,
    completed_at: new Date().toISOString(),
  });
  return true;
});

export const agentQueueMock = {
  enqueueAgentRunWithWait: enqueueAgentRunWithWaitMock,
  getAgentRunById: getAgentRunByIdMock,
  claimAgentRunById: claimAgentRunByIdMock,
  updateAgentRunResult: updateAgentRunResultMock,
};

export function setAgentQueueRun(run: V1RunRow | null) {
  runById.clear();
  if (run) {
    runById.set(run.id, run);
  }
}

export function setAgentQueueRuns(runs: Array<V1RunRow>) {
  runById.clear();
  for (const run of runs) runById.set(run.id, run);
}

export function setEnqueueRunId(runId: string) {
  enqueueRunId = runId;
}

export function resetAgentQueueMocks() {
  runById.clear();
  claimedBy.clear();
  enqueueRunId = "run-queued-001";
  enqueueAgentRunWithWaitMock.mockClear();
  getAgentRunByIdMock.mockClear();
  claimAgentRunByIdMock.mockClear();
  updateAgentRunResultMock.mockClear();
}

export function getClaimedBy(runId: string) {
  return claimedBy.get(runId);
}

export function createAgentQueueMockModule() {
  return {
    enqueueAgentRunWithWait: agentQueueMock.enqueueAgentRunWithWait,
    getAgentRunById: agentQueueMock.getAgentRunById,
    claimAgentRunById: agentQueueMock.claimAgentRunById,
    updateAgentRunResult: agentQueueMock.updateAgentRunResult,
  };
}
