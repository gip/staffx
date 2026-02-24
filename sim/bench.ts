import { SimApi, SimActorContext, SimError, type StaffXEvent } from "./api";
import { SimFrontendClient } from "./frontend";
import { SimDesktop } from "./desktop";
import { spawn } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export interface BenchScenarioConfig {
  databaseUrl: string;
  usePgMem: boolean;
  projectPrefix: string;
  dryRun: boolean;
  listOnly: boolean;
  actorCount: number;
  desktopPollIntervalMs: number;
  eventPollMs: number;
  runWaitMs: number;
  cancelDelayMs: number;
}

interface SummaryRow {
  scenario: string;
  ok: boolean;
  details: Record<string, unknown>;
}

interface ArtifactFingerprint {
  artifactCount: number;
  systemId: string | null;
  signatures: string[];
}

function envString(key: string, fallback: string): string {
  return process.env[key]?.trim() || fallback;
}

function envInt(key: string, fallback: number, min = 1, max = 60_000): number {
  const raw = process.env[key];
  const parsed = Number.parseInt(raw ?? "", 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(Math.max(parsed, min), max);
}

function envBool(key: string, fallback = false): boolean {
  const raw = process.env[key]?.toLowerCase();
  if (!raw) return fallback;
  return ["1", "true", "yes", "y", "on"].includes(raw);
}

function escapeLikePattern(raw: string): string {
  return raw.replace(/([%_\\])/g, "\\$1");
}

function buildPrefixFilter(prefix: string, usePgMem: boolean): string {
  return usePgMem ? `${prefix}%` : `${escapeLikePattern(prefix)}%`;
}

function runCommand(cwd: string, args: string[]): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn("pnpm", args, {
      cwd,
      stdio: "inherit",
      shell: false,
    });

    child.on("error", reject);
    child.on("close", (code, signal) => {
      if (code === 0 && !signal) {
        resolve();
      } else if (code === 0 && signal) {
        resolve();
      } else {
        reject(new Error(`Command failed with exit code ${String(code)} signal ${String(signal)}`));
      }
    });
  });
}

async function migrateDatabase(): Promise<void> {
  if (envBool("SIM_SKIP_MIGRATION") || envBool("SIM_USE_PG_MEM")) {
    return;
  }

  const here = dirname(fileURLToPath(import.meta.url));
  const workspaceRoot = resolve(here, "..");

  console.log(`[bench] running migration in ${workspaceRoot}`);
  await runCommand(workspaceRoot, ["--filter", "@staffx/api", "migrate"]);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function assert(condition: boolean, message: string): void {
  if (!condition) {
    throw new Error(message);
  }
}

function findByType(events: StaffXEvent[], type: string): boolean {
  return events.some((entry) => entry.type === type);
}

function ensureMonotonic(events: StaffXEvent[]): boolean {
  for (let i = 1; i < events.length; i += 1) {
    const prev = Date.parse(events[i - 1].occurredAt);
    const next = Date.parse(events[i].occurredAt);
    if (Number.isNaN(prev) || Number.isNaN(next)) return false;
    if (next < prev) return false;
  }
  return true;
}

function normalizeHandle(prefix: string, suffix: string): string {
  return `${prefix}${suffix}`;
}

async function collectArtifactFingerprint(api: SimApi, runId: string): Promise<ArtifactFingerprint> {
  const summary = await api.runArtifactsForRun(runId);
  if (!summary.matrixSystemId) {
    return { artifactCount: 0, systemId: null, signatures: [] };
  }

  const artifacts = await api.getArtifactsForSystem(summary.matrixSystemId);
  return {
    artifactCount: artifacts.length,
    systemId: summary.matrixSystemId,
    signatures: artifacts
      .slice()
      .sort((a, b) => a.id.localeCompare(b.id))
      .map((artifact) => `${artifact.id}:${artifact.text ?? ""}`),
  };
}

async function createClients(api: SimApi, seed: string): Promise<{
  owner: SimActorContext;
  viewer: SimActorContext;
}> {
  const owner = await SimFrontendClient.bootstrapUser(api, normalizeHandle(seed, "-owner"), null);
  const viewer = await SimFrontendClient.bootstrapUser(api, normalizeHandle(seed, "-viewer"), null);
  return { owner, viewer };
}

async function listSeedProjects(
  api: SimApi,
  prefix: string,
  usePgMem: boolean,
): Promise<Array<{ id: string; name: string; createdAt: string }>> {
  const filter = buildPrefixFilter(prefix, usePgMem);
  const rows = await api.query<{ id: string; name: string; created_at: Date }>(
    usePgMem
      ? "SELECT id, name, created_at FROM projects WHERE name LIKE $1 ORDER BY created_at DESC"
      : "SELECT id, name, created_at FROM projects WHERE name LIKE $1 ESCAPE '\\' ORDER BY created_at DESC",
    [filter],
  );
  return rows.rows.map((row) => ({ id: row.id, name: row.name, createdAt: row.created_at.toISOString() }));
}

async function ensureCleanSeedSpace(api: SimApi, prefix: string, usePgMem: boolean): Promise<number> {
  const deleted = await api.cleanupSimProjects(prefix, buildPrefixFilter(prefix, usePgMem), usePgMem);
  return deleted.removed;
}

async function runPositiveFlow(api: SimApi, owner: SimActorContext, cfg: BenchScenarioConfig): Promise<SummaryRow> {
  const frontend = new SimFrontendClient(api, owner);
  const projectName = `${cfg.projectPrefix}positive`;
  const project = await frontend.ensureProject({
    name: projectName,
    description: "StaffX simulation positive path.",
    visibility: "private",
  });
  const thread = await api.createThread(owner, { projectId: project.id, title: "Positive thread" });
  const before = await api.getMatrix(owner, thread.id);
  const firstNodeId = before.topology.nodes[0]?.id;
  if (firstNodeId) {
    await frontend.mutateMatrixLayout(thread.id, [{ nodeId: firstNodeId, x: 10, y: 12 }]);
  }

  await frontend.sendChat(thread.id, "Run deterministic direct execution.");
  const { run, events } = await frontend.startRunAndWait(thread.id, {
    assistantType: "direct",
    prompt: "Run one deterministic deterministic action.",
    timeoutMs: cfg.runWaitMs,
    pollMs: cfg.eventPollMs,
  });

  const messages = await api.listMessages(owner, thread.id);
  const final = await api.getRun(owner, run.runId);
  const artifactFingerprint = await collectArtifactFingerprint(api, run.runId);

  assert(final.status === "success", "positive flow run must complete as success");
  assert(artifactFingerprint.artifactCount === 1, "positive flow must create exactly one artifact");
  assert(findByType(events, "assistant.run.completed"), "positive flow must emit assistant.run.completed");
  assert(findByType(events, "assistant.run.progress"), "positive flow must emit assistant.run.progress");
  assert(messages.some((entry) => entry.role === "Assistant"), "run should create assistant completion message");
  const threadEvents = await api.queryEvents({
    aggregateType: "thread",
    aggregateId: thread.id,
    limit: 20,
  });
  assert(
    findByType(threadEvents.items, "thread.matrix.changed"),
    "matrix patch should emit thread.matrix.changed",
  );

  return {
    scenario: "positive-flow",
    ok: true,
    details: {
      runId: run.runId,
      projectId: project.id,
      threadId: thread.id,
      status: final.status,
      events: events.map((event) => event.type),
      artifactCount: artifactFingerprint.artifactCount,
      artifactSignatures: artifactFingerprint.signatures,
      assistantMessages: messages.length,
    },
  };
}

async function runAccessControlFlow(api: SimApi, owner: SimActorContext, viewer: SimActorContext, cfg: BenchScenarioConfig): Promise<SummaryRow> {
  const ownerClient = new SimFrontendClient(api, owner);
  const viewerClient = new SimFrontendClient(api, viewer);

  const project = await ownerClient.ensureProject({
    name: `${cfg.projectPrefix}access`,
    description: "StaffX access control checks.",
    visibility: "private",
  });
  const thread = await api.createThread(owner, { projectId: project.id, title: "Access thread" });
  const before = await ownerClient.openThreadState(thread.id);

  const mutations = await Promise.allSettled([
    viewerClient.createThread(project.id, "Viewer denied thread"),
    viewerClient.sendChat(thread.id, "viewer chat"),
    viewerClient.startRun(thread.id, {
      assistantType: "direct",
      prompt: "viewer run",
    }).then(() => "ok"),
    viewerClient.mutateMatrixLayout(thread.id, before.matrix.topology.nodes.slice(0, 1).map((node) => ({ nodeId: node.id, x: 1, y: 1 })),
  )]);

  const denied = mutations.filter((entry) => entry.status === "rejected");
  assert(denied.length >= 3, "viewer should be denied all mutation operations");

  const deniedReasons = denied.map((entry) => {
    if (entry.status !== "rejected") return "non-rejection";
    const error = entry.reason;
    if (error && typeof error === "object" && "status" in (error as { status?: unknown })) {
      return String((error as { status?: unknown }).status);
    }
    return "error";
  });

  return {
    scenario: "access-control",
    ok: true,
    details: {
      deniedCount: denied.length,
      deniedReasons,
      viewerHandle: viewer.handle,
    },
  };
}

async function runQueueSemanticsFlow(api: SimApi, owner: SimActorContext, cfg: BenchScenarioConfig): Promise<SummaryRow> {
  const frontend = new SimFrontendClient(api, owner);
  const project = await frontend.ensureProject({
    name: `${cfg.projectPrefix}queue`,
    description: "StaffX queue semantics.",
    visibility: "private",
  });
  const thread = await api.createThread(owner, { projectId: project.id, title: "Queue thread" });
  await frontend.sendChat(thread.id, "Queue scenario chat message.");
  const run = await frontend.startRun(thread.id, {
    assistantType: "direct",
    prompt: "Queue check run.",
  });

  const first = await api.claimRun(owner, run.runId, "sim-queue-worker-a");
  let duplicateFailed = false;
  try {
    await api.claimRun(owner, run.runId, "sim-queue-worker-b");
  } catch (error) {
    if (isSimError(error) && error.status === 409) {
      duplicateFailed = true;
    } else {
      throw error;
    }
  }
  if (!duplicateFailed) {
    await api.cancelRun(owner, run.runId);
  }

  assert(first !== null, "first claim should succeed");
  assert(duplicateFailed, "second claim attempt should be rejected");
  assert(first.status === "running" || first.status === "queued", "claim result must be running/queued");

  await api.completeRun(owner, run.runId, {
    status: "success",
    messages: ["Queue semantics completed by test harness."],
    changes: [],
  });

  return {
    scenario: "queue-semantics",
    ok: true,
    details: {
      runId: run.runId,
      claimedBy: first.runnerId ?? null,
      duplicateFailed,
    },
  };
}

async function runEventOrderingFlow(api: SimApi, owner: SimActorContext, cfg: BenchScenarioConfig): Promise<SummaryRow> {
  const frontend = new SimFrontendClient(api, owner);
  const project = await frontend.ensureProject({
    name: `${cfg.projectPrefix}event-order`,
    description: "StaffX event ordering.",
    visibility: "private",
  });
  const thread = await api.createThread(owner, { projectId: project.id, title: "Event thread" });
  const matrix = await api.getMatrix(owner, thread.id);
  if (matrix.topology.nodes[0]) {
    await frontend.mutateMatrixLayout(thread.id, [{ nodeId: matrix.topology.nodes[0].id, x: 1, y: 2 }]);
  }
  await frontend.sendChat(thread.id, "Event ordering run.");
  const { run, events } = await frontend.startRunAndWait(thread.id, {
    assistantType: "direct",
    prompt: "Emit ordered events.",
    timeoutMs: cfg.runWaitMs,
    pollMs: cfg.eventPollMs,
  });

  const runEvents = await api.queryEvents({
    aggregateType: "assistant-run",
    aggregateId: run.runId,
  });

  const ordered = ensureMonotonic(runEvents.items);
  assert(ordered, "assistant-run events must be monotonic");
  assert(
    findByType(runEvents.items, "assistant.run.started") &&
      findByType(runEvents.items, "assistant.run.waiting_input") &&
      findByType(runEvents.items, "assistant.run.completed"),
    "run lifecycle events should include started, waiting_input and completed",
  );
  assert(
    runEvents.items.some((event) => event.payload && typeof event.payload === "object" && event.payload.threadId === thread.id),
    "run events should include threadId payload",
  );

  return {
    scenario: "event-ordering",
    ok: true,
    details: {
      runId: run.runId,
      eventCount: events.length,
      ordered,
      runEvents: runEvents.items.map((item) => item.type),
    },
  };
}

async function runDeterminismFlow(api: SimApi, owner: SimActorContext, cfg: BenchScenarioConfig): Promise<SummaryRow> {
  const frontend = new SimFrontendClient(api, owner);
  const seedName = `${cfg.projectPrefix}determinism`;
  const input = {
    project: {
      name: seedName,
      description: "StaffX determinism check.",
      visibility: "private" as const,
    },
    threadTitle: "Determinism thread",
    chatContent: "Please generate deterministic output.",
    run: {
      assistantType: "direct" as const,
      prompt: "Deterministic run prompt.",
    },
  };

  const first = await frontend.runFullWorkflow(input);
  const firstFingerprint = await collectArtifactFingerprint(api, first.run.runId);
  await ensureCleanSeedSpace(api, `${cfg.projectPrefix}determinism`, cfg.usePgMem);

  const second = await frontend.runFullWorkflow(input);
  const secondFingerprint = await collectArtifactFingerprint(api, second.run.runId);

  assert(firstFingerprint.signatures.length === 1, "first deterministic run must create exactly one artifact");
  assert(secondFingerprint.signatures.length === 1, "second deterministic run must create exactly one artifact");
  assert(
    JSON.stringify(firstFingerprint.signatures) === JSON.stringify(secondFingerprint.signatures),
    "deterministic fingerprints must match across repeated seeds",
  );

  return {
    scenario: "determinism",
    ok: true,
    details: {
      runOne: firstFingerprint,
      runTwo: secondFingerprint,
      runOneId: first.run.runId,
      runTwoId: second.run.runId,
    },
  };
}

async function runCancellationFlow(
  api: SimApi,
  owner: SimActorContext,
  cfg: BenchScenarioConfig,
  desktop: SimDesktop,
): Promise<SummaryRow> {
  await desktop.stop();

  const frontend = new SimFrontendClient(api, owner);
  const project = await frontend.ensureProject({
    name: `${cfg.projectPrefix}cancel`,
    description: "StaffX cancellation check.",
    visibility: "private",
  });
  const thread = await api.createThread(owner, { projectId: project.id, title: "Cancellation thread" });
  await frontend.sendChat(thread.id, "Cancellation check message.");
  const run = await frontend.startRun(thread.id, {
    assistantType: "direct",
    prompt: "Cancel this run.",
  });

  await sleep(cfg.cancelDelayMs);
  const cancelled = await api.cancelRun(owner, run.runId);

  const events = await api.queryEvents({
    aggregateType: "assistant-run",
    aggregateId: run.runId,
  });

  assert(cancelled.status === "cancelled", "run must transition to cancelled");
  assert(findByType(events.items, "assistant.run.cancelled"), "cancellation should emit assistant.run.cancelled");

  return {
    scenario: "cancellation",
    ok: true,
    details: {
      runId: run.runId,
      cancelledStatus: cancelled.status,
      runEvents: events.items.map((event) => event.type),
    },
  };
}

function isSimError(error: unknown): error is SimError {
  return error instanceof SimError;
}

function parseArgs() {
  const args = new Set(process.argv.slice(2));
  return {
    listOnly: args.has("--list"),
    dryRun: args.has("--dry-run"),
  };
}

async function runAllScenarios(api: SimApi, cfg: BenchScenarioConfig): Promise<void> {
  const { owner, viewer } = await createClients(api, cfg.projectPrefix.replace(/_$/, ""));
  const effectiveViewer = cfg.actorCount > 1 ? viewer : owner;
  const desktop = new SimDesktop(api, {
    actor: owner,
    pollIntervalMs: cfg.desktopPollIntervalMs,
  });

  const summaries: SummaryRow[] = [];

    const runWithIsolation = async <T>(fn: () => Promise<T>): Promise<T> => {
    await ensureCleanSeedSpace(api, cfg.projectPrefix, cfg.usePgMem);
    return fn();
  };

  try {
    summaries.push(await runWithIsolation(() => runQueueSemanticsFlow(api, owner, cfg)));
    desktop.start();
    await sleep(25);
    summaries.push(await runWithIsolation(() => runPositiveFlow(api, owner, cfg)));
    summaries.push(await runWithIsolation(() => runAccessControlFlow(api, owner, effectiveViewer, cfg)));
    summaries.push(await runWithIsolation(() => runEventOrderingFlow(api, owner, cfg)));
    summaries.push(await runWithIsolation(() => runDeterminismFlow(api, owner, cfg)));
    summaries.push(await runWithIsolation(() => runCancellationFlow(api, owner, cfg, desktop)));
  } catch (error) {
    desktop.stop().catch(() => void 0);
    if (error instanceof Error) {
      summaries.push({ scenario: "unexpected-error", ok: false, details: { message: error.message } });
    } else {
      summaries.push({ scenario: "unexpected-error", ok: false, details: { message: String(error) } });
    }
  } finally {
    await desktop.stop();
  }

  const successful = summaries.filter((entry) => entry.ok).length;
  const failed = summaries.length - successful;

  const summary = {
    scenarios: summaries,
    counters: { successful, failed, total: summaries.length },
    projectPrefix: cfg.projectPrefix,
    dryRun: cfg.dryRun,
  };

  if (failed > 0) {
    console.error("[bench] FAILED", JSON.stringify(summary, null, 2));
    process.exitCode = 1;
  } else {
    console.log("[bench] OK", JSON.stringify(summary, null, 2));
  }
}

async function main() {
  const args = parseArgs();

  const usePgMem = envBool("SIM_USE_PG_MEM");
  const cfg: BenchScenarioConfig = {
    databaseUrl: envString("DATABASE_URL", usePgMem ? "pgmem://local" : ""),
    usePgMem,
    projectPrefix: envString("SIM_PREFIX", "staffx_sim_"),
    dryRun: envBool("SIM_DRY_RUN", args.dryRun),
    listOnly: envBool("SIM_LIST_ONLY", args.listOnly),
    actorCount: envInt("SIM_ACTORS", 2, 1, 5),
    desktopPollIntervalMs: envInt("SIM_DESKTOP_POLL_MS", 300, 25, 5000),
    eventPollMs: envInt("SIM_EVENT_POLL_MS", 500, 50, 5000),
    runWaitMs: envInt("SIM_RUN_WAIT_MS", 20_000, 1_000, 180_000),
    cancelDelayMs: envInt("SIM_CANCEL_DELAY_MS", 50, 1, 1000),
  };

  if (!cfg.usePgMem && !cfg.databaseUrl) {
    throw new Error("DATABASE_URL is required");
  }

  const api = new SimApi({ databaseUrl: cfg.databaseUrl });

  if (cfg.listOnly) {
    const rows = await listSeedProjects(api, cfg.projectPrefix, cfg.usePgMem);
    console.log(`SIM project listing (prefix="${cfg.projectPrefix}")`);
    if (rows.length === 0) {
      console.log("  no matching projects");
    } else {
      for (const row of rows) {
        console.log(`  ${row.id} | ${row.name} | ${row.createdAt}`);
      }
    }
    await api.close();
    return;
  }

  if (cfg.dryRun) {
    const listing = await listSeedProjects(api, cfg.projectPrefix, cfg.usePgMem);
    console.log(`Dry run enabled. Existing projects for ${cfg.projectPrefix}: ${listing.length}`);
    await api.close();
    return;
  }

  await migrateDatabase();
  await runAllScenarios(api, cfg);
  await api.close();
}

await main();
