import { mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { randomUUID } from "node:crypto";
import {
  diffOpenShipSnapshots,
  runClaudeAgent,
  snapshotOpenShipBundle,
  type SDKMessage,
} from "@staffx/agent-runtime";
import { getAccessToken } from "./auth.js";

const API_URL = import.meta.env.VITE_API_URL ?? "http://localhost:3001";
const DEFAULT_RUNNER_ID = process.env.STAFFX_AGENT_RUNNER_ID ?? "desktop-runner";
const AGENTS_BOOTSTRAP_FILE_NAME = "AGENTS.md";
const OPENSHIP_SPEC_WORKSPACE_FILE_PATH = "skills/openship-specs-v1/SKILL.md";

interface AssistantRunClaimResponse {
  runId: string;
  status: "queued" | "running" | "success" | "failed";
  systemId: string;
  prompt?: string;
  systemPrompt?: string | null;
  model?: string | null;
}

interface OpenShipBundleFile {
  path: string;
  content: string;
}

interface OpenShipBundleDescriptor {
  threadId: string;
  systemId: string;
  generatedAt: string;
  files: OpenShipBundleFile[];
}

interface AssistantRunCompleteRequest {
  status: "success" | "failed";
  messages: string[];
  changes: Array<{
    target_table: string;
    operation: "Create" | "Update" | "Delete";
    target_id: Record<string, unknown>;
    previous: Record<string, unknown> | null;
    current: Record<string, unknown> | null;
  }>;
  error?: string;
  runnerId: string;
  openShipBundleFiles?: OpenShipBundleFile[];
}

interface AssistantRunResultResponse {
  runId?: string;
  status?: string;
  filesChanged?: Array<{
    kind: "Create" | "Update" | "Delete";
    path: string;
    fromHash?: string;
    toHash?: string;
  }>;
  summary?: { status: "success" | "failed"; messages: string[] };
  messages?: Array<{
    id: string;
    actionId: string;
    role: "User" | "Assistant" | "System";
    actionType: string;
    actionPosition: number;
    content: string;
    createdAt: string;
  }>;
  threadState?: unknown;
}

async function collectOpenShipBundleFiles(bundleDir: string): Promise<OpenShipBundleFile[]> {
  const entries = await readdir(bundleDir, { withFileTypes: true });
  const files: OpenShipBundleFile[] = [];

  for (const entry of entries.sort((left, right) => left.name.localeCompare(right.name))) {
    const childPath = join(bundleDir, entry.name);

    if (entry.isDirectory()) {
      const nested = await collectOpenShipBundleFiles(childPath);
      for (const nestedFile of nested) {
        const nextPath = `${entry.name}/${nestedFile.path}`.replace(/\\+/g, "/");
        files.push({ path: nextPath, content: nestedFile.content });
      }
      continue;
    }

    if (!entry.isFile()) continue;

    const content = await readFile(childPath, "utf8");
    files.push({
      path: entry.name,
      content,
    });
  }

  return files;
}

function sanitizeComponent(value: string): string {
  return value.replace(/[^a-zA-Z0-9._-]/g, "_");
}

function resolveWorkspace(handle: string, projectName: string, threadId: string): string {
  const projectsRoot = process.env.STAFFX_PROJECTS_ROOT
    ? process.env.STAFFX_PROJECTS_ROOT
    : join(homedir(), ".staffx", "projects");

  return join(projectsRoot, "desktop", sanitizeComponent(handle), sanitizeComponent(projectName), sanitizeComponent(threadId));
}

function extractReadableText(value: unknown, depth = 0): string[] {
  if (depth > 8 || value === null || value === undefined) return [];

  if (typeof value === "string") {
    return [value];
  }

  if (Array.isArray(value)) {
    return value.flatMap((item) => extractReadableText(item, depth + 1));
  }

  if (typeof value === "object") {
    const typed = value as Record<string, unknown>;

    if (typeof typed.text === "string") {
      return [typed.text];
    }

    if (typeof typed.content === "string") {
      return [typed.content];
    }

    if (typeof typed.message === "string") {
      return [typed.message];
    }

    if (typed.response !== undefined) {
      return extractReadableText(typed.response, depth + 1);
    }

    if (typed.message !== undefined) {
      return extractReadableText(typed.message, depth + 1);
    }

    if (typed.content !== undefined) {
      return extractReadableText(typed.content, depth + 1);
    }

    if (typed.text !== undefined) {
      return extractReadableText(typed.text, depth + 1);
    }

    return [];
  }

  return [];
}

function summarizeSdkMessage(message: SDKMessage): { text: string; isAnomaly: boolean } {
  try {
    const typed = message as {
      content?: unknown;
      message?: unknown;
      response?: unknown;
      text?: unknown;
    };
    const text = [
      ...extractReadableText(typed.content),
      ...extractReadableText(typed.text),
      ...extractReadableText(typed.message),
      ...extractReadableText(typed.response),
    ]
      .flatMap((line) => line.split("\n"))
      .map((line) => line.trim())
      .filter((line) => line.length > 0)
      .join(" ");

    if (text.length > 0) {
      return { text, isAnomaly: false };
    }

    const payload = JSON.stringify(message);
    return { text: payload ?? "[unserializable-sdk-message]", isAnomaly: true };
  } catch (error: unknown) {
    const reason = error instanceof Error ? error.message : "unknown serialization error";
    return { text: reason, isAnomaly: true };
  }
}

function truncateForLog(value: string, maxLength: number): string {
  if (value.length <= maxLength) return value;
  return `${value.slice(0, maxLength - 1)}…`;
}

function summarizeFailureReason(messages: string[] | undefined): string | null {
  if (!messages || messages.length === 0) return null;
  const joined = messages
    .map((message) => message.trim())
    .filter((message) => message.length > 0)
    .join(" | ");
  return joined.length > 0 ? truncateForLog(joined, 1200) : null;
}

function logRunOutcome(payload: {
  runId: string;
  threadId: string;
  stage: string;
  status: "success" | "failed";
  reason?: string | null;
}): void {
  const reason = payload.reason?.trim() || null;
  if (payload.status === "failed") {
    console.warn("[desktop-agent] run outcome", {
      runId: payload.runId,
      threadId: payload.threadId,
      stage: payload.stage,
      status: payload.status,
      reason: reason ?? "Unknown failure.",
    });
    return;
  }

  console.info("[desktop-agent] run outcome", {
    runId: payload.runId,
    threadId: payload.threadId,
    stage: payload.stage,
    status: payload.status,
  });
}

function getMessageType(message: SDKMessage): string {
  return typeof (message as { type?: unknown }).type === "string"
    ? String((message as { type?: string }).type)
    : "unknown";
}

async function getWorkspaceAccessToken(): Promise<string> {
  const token = await getAccessToken();
  if (!token) {
    throw new Error("Not authenticated");
  }
  return token;
}

async function apiRequest<T>(
  token: string,
  path: string,
  options: Omit<RequestInit, "headers"> & { body?: string | object | null } = {},
): Promise<T> {
  const headers: Record<string, string> = {
    Authorization: `Bearer ${token}`,
  };

  const response = await fetch(`${API_URL}${path}`, {
    ...options,
    headers: {
      ...headers,
      ...(typeof options.headers === "object" && options.headers !== null
        ? options.headers as Record<string, string>
        : {}),
    },
    body: typeof options.body === "string" ? options.body : options.body === undefined || options.body === null ? undefined : JSON.stringify(options.body),
  });

  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(body || `HTTP ${response.status}`);
  }
  return (await response.json()) as T;
}

async function writeOpenShipBundle(workspace: string, files: OpenShipBundleFile[]): Promise<void> {
  const bundleDir = join(workspace, "openship");
  await rm(bundleDir, { recursive: true, force: true });
  await mkdir(bundleDir, { recursive: true });

  for (const file of files) {
    const safePath = file.path.split("/").filter((segment) => segment && segment !== "..");
    if (safePath.length === 0) continue;
    const filePath = join(bundleDir, ...safePath);
    await mkdir(dirname(filePath), { recursive: true });
    await writeFile(filePath, file.content, "utf8");

    if (file.path === AGENTS_BOOTSTRAP_FILE_NAME) {
      await writeFile(join(workspace, AGENTS_BOOTSTRAP_FILE_NAME), file.content, "utf8");
      continue;
    }

    if (file.path === OPENSHIP_SPEC_WORKSPACE_FILE_PATH) {
      const workspaceSpecPath = join(workspace, OPENSHIP_SPEC_WORKSPACE_FILE_PATH);
      await mkdir(dirname(workspaceSpecPath), { recursive: true });
      await writeFile(workspaceSpecPath, file.content, "utf8");
    }
  }
}

function buildRunSummary(changes: AssistantRunCompleteRequest["changes"]): string {
  let created = 0;
  let updated = 0;
  let deleted = 0;

  for (const change of changes) {
    if (change.operation === "Create") created += 1;
    else if (change.operation === "Update") updated += 1;
    else if (change.operation === "Delete") deleted += 1;
  }

  return `OpenShip changes: A=${created}, M=${updated}, D=${deleted}`;
}

async function completeRun(
  token: string,
  handle: string,
  projectName: string,
  threadId: string,
  runId: string,
  payload: AssistantRunCompleteRequest,
) {
  await apiRequest<{ runId: string }>(
    token,
    `/projects/${encodeURIComponent(handle)}/${encodeURIComponent(projectName)}/thread/${encodeURIComponent(threadId)}/assistant/run/${encodeURIComponent(runId)}/complete`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: payload,
    },
  );

  return await apiRequest<AssistantRunResultResponse>(
    token,
    `/projects/${encodeURIComponent(handle)}/${encodeURIComponent(projectName)}/thread/${encodeURIComponent(threadId)}/assistant/run/${encodeURIComponent(runId)}`,
    { method: "GET" },
  );
}

export async function startAssistantRunLocal(payload: {
  handle: string;
  projectName: string;
  threadId: string;
  runId: string;
}): Promise<unknown> {
  const toError = (error: unknown): string =>
    error instanceof Error ? error.message : typeof error === "string" ? error : "Failed to execute local agent run.";
  const token = await getWorkspaceAccessToken();
  const runnerId = `${DEFAULT_RUNNER_ID}-${randomUUID()}`;
  let claimPayload: AssistantRunClaimResponse;
  try {
    claimPayload = await apiRequest<AssistantRunClaimResponse>(
      token,
      `/projects/${encodeURIComponent(payload.handle)}/${encodeURIComponent(payload.projectName)}/thread/${encodeURIComponent(payload.threadId)}/assistant/run/${encodeURIComponent(payload.runId)}/claim`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: { runnerId },
      },
    );
  } catch (error: unknown) {
    const message = toError(error);
    if (message.includes("HTTP 409") || message.includes("Run is not available for claiming")) {
      try {
        return await apiRequest<unknown>(
          token,
          `/projects/${encodeURIComponent(payload.handle)}/${encodeURIComponent(payload.projectName)}/thread/${encodeURIComponent(payload.threadId)}/assistant/run/${encodeURIComponent(payload.runId)}`,
          { method: "GET" },
        );
      } catch (readError: unknown) {
        logRunOutcome({
          runId: payload.runId,
          threadId: payload.threadId,
          stage: "claim",
          status: "failed",
          reason: toError(readError),
        });
        return { error: toError(readError) };
      }
    }
    logRunOutcome({
      runId: payload.runId,
      threadId: payload.threadId,
      stage: "claim",
      status: "failed",
      reason: toError(error),
    });
    return { error: toError(error) };
  }

  if (claimPayload.status !== "running" && claimPayload.status !== "queued") {
    try {
      return await apiRequest<unknown>(
        token,
        `/projects/${encodeURIComponent(payload.handle)}/${encodeURIComponent(payload.projectName)}/thread/${encodeURIComponent(payload.threadId)}/assistant/run/${encodeURIComponent(payload.runId)}`,
        { method: "GET" },
      );
    } catch (error: unknown) {
      return { error: toError(error) };
    }
  }

  let descriptor: OpenShipBundleDescriptor;
  try {
    descriptor = await apiRequest<OpenShipBundleDescriptor>(
      token,
      `/projects/${encodeURIComponent(payload.handle)}/${encodeURIComponent(payload.projectName)}/thread/${encodeURIComponent(payload.threadId)}/openship/bundle`,
      {
        method: "GET",
        headers: { Accept: "application/json" },
      },
    );
  } catch (error: unknown) {
    logRunOutcome({
      runId: payload.runId,
      threadId: payload.threadId,
      stage: "bundle_fetch",
      status: "failed",
      reason: toError(error),
    });
    return { error: toError(error) };
  }

  const workspace = resolveWorkspace(payload.handle, payload.projectName, payload.threadId);
  await writeOpenShipBundle(workspace, descriptor.files);
  const bundleDir = join(workspace, "openship");
  const before = await snapshotOpenShipBundle(bundleDir);
  let turnIndex = 0;
  const logTurn = (message: SDKMessage): void => {
    const sequence = ++turnIndex;
    try {
      const { text, isAnomaly } = summarizeSdkMessage(message);
      const messageType = getMessageType(message);
      const prefix = `[desktop-agent][turn] runId=${payload.runId} threadId=${payload.threadId} seq=${sequence} type=${messageType}`;
      const safeText = truncateForLog(text, 1200);

      if (isAnomaly) {
        console.warn(`${prefix} parse_anomaly ${safeText}`);
        return;
      }

      console.info(`${prefix} ${safeText}`);
    } catch (error: unknown) {
      const warnMessage = error instanceof Error ? error.message : "Unable to log turn";
      console.warn(
        `[desktop-agent][turn] runId=${payload.runId} threadId=${payload.threadId} seq=${sequence} type=unknown parse_failure ${warnMessage}`,
      );
    }
  };

  console.info("[desktop-agent] invoking Claude agent", {
    runId: payload.runId,
    workspace,
    bundleDir,
    systemPrompt: claimPayload.systemPrompt ?? null,
  });

  const runResult = await runClaudeAgent({
    prompt: claimPayload.prompt ?? "Run this request.",
    cwd: workspace,
    systemPrompt: claimPayload.systemPrompt ?? undefined,
    allowedTools: ["Read", "Grep", "Glob", "Bash", "Edit", "Write"],
    model: claimPayload.model?.trim(),
    onMessage: logTurn,
  });
  logRunOutcome({
    runId: payload.runId,
    threadId: payload.threadId,
    stage: "agent_run",
    status: runResult.status,
    reason: runResult.status === "failed"
      ? runResult.error ?? summarizeFailureReason(runResult.messages)
      : null,
  });

  const after = await snapshotOpenShipBundle(bundleDir);
  const fileChanges = diffOpenShipSnapshots(before, after);
  let changes = runResult.changes;
  if (changes.length === 0) {
    changes = fileChanges;
  } else if (fileChanges.length > 0) {
    changes = [...changes, ...fileChanges];
  }
  const messages = [...runResult.messages, buildRunSummary(fileChanges)];

  if (fileChanges.length === 0) {
    console.info("[desktop-agent] OpenShip bundle diff result", {
      runId: payload.runId,
      changed: 0,
      message: "No files changed in OpenShip bundle.",
    });
  } else {
    console.info("[desktop-agent] OpenShip bundle diff result", {
      runId: payload.runId,
      changed: fileChanges.length,
      changes: fileChanges,
    });
  }

  let openShipBundleFiles: OpenShipBundleFile[] | undefined;
  if (runResult.status === "success" && fileChanges.length > 0) {
    try {
      openShipBundleFiles = await collectOpenShipBundleFiles(bundleDir);
    } catch (snapshotError: unknown) {
      const snapshotFailure = `OpenShip reconciliation snapshot failed: ${toError(snapshotError)}`;
      logRunOutcome({
        runId: payload.runId,
        threadId: payload.threadId,
        stage: "snapshot",
        status: "failed",
        reason: snapshotFailure,
      });
      return await completeRun(token, payload.handle, payload.projectName, payload.threadId, payload.runId, {
        status: "failed",
        messages: [...messages, snapshotFailure],
        changes,
        error: toError(snapshotError),
        runnerId,
      });
    }
  }

  try {
    const completionResult = await completeRun(token, payload.handle, payload.projectName, payload.threadId, payload.runId, {
      status: runResult.status,
      messages,
      changes,
      error: runResult.error,
      ...(openShipBundleFiles ? { openShipBundleFiles } : {}),
      runnerId,
    });
    const completionStatus = completionResult.summary?.status ?? runResult.status;
    logRunOutcome({
      runId: payload.runId,
      threadId: payload.threadId,
      stage: "completion",
      status: completionStatus,
      reason: completionStatus === "failed"
        ? summarizeFailureReason(completionResult.summary?.messages) ?? runResult.error ?? null
        : null,
    });
    return completionResult;
  } catch (error: unknown) {
    logRunOutcome({
      runId: payload.runId,
      threadId: payload.threadId,
      stage: "completion",
      status: "failed",
      reason: toError(error),
    });
    return { error: toError(error) };
  }
}
