import { mkdir, rm, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { randomUUID } from "node:crypto";
import { diffOpenShipSnapshots, runClaudeAgent, snapshotOpenShipBundle } from "@staffx/agent-runtime";
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

  return await apiRequest<{
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
  }>(
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
        return { error: toError(readError) };
      }
    }
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
    return { error: toError(error) };
  }

  const workspace = resolveWorkspace(payload.handle, payload.projectName, payload.threadId);
  await writeOpenShipBundle(workspace, descriptor.files);
  const bundleDir = join(workspace, "openship");
  const before = await snapshotOpenShipBundle(bundleDir);

  const runResult = await runClaudeAgent({
    prompt: claimPayload.prompt ?? "Run this request.",
    cwd: workspace,
    systemPrompt: claimPayload.systemPrompt ?? undefined,
    allowedTools: ["Read", "Grep", "Glob", "Bash", "Edit", "Write"],
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

  try {
    return await completeRun(token, payload.handle, payload.projectName, payload.threadId, payload.runId, {
      status: runResult.status,
      messages,
      changes,
      error: runResult.error,
      runnerId,
    });
  } catch (error: unknown) {
    return { error: toError(error) };
  }
}
