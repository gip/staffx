import { createHash } from "node:crypto";
import { mkdir, readdir, readFile, stat } from "node:fs/promises";
import { join, relative, resolve } from "node:path";
import { homedir } from "node:os";
import { type Query, type SDKMessage, query } from "@anthropic-ai/claude-agent-sdk";

type AgentRunStatus = "success" | "failed";

const DEFAULT_TOOLS = ["Read", "Grep", "Glob", "Bash", "Edit", "Write"] as const;

export interface AgentRunPlanChange {
  target_table: string;
  operation: "Create" | "Update" | "Delete";
  target_id: Record<string, unknown>;
  previous: Record<string, unknown> | null;
  current: Record<string, unknown> | null;
}

export interface AgentRunResult {
  status: AgentRunStatus;
  messages: string[];
  changes: AgentRunPlanChange[];
  error?: string;
}

export interface OpenShipBundleSnapshotEntry {
  path: string;
  hash: string;
  size: number;
}

interface OpenShipBundleSnapshotOptions {
  ignoreFileNames?: string[];
  ignoreDirectoryNames?: string[];
  ignoreFileNamePatterns?: RegExp[];
}

export interface ResolveThreadWorkspacePathInput {
  projectId: string;
  threadId: string;
  baseDir?: string;
}

export interface RunClaudeAgentInput {
  prompt: string;
  cwd: string;
  allowedTools?: string[];
  systemPrompt?: string;
  model?: string;
  onMessage?: (message: SDKMessage) => void;
}

export function resolveThreadWorkspacePath(input: ResolveThreadWorkspacePathInput): string {
  const baseDir = input.baseDir?.trim() || join(homedir(), ".staffx", "projects");
  return join(baseDir, input.projectId, input.threadId);
}

function extractMessageText(value: unknown, depth = 0): string[] {
  if (depth > 5 || value === null || value === undefined) return [];

  if (typeof value === "string") {
    return [value];
  }

  if (Array.isArray(value)) {
    return value.flatMap((item) => extractMessageText(item, depth + 1));
  }

  if (typeof value === "object") {
    const typedValue = value as Record<string, unknown>;

    if (typeof typedValue.text === "string") {
      return [typedValue.text];
    }

    if (typeof typedValue.content === "string") {
      return [typedValue.content];
    }

    if (typedValue.content !== undefined) {
      return extractMessageText(typedValue.content, depth + 1);
    }

    if (typedValue.message !== undefined) {
      return extractMessageText(typedValue.message, depth + 1);
    }

    if (typedValue.result !== undefined) {
      return extractMessageText(typedValue.result, depth + 1);
    }

    return [];
  }

  return [];
}

function extractMessageSummary(message: SDKMessage): string | null {
  if (typeof message !== "object" || message === null) return null;
  const typed = message as SDKMessage & {
    content?: unknown;
    text?: unknown;
    type?: unknown;
    message?: unknown;
  };
  const type = typeof typed.type === "string" ? typed.type : "message";

  if (type !== "assistant") {
    return null;
  }

  const contentTextValues = [
    ...(typed.message !== undefined ? extractMessageText(typed.message) : []),
    ...(typed.content !== undefined ? extractMessageText(typed.content) : []),
    ...(typed.text !== undefined ? extractMessageText(typed.text) : []),
  ]
    .filter((entry) => entry.trim().length > 0);

  if (contentTextValues.length === 0) {
    return null;
  }

  return contentTextValues.join("\n");
}

const DEFAULT_IGNORE_FILE_NAMES = new Set([
  ".DS_Store",
  "Thumbs.db",
  "desktop.ini",
]);

const DEFAULT_IGNORE_DIRECTORY_NAMES = new Set([
  ".git",
  ".svn",
  "node_modules",
  ".next",
  ".turbo",
  "dist",
  "build",
  "coverage",
]);

const DEFAULT_IGNORE_FILE_PATTERNS = [
  /^\._/,            // macOS resource fork files
  /^\~/,             // editor backup names
  /~$/,              // editor backup suffix
  /\.swp$/i,         // vim swap files
  /\.tmp$/i,         // temp files
];

function shouldIgnoreFile(pathName: string, options?: OpenShipBundleSnapshotOptions): boolean {
  if (DEFAULT_IGNORE_FILE_NAMES.has(pathName)) return true;

  if (options?.ignoreFileNames?.includes(pathName)) return true;

  const ignoredPatterns = [
    ...DEFAULT_IGNORE_FILE_PATTERNS,
    ...(options?.ignoreFileNamePatterns ?? []),
  ];

  return ignoredPatterns.some((pattern) => pattern.test(pathName));
}

function shouldIgnoreDir(pathName: string, options?: OpenShipBundleSnapshotOptions): boolean {
  if (DEFAULT_IGNORE_DIRECTORY_NAMES.has(pathName)) return true;
  return options?.ignoreDirectoryNames?.includes(pathName) ?? false;
}

async function computeFileHash(path: string): Promise<{ hash: string; size: number }> {
  const contents = await readFile(path);
  return {
    hash: `sha256:${createHash("sha256").update(contents).digest("hex")}`,
    size: contents.length,
  };
}

async function walkOpenShipBundle(
  root: string,
  current: string,
  accumulator: OpenShipBundleSnapshotEntry[],
  options?: OpenShipBundleSnapshotOptions,
): Promise<void> {
  const entries = await readdir(current, { withFileTypes: true });
  const sortedEntries = entries.sort((a, b) => a.name.localeCompare(b.name));

  for (const entry of sortedEntries) {
    if (shouldIgnoreFile(entry.name, options)) continue;
    const childPath = join(current, entry.name);

    if (entry.isDirectory()) {
      if (shouldIgnoreDir(entry.name, options)) continue;
      await walkOpenShipBundle(root, childPath, accumulator, options);
      continue;
    }

    if (!entry.isFile()) continue;

    const { hash, size } = await computeFileHash(childPath);
    const relativePath = relative(root, childPath).replace(/\\/g, "/");
    accumulator.push({
      path: relativePath,
      hash,
      size,
    });
  }
}

export async function snapshotOpenShipBundle(
  bundleDir: string,
  options?: OpenShipBundleSnapshotOptions,
): Promise<OpenShipBundleSnapshotEntry[]> {
  const normalized = resolve(bundleDir);

  try {
    const nodeStats = await stat(normalized);
    if (!nodeStats.isDirectory()) {
      return [];
    }
  } catch {
    return [];
  }

  const snapshot: OpenShipBundleSnapshotEntry[] = [];
  await walkOpenShipBundle(normalized, normalized, snapshot, options);
  return snapshot.sort((left, right) => left.path.localeCompare(right.path));
}

export function diffOpenShipSnapshots(
  before: OpenShipBundleSnapshotEntry[],
  after: OpenShipBundleSnapshotEntry[],
): AgentRunPlanChange[] {
  const beforeByPath = new Map(before.map((entry) => [entry.path, entry]));
  const afterByPath = new Map(after.map((entry) => [entry.path, entry]));
  const paths = new Set<string>([
    ...beforeByPath.keys(),
    ...afterByPath.keys(),
  ]);
  const changes: AgentRunPlanChange[] = [];

  const sortedPaths = [...paths].sort((left, right) => left.localeCompare(right));

  for (const path of sortedPaths) {
    const oldEntry = beforeByPath.get(path);
    const newEntry = afterByPath.get(path);

    if (!oldEntry && newEntry) {
      changes.push({
        target_table: "OpenShipBundleFile",
        operation: "Create",
        target_id: { path },
        previous: null,
        current: { hash: newEntry.hash },
      });
      continue;
    }

    if (oldEntry && !newEntry) {
      changes.push({
        target_table: "OpenShipBundleFile",
        operation: "Delete",
        target_id: { path },
        previous: { hash: oldEntry.hash },
        current: null,
      });
      continue;
    }

    if (oldEntry && newEntry && oldEntry.hash !== newEntry.hash) {
      changes.push({
        target_table: "OpenShipBundleFile",
        operation: "Update",
        target_id: { path },
        previous: { hash: oldEntry.hash },
        current: { hash: newEntry.hash },
      });
    }
  }

  return changes;
}

export function summarizeOpenShipBundleChanges(changes: AgentRunPlanChange[]): string {
  let added = 0;
  let updated = 0;
  let deleted = 0;

  for (const change of changes) {
    if (change.operation === "Create") added += 1;
    else if (change.operation === "Update") updated += 1;
    else if (change.operation === "Delete") deleted += 1;
  }

  return `OpenShip changes: A=${added}, M=${updated}, D=${deleted}`;
}

export async function runClaudeAgent(input: RunClaudeAgentInput): Promise<AgentRunResult> {
  const cwd = input.cwd;
  await mkdir(cwd, { recursive: true });

  const allowedTools = input.allowedTools ?? Array.from(DEFAULT_TOOLS);
  const messages: string[] = [];

  const q = query({
    prompt: input.prompt,
    options: {
      permissionMode: "bypassPermissions",
      allowDangerouslySkipPermissions: true,
      allowedTools,
      cwd,
      ...(input.systemPrompt ? { systemPrompt: input.systemPrompt } : {}),
      ...(input.model ? { model: input.model } : {}),
    },
  });

  try {
    for await (const message of q as AsyncIterable<SDKMessage>) {
      const summary = extractMessageSummary(message);
      if (summary) {
        messages.push(summary);
      }

      if (input.onMessage) {
        input.onMessage(message);
      }
    }

    return {
      status: "success",
      messages: messages.length > 0 ? messages : ["Execution completed."],
      changes: [],
    };
  } catch (error: unknown) {
    const message = error instanceof Error
      ? error.message
      : typeof error === "string"
        ? error
        : "Unknown agent execution error.";

    return {
      status: "failed",
      messages: messages.length > 0 ? [...messages, `Execution failed: ${message}`] : ["Execution failed."],
      changes: [],
      error: message,
    };
  }
}

export type { Query, SDKMessage };
export type { AgentRunStatus };
