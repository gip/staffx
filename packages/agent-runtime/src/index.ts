import { createHash } from "node:crypto";
import { mkdir, readdir, readFile, stat } from "node:fs/promises";
import { join, relative, resolve } from "node:path";
import { homedir } from "node:os";
import { type Query, type SDKMessage, query } from "@anthropic-ai/claude-agent-sdk";

type AgentRunStatus = "success" | "failed";

const DEFAULT_TOOLS = ["Read", "Grep", "Glob", "Bash", "Edit", "Write"] as const;
const DEFAULT_MODEL = "claude-opus-4-6";
const ALLOWED_ASSISTANT_MODELS = ["claude-opus-4-6", "claude-sonnet-4-6", "codex-5.3"] as const;
const CODEX_MODEL = "codex-5.3";
type AssistantModel = (typeof ALLOWED_ASSISTANT_MODELS)[number];

export type { AssistantModel };

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

function normalizeAssistantModel(rawModel: string | undefined): AssistantModel {
  return rawModel === "claude-sonnet-4-6" || rawModel === "codex-5.3" ? rawModel : DEFAULT_MODEL;
}

interface BaseRunAgentInput {
  prompt: string;
  cwd: string;
  allowedTools?: string[];
  systemPrompt?: string;
  model?: string;
  onMessage?: (message: SDKMessage) => void;
}

export interface RunClaudeAgentInput extends BaseRunAgentInput {
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null ? value as Record<string, unknown> : null;
}

function isFunction(value: unknown): value is (...args: unknown[]) => unknown {
  return typeof value === "function";
}

function isAsyncIterable<T>(value: unknown): value is AsyncIterable<T> {
  return typeof value === "object" && value !== null && typeof (value as { [Symbol.asyncIterator]?: unknown })[Symbol.asyncIterator] === "function";
}

function normalizeCodexError(error: unknown): string {
  if (error instanceof Error) return error.message || "Unknown codex execution error.";
  if (typeof error === "string") return error;
  return "Unknown codex execution error.";
}

function buildCodexPayload(input: BaseRunAgentInput): Record<string, unknown> {
  return {
    prompt: input.prompt,
    model: CODEX_MODEL,
    cwd: input.cwd,
    systemPrompt: input.systemPrompt,
    allowedTools: input.allowedTools,
  };
}

async function runCodexAgent(input: BaseRunAgentInput): Promise<AgentRunResult> {
  const messages: string[] = [];

  try {
    const codex = await import("@openai/codex-sdk") as Record<string, unknown>;
    const payload = buildCodexPayload(input);
    const invocationTargets: Array<(payload: Record<string, unknown>) => Promise<unknown>> = [];

    const registerFunction = (value: unknown): void => {
      if (!isFunction(value)) return;
      const fn = value as (...args: unknown[]) => unknown;
      invocationTargets.push(async (nextPayload) => fn(nextPayload));
    };

    registerFunction((codex as Record<string, unknown>).run);
    registerFunction((codex as Record<string, unknown>).execute);
    registerFunction((codex as Record<string, unknown>).chat);
    registerFunction((codex as Record<string, unknown>).generate);

    const defaultExport = (codex as Record<string, unknown>).default;
    registerFunction(defaultExport);

    const constructors = [
      (codex as Record<string, unknown>).CodexAgent,
      (codex as Record<string, unknown>).Codex,
      (codex as Record<string, unknown>).Client,
      (codex as Record<string, unknown>).SDK,
    ];

    for (const constructor of constructors) {
      if (!isFunction(constructor)) continue;
      invocationTargets.push(async (nextPayload) => {
        const Constructor = constructor as unknown as { new (options: Record<string, unknown>): unknown };
        const instance = new Constructor(nextPayload);
        const runMethod = asRecord(instance)?.run;
        const executeMethod = asRecord(instance)?.execute;
        if (isFunction(runMethod)) return runMethod.call(instance, nextPayload);
        if (isFunction(executeMethod)) return executeMethod.call(instance, nextPayload);
        throw new Error("No executable method found on codex sdk instance");
      });
    }

    invocationTargets.push(() => {
      throw new Error("Unable to resolve a codex invocation path");
    });

    let lastError: string | null = null;
    let result: unknown = null;

    for (const invoke of invocationTargets) {
      try {
        const output = await invoke(payload);
        result = output;
        if (isAsyncIterable<unknown>(output)) {
          for await (const message of output) {
            if (input.onMessage) input.onMessage(message as SDKMessage);
            const summary = codexMessageText(message);
            if (summary) messages.push(summary);
          }
          break;
        }

        const fallbackSummary = codexMessageText(output);
        if (fallbackSummary) messages.push(fallbackSummary);
        if (input.onMessage) input.onMessage(output as SDKMessage);
        break;
      } catch (error: unknown) {
        lastError = normalizeCodexError(error);
      }
    }

    if (result === null) {
      return {
        status: "failed",
        messages: [lastError ?? "No codex execution path matched."],
        changes: [],
        error: lastError ?? "No codex execution path matched.",
      };
    }

    return {
      status: "success",
      messages: messages.length > 0 ? messages : ["Execution completed."],
      changes: [],
    };
  } catch (error: unknown) {
    return {
      status: "failed",
      messages: ["Execution failed."],
      changes: [],
      error: normalizeCodexError(error),
    };
  }
}

export interface RunAgentInput extends BaseRunAgentInput {
}

export async function runAgent(input: RunAgentInput): Promise<AgentRunResult> {
  const model = normalizeAssistantModel(input.model);
  if (model === CODEX_MODEL) {
    return runCodexAgent(input);
  }
  return runClaudeAgent({
    ...input,
    model,
  });
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

function parseCodexText(value: unknown, depth = 0): string[] {
  if (depth > 8 || value === null || value === undefined) return [];
  if (typeof value === "string") return [value];
  if (Array.isArray(value)) return value.flatMap((entry) => parseCodexText(entry, depth + 1));
  if (typeof value === "object") {
    const typed = value as Record<string, unknown>;
    const source = [
      typed.text,
      typed.content,
      typed.message,
      typed.response,
    ];
    for (const item of source) {
      const parsed = parseCodexText(item, depth + 1);
      if (parsed.length > 0) {
        return parsed;
      }
    }
  }
  return [];
}

function codexMessageText(message: unknown): string | null {
  if (message === null || message === undefined) return null;
  const values = parseCodexText(message);
  const joined = values.map((entry) => entry.trim()).filter((entry) => entry.length > 0);
  return joined.length > 0 ? joined.join("\n") : null;
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
  const model = normalizeAssistantModel(input.model);
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
      model,
      cwd,
      ...(input.systemPrompt ? { systemPrompt: input.systemPrompt } : {}),
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
