import { mkdir } from "node:fs/promises";
import { join } from "node:path";
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

function extractMessageSummary(message: SDKMessage): string | null {
  if (typeof message !== "object" || message === null) return null;
  const typed = message as SDKMessage & { content?: unknown; text?: unknown; type?: unknown };
  const type = typeof typed.type === "string" ? typed.type : "message";
  if (typed.type !== "system" && typed.type !== "assistant" && typed.type !== "user" && typed.type !== "tool") {
    return `[${type}] ${JSON.stringify(typed).slice(0, 400)}`;
  }

  const content = typeof typed.content === "string"
    ? typed.content
    : typeof typed.text === "string"
      ? typed.text
      : null;

  return content
    ? `[${type}] ${content.slice(0, 400)}`
    : `[${type}] ${JSON.stringify(typed).slice(0, 400)}`;
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
