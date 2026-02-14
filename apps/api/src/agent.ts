import { mkdir } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { query, type SDKMessage } from "@anthropic-ai/claude-agent-sdk";

export interface AgentRunParams {
  prompt: string;
  handle: string;
  projectName: string;
  threadId: string;
  systemPrompt?: string;
  allowedTools?: string[];
  model?: string;
}

export interface AgentRunCallbacks {
  onMessage: (msg: SDKMessage) => void;
  onDone: (status: "completed" | "error" | "cancelled") => void;
}

export function getWorkspacePath(handle: string, projectName: string, threadId: string): string {
  return join(homedir(), ".staffx", "projects", handle, projectName, threadId);
}

export async function runAgent(
  params: AgentRunParams,
  callbacks: AgentRunCallbacks,
  signal?: AbortSignal,
): Promise<void> {
  const cwd = getWorkspacePath(params.handle, params.projectName, params.threadId);
  await mkdir(cwd, { recursive: true });

  const abortController = new AbortController();
  if (signal) {
    signal.addEventListener("abort", () => abortController.abort(), { once: true });
  }

  const q = query({
    prompt: params.prompt,
    options: {
      permissionMode: "bypassPermissions",
      allowDangerouslySkipPermissions: true,
      abortController,
      allowedTools: params.allowedTools ?? ["Read", "Grep", "Glob", "Bash", "Edit", "Write"],
      cwd,
      ...(params.systemPrompt ? { systemPrompt: params.systemPrompt } : {}),
      ...(params.model ? { model: params.model } : {}),
    },
  });

  try {
    for await (const message of q) {
      if (signal?.aborted) break;
      callbacks.onMessage(message);
    }
    callbacks.onDone(abortController.signal.aborted ? "cancelled" : "completed");
  } catch (err: unknown) {
    if (abortController.signal.aborted || signal?.aborted) {
      callbacks.onDone("cancelled");
    } else {
      console.error("[agent] error:", err);
      callbacks.onDone("error");
    }
  }
}
