import { randomUUID } from "node:crypto";
import { query, type Query, type SDKMessage } from "@anthropic-ai/claude-agent-sdk";
import type { BrowserWindow } from "electron";

interface ActiveAgent {
  id: string;
  query: Query;
  abortController: AbortController;
  sessionId: string | null;
  status: "running" | "completed" | "error" | "cancelled";
}

const activeAgents = new Map<string, ActiveAgent>();

interface StartAgentParams {
  prompt: string;
  cwd?: string;
  allowedTools?: string[];
  systemPrompt?: string;
  model?: string;
}

export function startAgent(win: BrowserWindow, params: StartAgentParams): string {
  const threadId = randomUUID();
  const abortController = new AbortController();

  const q = query({
    prompt: params.prompt,
    options: {
      permissionMode: "bypassPermissions",
      allowDangerouslySkipPermissions: true,
      abortController,
      allowedTools: params.allowedTools ?? ["Read", "Grep", "Glob", "Bash", "Edit", "Write"],
      cwd: params.cwd ?? process.cwd(),
      ...(params.systemPrompt ? { systemPrompt: params.systemPrompt } : {}),
      ...(params.model ? { model: params.model } : {}),
    },
  });

  const agent: ActiveAgent = {
    id: threadId,
    query: q,
    abortController,
    sessionId: null,
    status: "running",
  };

  activeAgents.set(threadId, agent);

  (async () => {
    try {
      for await (const message of q) {
        if (message.type === "system" && message.subtype === "init") {
          agent.sessionId = message.session_id;
        }

        if (!win.isDestroyed()) {
          win.webContents.send("agent:message", { threadId, message });
        }
      }

      agent.status = "completed";
    } catch (err: unknown) {
      if (abortController.signal.aborted) {
        agent.status = "cancelled";
      } else {
        agent.status = "error";
        console.error(`Agent ${threadId} error:`, err);
      }
    } finally {
      if (!win.isDestroyed()) {
        win.webContents.send("agent:done", { threadId, status: agent.status });
      }
    }
  })();

  return threadId;
}

export function stopAgent(threadId: string): void {
  const agent = activeAgents.get(threadId);
  if (agent && agent.status === "running") {
    agent.abortController.abort();
  }
}

export function getAgentStatus(threadId: string): { status: string; sessionId: string | null } | null {
  const agent = activeAgents.get(threadId);
  if (!agent) return null;
  return { status: agent.status, sessionId: agent.sessionId };
}
