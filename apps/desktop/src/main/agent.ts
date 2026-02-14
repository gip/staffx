import { randomUUID } from "node:crypto";
import { appendFile, mkdir } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { query, type Query, type SDKMessage } from "@anthropic-ai/claude-agent-sdk";
import { app, type BrowserWindow } from "electron";

interface ActiveAgent {
  id: string;
  query: Query;
  abortController: AbortController;
  sessionId: string | null;
  status: "running" | "completed" | "error" | "cancelled";
}

const activeAgents = new Map<string, ActiveAgent>();
const AGENT_LOG_PATH = join(app.getPath("userData"), "staffx_agent.log");

type AgentLogPayload = {
  threadId: string;
  level: "info" | "error";
  event: string;
  data: Record<string, unknown>;
};

async function appendAgentLog(payload: AgentLogPayload) {
  try {
    const line = JSON.stringify({
      ts: new Date().toISOString(),
      ...payload,
    });
    await appendFile(AGENT_LOG_PATH, `${line}\n`, { encoding: "utf8" });
  } catch (error) {
    console.error("[agent-log] Failed to append", error);
  }
}

interface StartAgentParams {
  prompt: string;
  handle?: string;
  projectName?: string;
  threadId?: string;
  cwd?: string;
  allowedTools?: string[];
  systemPrompt?: string;
  model?: string;
}

function getWorkspacePath(handle: string, projectName: string, threadId: string): string {
  return join(homedir(), ".staffx", "projects", handle, projectName, threadId);
}

export function startAgent(win: BrowserWindow, params: StartAgentParams): string {
  const threadId = randomUUID();
  const abortController = new AbortController();
  const messageSummaries: string[] = [];

  const workspaceCwd =
    params.handle && params.projectName && params.threadId
      ? getWorkspacePath(params.handle, params.projectName, params.threadId)
      : params.cwd ?? process.cwd();

  const initPromise =
    params.handle && params.projectName && params.threadId
      ? mkdir(workspaceCwd, { recursive: true })
      : Promise.resolve();

  const q = query({
    prompt: params.prompt,
    options: {
      permissionMode: "bypassPermissions",
      allowDangerouslySkipPermissions: true,
      abortController,
      allowedTools: params.allowedTools ?? ["Read", "Grep", "Glob", "Bash", "Edit", "Write"],
      cwd: workspaceCwd,
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
  void appendAgentLog({
    threadId,
    level: "info",
    event: "start",
    data: {
      prompt: params.prompt,
      cwd: workspaceCwd,
      allowedTools: params.allowedTools ?? ["Read", "Grep", "Glob", "Bash", "Edit", "Write"],
      systemPrompt: params.systemPrompt ?? null,
      model: params.model ?? null,
    },
  });

  console.info("[agent] start", {
    threadId,
    prompt: params.prompt,
    cwd: workspaceCwd,
    allowedTools: params.allowedTools ?? ["Read", "Grep", "Glob", "Bash", "Edit", "Write"],
    model: params.model,
    systemPrompt: params.systemPrompt,
  });

  (async () => {
    try {
      await initPromise;
      for await (const message of q) {
        if (typeof message === "object" && message !== null) {
          const safeMessage = message as SDKMessage & { content?: unknown; text?: unknown };
          if (typeof safeMessage.type === "string") {
            const content = typeof safeMessage.content === "string"
              ? safeMessage.content
              : typeof safeMessage.text === "string"
                ? safeMessage.text
                : undefined;
            messageSummaries.push(
              content
                ? `[${safeMessage.type}] ${content.slice(0, 400)}`
                : `[${safeMessage.type}] ${JSON.stringify(safeMessage)}`,
            );
          }
        }

        if (message.type === "system" && message.subtype === "init") {
          agent.sessionId = message.session_id;
        }

        if (!win.isDestroyed()) {
          win.webContents.send("agent:message", { threadId, message });
        }
      }

      agent.status = "completed";
      void appendAgentLog({
        threadId,
        level: "info",
        event: "done",
        data: {
          status: agent.status,
          sessionId: agent.sessionId,
          messageCount: messageSummaries.length,
          messageSamples: messageSummaries.slice(-3),
        },
      });
      console.info("[agent] done", {
        threadId,
        status: agent.status,
        sessionId: agent.sessionId,
        resultSamples: messageSummaries.slice(-3),
        messageCount: messageSummaries.length,
      });
    } catch (err: unknown) {
      if (abortController.signal.aborted) {
        agent.status = "cancelled";
        void appendAgentLog({
          threadId,
          level: "info",
          event: "cancelled",
          data: {
            status: agent.status,
            sessionId: agent.sessionId,
            messageCount: messageSummaries.length,
          },
        });
      } else {
        agent.status = "error";
        console.error(`Agent ${threadId} error:`, err);
        void appendAgentLog({
          threadId,
          level: "error",
          event: "error",
          data: {
            status: agent.status,
            sessionId: agent.sessionId,
            messageCount: messageSummaries.length,
            messageSamples: messageSummaries.slice(-3),
            error: err instanceof Error ? err.message : String(err),
          },
        });
        console.error("[agent] done", {
          threadId,
          status: agent.status,
          sessionId: agent.sessionId,
          resultSamples: messageSummaries.slice(-3),
          messageCount: messageSummaries.length,
          error: String(err),
        });
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
