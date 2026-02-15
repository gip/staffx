import type { BrowserWindow } from "electron";

export interface StartAgentParams {
  prompt: string;
  cwd?: string;
  allowedTools?: string[];
  systemPrompt?: string;
  model?: string;
}

export type AgentStatus = "running" | "completed" | "error" | "cancelled";

export function startAgent(_win: BrowserWindow, _params: StartAgentParams): string {
  throw new Error("Local agent execution has been moved to the shared queue runner.");
}

export function stopAgent(): void {
  // No-op for compatibility.
}

export function getAgentStatus(): { status: AgentStatus; sessionId: string | null } | null {
  return { status: "cancelled", sessionId: null };
}
