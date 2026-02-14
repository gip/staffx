import { useCallback, useEffect, useRef, useState } from "react";

export type AgentStatus = "idle" | "running" | "completed" | "error" | "cancelled";

interface AgentMessage {
  threadId: string;
  message: unknown;
}

interface StartParams {
  prompt: string;
  cwd?: string;
  allowedTools?: string[];
  systemPrompt?: string;
  model?: string;
}

export function useLocalAgent() {
  const [messages, setMessages] = useState<unknown[]>([]);
  const [status, setStatus] = useState<AgentStatus>("idle");
  const [error, setError] = useState<string | null>(null);
  const [threadId, setThreadId] = useState<string | null>(null);

  const threadIdRef = useRef<string | null>(null);
  const cleanupRef = useRef<(() => void) | null>(null);
  const messageCountRef = useRef<number>(0);

  const cleanup = useCallback(() => {
    if (cleanupRef.current) {
      cleanupRef.current();
      cleanupRef.current = null;
    }
  }, []);

  useEffect(() => {
    return cleanup;
  }, [cleanup]);

  const start = useCallback(async (params: StartParams) => {
    if (threadIdRef.current) {
      window.electronAPI.agent.stop(threadIdRef.current);
    }

    cleanup();
    setMessages([]);
    setError(null);
    setStatus("running");
    messageCountRef.current = 0;
    console.info("[local-agent] start", {
      prompt: params.prompt,
      allowedTools: params.allowedTools,
      cwd: params.cwd,
      systemPrompt: params.systemPrompt,
      model: params.model,
    });

    const { threadId: id } = await window.electronAPI.agent.start(params);
    threadIdRef.current = id;
    setThreadId(id);

    const unsubMessage = window.electronAPI.agent.onMessage((data: AgentMessage) => {
      if (data.threadId === threadIdRef.current) {
        messageCountRef.current += 1;
        setMessages((prev) => [...prev, data.message]);
        console.info("[local-agent] message", {
          threadId: data.threadId,
          messageCount: messageCountRef.current,
          message: data.message,
        });
      }
    });

    const unsubDone = window.electronAPI.agent.onDone((data: { threadId: string; status: string }) => {
      if (data.threadId === threadIdRef.current) {
        setStatus(data.status as AgentStatus);
        console.info("[local-agent] done", {
          threadId: data.threadId,
          status: data.status,
          messageCount: messageCountRef.current,
        });
        if (data.status === "error") {
          setError("Agent encountered an error");
        }
      }
    });

    cleanupRef.current = () => {
      unsubMessage();
      unsubDone();
    };

    return id;
  }, [cleanup]);

  const stop = useCallback(() => {
    if (threadIdRef.current) {
      window.electronAPI.agent.stop(threadIdRef.current);
      threadIdRef.current = null;
      setThreadId(null);
      setStatus("idle");
      setError(null);
    }
  }, []);

  return { messages, status, error, threadId, start, stop };
}
