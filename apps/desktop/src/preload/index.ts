import { contextBridge, ipcRenderer } from "electron";

contextBridge.exposeInMainWorld("electronAPI", {
  platform: process.platform,
  auth: {
    getState: () => ipcRenderer.invoke("auth:get-state") as Promise<{ isAuthenticated: boolean }>,
    getAccessToken: () => ipcRenderer.invoke("auth:get-token") as Promise<string | null>,
    login: () => ipcRenderer.send("auth:login"),
    logout: () => ipcRenderer.send("auth:logout"),
    onStateChanged: (callback: (state: { isAuthenticated: boolean }) => void) => {
      const handler = (_event: Electron.IpcRendererEvent, state: { isAuthenticated: boolean }) =>
        callback(state);
      ipcRenderer.on("auth:state-changed", handler);
      return () => ipcRenderer.removeListener("auth:state-changed", handler);
    },
  },
  agent: {
    start: (params: { prompt: string; handle?: string; projectName?: string; threadId?: string; cwd?: string; allowedTools?: string[]; systemPrompt?: string; model?: string }) =>
      ipcRenderer.invoke("agent:start", params) as Promise<{ threadId: string }>,
    stop: (threadId: string) => ipcRenderer.send("agent:stop", { threadId }),
    getStatus: (threadId: string) =>
      ipcRenderer.invoke("agent:get-status", { threadId }) as Promise<{ status: string; sessionId: string | null } | null>,
    onMessage: (callback: (data: { threadId: string; message: unknown }) => void) => {
      const handler = (_event: Electron.IpcRendererEvent, data: { threadId: string; message: unknown }) =>
        callback(data);
      ipcRenderer.on("agent:message", handler);
      return () => ipcRenderer.removeListener("agent:message", handler);
    },
    onDone: (callback: (data: { threadId: string; status: string }) => void) => {
      const handler = (_event: Electron.IpcRendererEvent, data: { threadId: string; status: string }) =>
        callback(data);
      ipcRenderer.on("agent:done", handler);
      return () => ipcRenderer.removeListener("agent:done", handler);
    },
  },
});
