import { app, BrowserWindow, ipcMain } from "electron";
import { join } from "node:path";
import { startAgent, stopAgent, getAgentStatus } from "./agent.js";
import { getAccessToken, getAuthState, login, logout, notifyRenderer } from "./auth.js";

let mainWindow: BrowserWindow | null = null;

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1024,
    height: 768,
    titleBarStyle: "hiddenInset",
    trafficLightPosition: { x: 16, y: 14 },
    webPreferences: {
      preload: join(__dirname, "../preload/index.js"),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  if (process.env.ELECTRON_RENDERER_URL) {
    mainWindow.loadURL(process.env.ELECTRON_RENDERER_URL);
  } else {
    mainWindow.loadFile(join(__dirname, "../renderer/index.html"));
  }

  mainWindow.on("closed", () => {
    mainWindow = null;
  });
}

// IPC handlers
ipcMain.handle("auth:get-state", () => getAuthState());
ipcMain.handle("auth:get-token", () => getAccessToken());

ipcMain.on("auth:login", async () => {
  const success = await login();
  if (success && mainWindow) {
    notifyRenderer(mainWindow);
    mainWindow.focus();
  }
});

ipcMain.on("auth:logout", async () => {
  await logout();
  if (mainWindow) notifyRenderer(mainWindow);
});

// Agent IPC handlers
ipcMain.handle("agent:start", (_e, params) => {
  if (!mainWindow) throw new Error("No main window");
  return { threadId: startAgent(mainWindow, params) };
});

ipcMain.on("agent:stop", (_e, { threadId }) => stopAgent(threadId));

ipcMain.handle("agent:get-status", (_e, { threadId }) => getAgentStatus(threadId));

app.whenReady().then(createWindow);

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") app.quit();
});

app.on("activate", () => {
  if (BrowserWindow.getAllWindows().length === 0) createWindow();
});
