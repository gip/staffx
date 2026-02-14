import { app, BrowserWindow, ipcMain, nativeImage } from "electron";
import { join } from "node:path";
import { existsSync } from "node:fs";
import { startAgent, stopAgent, getAgentStatus } from "./agent.js";
import { getAccessToken, getAuthState, login, logout, notifyRenderer } from "./auth.js";

let mainWindow: BrowserWindow | null = null;
let appIcon: Electron.NativeImage | null = null;

function resolveIcon(): string | null {
  const candidates = [
    join(app.getAppPath(), "assets", "icon.png"),
    join(process.resourcesPath, "assets", "icon.png"),
    join(process.resourcesPath, "app.asar.unpacked", "assets", "icon.png"),
    join(app.getAppPath(), "assets", "icon.icns"),
    join(process.resourcesPath, "assets", "icon.icns"),
  ];
  return candidates.find((candidatePath) => existsSync(candidatePath)) ?? null;
}

function loadAppIcon() {
  const iconPath = resolveIcon();
  if (!iconPath) {
    console.warn("StaffX icon not found, using Electron default icon.");
    return;
  }
  const icon = nativeImage.createFromPath(iconPath);
  if (icon.isEmpty()) {
    console.warn(`Unable to load StaffX icon from ${iconPath}, using Electron default.`);
    return;
  }
  appIcon = icon;
}

function createWindow() {
  loadAppIcon();

  if (process.platform === "darwin" && app.dock && appIcon) {
    app.dock.setIcon(appIcon);
  }

  mainWindow = new BrowserWindow({
    width: 1024,
    height: 768,
    icon: appIcon ?? undefined,
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
