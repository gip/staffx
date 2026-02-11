import { app, BrowserWindow, ipcMain } from "electron";
import { join } from "node:path";
import { getAuthState, login, logout, notifyRenderer } from "./auth.js";

let mainWindow: BrowserWindow | null = null;

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1024,
    height: 768,
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

ipcMain.on("auth:login", async () => {
  const success = await login();
  if (success && mainWindow) {
    notifyRenderer(mainWindow);
    mainWindow.focus();
  }
});

ipcMain.on("auth:logout", async () => {
  if (mainWindow) notifyRenderer(mainWindow);
  await logout();
});

app.whenReady().then(createWindow);

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") app.quit();
});

app.on("activate", () => {
  if (BrowserWindow.getAllWindows().length === 0) createWindow();
});
