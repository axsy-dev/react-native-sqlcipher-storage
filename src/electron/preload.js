import { contextBridge, ipcRenderer } from "electron";

export function initSqliteAPI() {
  contextBridge.exposeInMainWorld("sqliteapi", {
    open: (options, success, error) => {
      ipcRender.invoke("sqlite:open", options).then(success).catch(error);
    },
    close: () => {
      ipcRenderer.invoke;
    },
  });
}
