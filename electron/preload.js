import { contextBridge, ipcRenderer } from "electron";

function initSqliteAPI() {
  contextBridge.exposeInMainWorld("sqliteapi", {
    open: async (options, success, error) => {
      try {
        const result = await ipcRenderer.invoke("sqlite:open", options);
        success(result);
      } catch (e) {
        console.error(e);
        error(e);
      }
    },
    close: async (options, success, error) => {
      try {
        const result = await ipcRenderer.invoke("sqlite:close", options);
        success(result);
      } catch (e) {
        console.error(e);
        error(e);
      }
    },
    delete: async (options, success, error) => {
      try {
        await ipcRenderer.invoke("sqlite:delete", options);
        success();
      } catch (e) {
        error(e);
      }
    },
    backgroundExecuteSqlBatch: async (options, success, error) => {
      try {
        const result = await ipcRenderer.invoke(
          "sqlite:backgroundExecuteSqlBatch",
          options,
        );
        success(result);
      } catch (e) {
        error(e);
      }
    },
  });
}

export const db = {
  preload: {
    init() {
      initSqliteAPI();
    },
  },
};
