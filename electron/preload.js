import { contextBridge, ipcRenderer } from "electron";

export function initSqliteAPI() {
  contextBridge.exposeInMainWorld("sqliteapi", {
    open: async (options, success, error) => {
      console.log("sqliteapi:open");
      try {
        const result = await ipcRenderer.invoke("sqlite:open", options);
        console.log("---open", result);
        success(result);
      } catch (e) {
        console.error(e);
        error(e);
      }
    },
    close: async (options, success, error) => {
      console.log("sqliteapi:close");
      try {
        const result = await ipcRenderer.invoke("sqlite:close", options);
        console.log("---close", result);
        success(result);
      } catch (e) {
        console.error(e);
        error(e);
      }
    },
    delete: async (options, success, error) => {
      console.log("sqliteapi:delete");
      try {
        const result = await ipcRenderer.invoke("sqlite:delete", options);
        success();
      } catch (e) {
        error(e);
      }
    },
    backgroundExecuteSqlBatch: async (options, success, error) => {
      console.log("sqliteapi:backgroundExecuteSqlBatch");
      try {
        const result = await ipcRenderer.invoke(
          "sqlite:backgroundExecuteSqlBatch",
          options
        );
        success(result);
      } catch (e) {
        error(e);
      }
    }
  });
}
