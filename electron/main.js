import {ipcMain, app} from "electron";
import {SQLite} from "./core.js";

const sqlite = new SQLite(app.getPath("userData"));

export const db = {
  main: {
    init() {
      ipcMain.handle("sqlite:open", (_event, options) => sqlite.open(options));
      ipcMain.handle("sqlite:close", (_event, options) =>
        sqlite.close(options),
      );
      ipcMain.handle("sqlite:delete", (_event, options) =>
        sqlite.delete(options),
      );
      ipcMain.handle("sqlite:backgroundExecuteSqlBatch", (_event, options) =>
        sqlite.backgroundExecuteSqlBatch(options),
      );
    },
  },
};
