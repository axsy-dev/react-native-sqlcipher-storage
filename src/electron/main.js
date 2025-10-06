const { ipcMain } = require("electron");
const sqlite3 = require("@journeyapps/sqlcipher");

const databases = new Map();

function open(options) {
  return new Promise((resolve, reject) => {
    console.log("*** open", options);
    db = new sqlite3.Database(options.name, (err) => {
      if (err) {
        reject(err);
      } else {
        databases.set(options.name, db);
        resolve(db);
      }
    });
  });
}

function close(options) {
  return new Promise((resolve, reject) => {
    const db = databases.get(options.path);
    if (db) {
      db.close();
      databases.delete(options.path);
    }
  });
}

export function handleSqliteEvents() {
  ipcMain.handle("sqlite:open", open);
  ipcMain.handle("sqlite:close", close);
}
