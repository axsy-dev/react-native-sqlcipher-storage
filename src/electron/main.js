const { ipcMain, app, Event } = require("electron");
const sqlite3 = require("@journeyapps/sqlcipher");
const path = require("path");
const { existsSync, unlinkSync } = require("node:fs");
const log = require("electron-log/main");

class SQLite {
  /**
   * @type {Map<string, sqlite3.Database>}
   */
  databases = new Map();

  dbLocation = app.getPath("userData");

  /**
   *
   * @param {Event} event
   * @param {{name: string}} options
   * @returns {Promise}
   */
  open = async (event, options) => {
    log.info(`open ${JSON.stringify(options)}`);
    return new Promise((resolve, reject) => {
      const openPath = path.resolve(path.join(this.dbLocation, options.name));
      log.info(`*** sqlite open ${openPath}`);

      const db = new sqlite3.Database(openPath, (err) => {
        if (err) {
          reject(err);
        } else {
          this.databases.set(options.name, db);
          log.info(`*** sqlite open - success`, db);
          resolve(db);
        }
      });
    });
  };

  /**
   *
   * @param {Event} event
   * @param {{path: string}} options
   * @returns {Promise}
   */
  close = async (event, options) => {
    log.info(`*** close - begin`);
    return new Promise((resolve, reject) => {
      const db = this.databases.get(options.path);
      if (db) {
        log.info(`*** close ${options.path}`);
        db.close((err) => {
          if (err) {
            reject(err);
          } else {
            log.info(`*** close - success`);
            this.databases.delete(options.path);
            resolve();
          }
        });
      } else {
        resolve();
      }
    });
  };

  /**
   *
   * @param {Event} event
   * @param {{dbargs: {dbname: string}, executes: Array<{qid: string, sql: string, params: any[]}>} options
   * @returns
   */
  backgroundExecuteSqlBatch = async (event, options) => {
    return new Promise((resolve, reject) => {
      const db = this.databases.get(options.dbargs.dbname);
      if (!db) {
        reject("Database does not exist");
        return;
      }

      const executes = options.executes;

      let totalChanges = db.totalChanges;
      const results = [];

      for (const e of executes) {
        const execute = e;
        const qid = execute.qid;
        const sql = execute.sql;
        const params = execute.params;
        const rows = db.all(sql, params);
        const rowsAffected = db.totalChanges - totalChanges;
        totalChanges = db.totalChanges;
        const resultInfo = {
          qid,
          type: "success",
          result: {
            rowsAffected,
            rows,
            insertId: db.insertId,
          },
        };
        results.push(resultInfo);
      }

      resolve(results);
    });
  };

  /**
   *
   * @param {Event} event
   * @param {{path: string}} options
   */
  delete = async (event, options) => {
    if (this.databases.has(options.path)) {
      await this.close(event, options);
      await this.#deleteDatabase(options.path);
    }
  };

  #deleteDatabase = async (dbname) => {
    const dbPath = path.resolve(path.join(this.dbLocation, dbname));
    if (existsSync(dbPath)) {
      unlinkSync(dbPath);
    }
  };
}

const sqlite = new SQLite();

export function handleSqliteEvents() {
  ipcMain.handle("sqlite:open", sqlite.open);
  ipcMain.handle("sqlite:close", sqlite.close);
  ipcMain.handle("sqlite:delete", sqlite.delete);
  ipcMain.handle;
}
