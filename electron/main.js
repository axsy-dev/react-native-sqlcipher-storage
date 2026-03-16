import { ipcMain, app } from "electron";
import sqlite3 from "@journeyapps/sqlcipher";
import path from "path";
import { existsSync, unlinkSync } from "fs";

/**
 * @import {Database, Statement, RunResult} from "@journeyapps/sqlcipher"
 *
 * @typedef {object} RunResultWithRows
 * @prop {number} lastID
 * @prop {number} changes
 * @prop {Array<any>} rows
 */

class SQLite {
  /**
   * @type {Map<string, AsyncDatabase>}
   * @private
   */
  #databases = new Map();

  /**
   * @type {string}
   * @private
   */
  #dbLocation = app.getPath("userData");

  /**
   * @param {Event} event
   * @param {{name: string, key: string}} options
   * @returns {Promise<void>}
   */
  open = async (event, options) => {
    const openPath = path.resolve(path.join(this.#dbLocation, options.name));
    const db = await AsyncDatabase.newDatabase(openPath);
    if (options.key) {
      const escapedKey = options.key.replace(/'/g, "''");
      await db.run(`PRAGMA key = '${escapedKey}'`);
      await db.run("PRAGMA cipher_migrate");
    }
    this.#databases.set(options.name, db);
  };

  /**
   * @param {Event} event
   * @param {{path: string}} options
   * @returns {Promise<void>}
   */
  close = async (event, options) => {
    const db = this.#databases.get(options.path);
    if (db) {
      await db.close();
      this.#databases.delete(options.path);
    }
  };

  /**
   * @param {Event} event
   * @param {{path: string}} options
   */
  delete = async (event, options) => {
    if (this.#databases.has(options.path)) {
      await this.close(event, options);
    }
    this.#deleteDatabase(options.path);
  };

  /**
   * @param {Event} event
   * @param {{dbargs: {dbname: string}, executes: Array<{qid: string, sql: string, params: any[]}>}} options
   * @returns {Promise<{
   *  qid: string,
   *  type: "success" | "error",
   *  result: string | {
   *    rowsAffected: number,
   *    rows: any[]
   *  }
   * }[]>}
   */
  backgroundExecuteSqlBatch = async (event, options) => {
    const db = this.#databases.get(options.dbargs.dbname);
    if (!db) {
      throw new Error("Database does not exist");
    }

    const results = [];
    const executes = options.executes;

    for (const e of executes) {
      const execute = e;
      const qid = execute.qid;
      const sql = execute.sql;
      const params = execute.params;

      let resultInfo = { qid };

      try {
        const { rows, rowsAffected, insertId } = await this.#all(
          db,
          sql,
          params
        );

        const hasInsertId = rowsAffected > 0 && insertId !== 0;

        resultInfo = {
          ...resultInfo,
          type: "success",
          result: {
            rowsAffected,
            rows,
          },
        };
        if (hasInsertId) {
          resultInfo.result.insertId = insertId;
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        resultInfo = {
          ...resultInfo,
          type: "error",
          message,
          result: message,
        };
      }

      results.push(resultInfo);
    }

    return results;
  };

  /**
   * @param {AsyncDatabase} db
   * @param {string} sql
   * @param {any[]} params
   * @returns {Promise<{rows: any[], rowsAffected: number, insertId: number}>}
   */
  async #all(db, sql, params) {
    const statement = await db.prepare(sql);

    let result;

    const trimmed = sql.trimStart().toLocaleLowerCase();
    const returnsRows =
      trimmed.startsWith("select") ||
      trimmed.startsWith("pragma") ||
      trimmed.startsWith("explain") ||
      trimmed.startsWith("with");

    if (returnsRows) {
      const all = await statement.all(params);
      result = {
        rowsAffected: all.changes,
        insertId: all.lastID,
        rows: all.rows,
      };
    } else {
      const all = await statement.run(params);
      result = {
        rowsAffected: all.changes,
        insertId: all.lastID,
        rows: [],
      };
    }

    await statement.finalize();

    return result;
  }

  /**
   * @param {string} dbname
   */
  #deleteDatabase(dbname) {
    const dbPath = path.resolve(path.join(this.#dbLocation, dbname));
    if (existsSync(dbPath)) {
      unlinkSync(dbPath);
    }
  }
}

class AsyncDatabase {
  /**
   * @type {Database}
   */
  #db;

  /**
   * @param {Database} db
   */
  constructor(db) {
    /**
     * @type {Database}
     */
    this.#db = db;
  }

  /**
   * @param {string} openPath
   * @returns {Promise<AsyncDatabase>}
   */
  static newDatabase(openPath) {
    return new Promise((resolve, reject) => {
      const db = new sqlite3.Database(openPath, (err) => {
        if (err) {
          reject(err);
        } else {
          resolve(new AsyncDatabase(db));
        }
      });
    });
  }

  /**
   * @param {string} sql
   * @returns {{Promise<RunResult>}}
   */
  run(sql) {
    return new Promise((resolve, reject) => {
      this.#db.run(sql, function (err) {
        if (err) {
          reject(err);
        } else {
          resolve(this);
        }
      });
    });
  }

  /**
   * @returns {Promise<void>}
   */
  close() {
    return new Promise((resolve, reject) => {
      this.#db.close((err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  /**
   * @param {string} sql
   * @returns {AsyncStatement}
   */
  prepare(sql) {
    return new Promise((resolve, reject) => {
      this.#db.prepare(sql, function (err) {
        if (err) {
          reject(err);
        } else {
          resolve(new AsyncStatement(this));
        }
      });
    });
  }
}

class AsyncStatement {
  /**
   * @type {Statement}
   */
  #statement;

  /**
   * @param {Statement} statement
   */
  constructor(statement) {
    this.#statement = statement;
  }

  /**
   * @param {any[]} params
   * @returns {Promise<RunResultWithRows>}
   */
  all(params) {
    return new Promise((resolve, reject) => {
      this.#statement.all(params, function (err, rows) {
        if (err) {
          reject(err);
        } else {
          resolve({ lastID: this.lastID, changes: this.changes, rows });
        }
      });
    });
  }

  /**
   * @param {any[]} params
   * @returns {Promise<RunResult>}
   */
  run(params) {
    return new Promise((resolve, reject) => {
      this.#statement.run(params, function (err) {
        if (err) {
          reject(err);
        } else {
          resolve(this);
        }
      });
    });
  }

  /**
   * @returns {Promise<void>}
   */
  finalize() {
    return new Promise((resolve, reject) => {
      this.#statement.finalize((err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }
}

const sqlite = new SQLite();

export const db = {
  main: {
    init() {
      ipcMain.handle("sqlite:open", sqlite.open);
      ipcMain.handle("sqlite:close", sqlite.close);
      ipcMain.handle("sqlite:delete", sqlite.delete);
      ipcMain.handle(
        "sqlite:backgroundExecuteSqlBatch",
        sqlite.backgroundExecuteSqlBatch
      );
    },
  },
};
