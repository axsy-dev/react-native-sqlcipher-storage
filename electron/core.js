import sqlite3 from "@journeyapps/sqlcipher";
import path from "path";
import {existsSync, unlinkSync} from "fs";

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
  #dbLocation;

  /**
   * @param {string} dbLocation
   */
  constructor(dbLocation) {
    this.#dbLocation = dbLocation;
  }

  /**
   * @param {object} options
   * @returns {string}
   */
  #dbKey(options) {
    return options.name ?? options.path ?? options.dbargs?.dbname;
  }

  /**
   * @param {string} name
   * @returns {string}
   */
  #safePath(name) {
    const resolved = path.resolve(path.join(this.#dbLocation, name));
    if (
      !resolved.startsWith(this.#dbLocation + path.sep) &&
      resolved !== this.#dbLocation
    ) {
      throw new Error("Invalid database name: path traversal detected");
    }
    return resolved;
  }

  /**
   * @param {{name: string, key: string}} options
   * @returns {Promise<void>}
   */
  open = async (options) => {
    const openPath = this.#safePath(options.name);
    const db = await AsyncDatabase.newDatabase(openPath);
    try {
      if (options.key) {
        const escapedKey = options.key.replace(/'/g, "''");
        await db.run(`PRAGMA key = '${escapedKey}'`);
        if (options.migrate !== false) {
          await db.run("PRAGMA cipher_migrate");
        }
      }
      this.#databases.set(this.#dbKey(options), db);
    } catch (err) {
      await db.close();
      throw err;
    }
  };

  /**
   * @param {{path: string}} options
   * @returns {Promise<void>}
   */
  close = async (options) => {
    const key = this.#dbKey(options);
    const db = this.#databases.get(key);
    if (db) {
      await db.close();
      this.#databases.delete(key);
    }
  };

  /**
   * @param {{path: string}} options
   */
  delete = async (options) => {
    const key = this.#dbKey(options);
    if (this.#databases.has(key)) {
      await this.close(options);
    }
    this.#deleteDatabase(key);
  };

  /**
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
  backgroundExecuteSqlBatch = async (options) => {
    const db = this.#databases.get(this.#dbKey(options));
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

      let resultInfo = {qid};

      try {
        const {rows, rowsAffected, insertId} = await this.#all(db, sql, params);

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
    try {
      let result;

      const trimmed = sql.trimStart().toLowerCase();
      const cteDmlPattern = /^with\s+.+\)\s*(insert|update|delete)\s/is;
      const returnsRows =
        trimmed.startsWith("select") ||
        trimmed.startsWith("pragma") ||
        trimmed.startsWith("explain") ||
        (trimmed.startsWith("with") && !cteDmlPattern.test(trimmed));

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

      return result;
    } finally {
      await statement.finalize();
    }
  }

  /**
   * @param {string} dbname
   */
  #deleteDatabase(dbname) {
    const dbPath = this.#safePath(dbname);
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
          resolve({lastID: this.lastID, changes: this.changes, rows});
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

export {SQLite, AsyncDatabase, AsyncStatement};
