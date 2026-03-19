// Mock fs
jest.mock("fs", () => ({
  existsSync: jest.fn(() => true),
  unlinkSync: jest.fn(),
}));

// Build a mock sqlite3 Database that simulates the callback-based API
const mockDbInstance = {
  run: jest.fn(),
  close: jest.fn(),
  prepare: jest.fn(),
};

jest.mock("@journeyapps/sqlcipher", () => ({
  default: {
    // Use a regular function so `new` works. Use Promise.resolve().then()
    // to defer the callback so that `const db = new sqlite3.Database(...)` finishes assigning.
    Database: jest.fn(function (path, cb) {
      Promise.resolve().then(() => cb(null));
      Object.assign(this, mockDbInstance);
    }),
  },
  __esModule: true,
}));

import {ipcMain} from "electron";
import {existsSync, unlinkSync} from "fs";
import sqlite3 from "@journeyapps/sqlcipher";
import {db} from "../electron/main.js";

describe("db.main.init()", () => {
  it("registers IPC handlers for all four operations", () => {
    db.main.init();
    expect(ipcMain.handle).toHaveBeenCalledWith(
      "sqlite:open",
      expect.any(Function),
    );
    expect(ipcMain.handle).toHaveBeenCalledWith(
      "sqlite:close",
      expect.any(Function),
    );
    expect(ipcMain.handle).toHaveBeenCalledWith(
      "sqlite:delete",
      expect.any(Function),
    );
    expect(ipcMain.handle).toHaveBeenCalledWith(
      "sqlite:backgroundExecuteSqlBatch",
      expect.any(Function),
    );
  });
});

describe("SQLite operations via IPC handlers", () => {
  let handlers;

  beforeEach(() => {
    jest.clearAllMocks();
    ipcMain.handle.mockClear();
    db.main.init();
    handlers = {};
    ipcMain.handle.mock.calls.forEach(([channel, handler]) => {
      handlers[channel] = handler;
    });

    // Reset mock db behavior
    mockDbInstance.run.mockImplementation(function (sql, cb) {
      cb.call({lastID: 0, changes: 0}, null);
    });
    mockDbInstance.close.mockImplementation((cb) => cb(null));
    mockDbInstance.prepare.mockImplementation(function (sql, cb) {
      const mockStatement = {
        all: jest.fn((params, cb) => {
          cb.call({lastID: 0, changes: 0}, null, []);
        }),
        run: jest.fn((params, cb) => {
          cb.call({lastID: 1, changes: 1}, null);
        }),
        finalize: jest.fn((cb) => cb(null)),
      };
      cb.call(mockStatement, null);
    });
  });

  describe("open", () => {
    it("opens a database without key", async () => {
      await handlers["sqlite:open"]({}, {name: "test.db"});
      expect(sqlite3.Database).toHaveBeenCalled();
    });

    it("sets PRAGMA key with escaped quotes", async () => {
      await handlers["sqlite:open"]({}, {name: "key.db", key: "my'secret"});
      expect(mockDbInstance.run).toHaveBeenCalledWith(
        "PRAGMA key = 'my''secret'",
        expect.any(Function),
      );
    });
  });

  describe("close", () => {
    it("closes and removes from map", async () => {
      await handlers["sqlite:open"]({}, {name: "close-test.db"});
      await handlers["sqlite:close"]({}, {path: "close-test.db"});
      expect(mockDbInstance.close).toHaveBeenCalled();
    });
  });

  describe("delete", () => {
    it("closes then deletes file", async () => {
      await handlers["sqlite:open"]({}, {name: "del-test.db"});
      await handlers["sqlite:delete"]({}, {path: "del-test.db"});
      expect(mockDbInstance.close).toHaveBeenCalled();
      expect(unlinkSync).toHaveBeenCalled();
    });

    it("deletes file even if not open", async () => {
      existsSync.mockReturnValue(true);
      await handlers["sqlite:delete"]({}, {path: "not-open.db"});
      expect(unlinkSync).toHaveBeenCalled();
    });
  });

  describe("backgroundExecuteSqlBatch", () => {
    it("routes SELECT to all() and returns rows", async () => {
      await handlers["sqlite:open"]({}, {name: "batch.db"});

      mockDbInstance.prepare.mockImplementation(function (sql, cb) {
        const mockStatement = {
          all: jest.fn((params, cb) => {
            cb.call({lastID: 0, changes: 0}, null, [{id: 1}]);
          }),
          run: jest.fn(),
          finalize: jest.fn((cb) => cb(null)),
        };
        cb.call(mockStatement, null);
      });

      const results = await handlers["sqlite:backgroundExecuteSqlBatch"](
        {},
        {
          dbargs: {dbname: "batch.db"},
          executes: [{qid: "1", sql: "SELECT * FROM t", params: []}],
        },
      );
      expect(results[0].type).toBe("success");
      expect(results[0].result.rows).toEqual([{id: 1}]);
    });

    it("routes INSERT to run() and includes insertId when appropriate", async () => {
      await handlers["sqlite:open"]({}, {name: "insert.db"});

      mockDbInstance.prepare.mockImplementation(function (sql, cb) {
        const mockStatement = {
          all: jest.fn(),
          run: jest.fn((params, cb) => {
            cb.call({lastID: 42, changes: 1}, null);
          }),
          finalize: jest.fn((cb) => cb(null)),
        };
        cb.call(mockStatement, null);
      });

      const results = await handlers["sqlite:backgroundExecuteSqlBatch"](
        {},
        {
          dbargs: {dbname: "insert.db"},
          executes: [{qid: "2", sql: "INSERT INTO t VALUES (?)", params: [1]}],
        },
      );
      expect(results[0].type).toBe("success");
      expect(results[0].result.insertId).toBe(42);
      expect(results[0].result.rowsAffected).toBe(1);
    });

    it("handles errors", async () => {
      await handlers["sqlite:open"]({}, {name: "err.db"});

      mockDbInstance.prepare.mockImplementation(function (sql, cb) {
        cb.call(null, new Error("syntax error"));
      });

      const results = await handlers["sqlite:backgroundExecuteSqlBatch"](
        {},
        {
          dbargs: {dbname: "err.db"},
          executes: [{qid: "3", sql: "INVALID SQL", params: []}],
        },
      );
      expect(results[0].type).toBe("error");
      expect(results[0].result).toContain("syntax error");
    });

    it("throws when database does not exist", async () => {
      await expect(
        handlers["sqlite:backgroundExecuteSqlBatch"](
          {},
          {
            dbargs: {dbname: "nonexistent.db"},
            executes: [],
          },
        ),
      ).rejects.toThrow("Database does not exist");
    });

    it("does not include insertId when rowsAffected is 0", async () => {
      await handlers["sqlite:open"]({}, {name: "noid.db"});

      mockDbInstance.prepare.mockImplementation(function (sql, cb) {
        const mockStatement = {
          all: jest.fn(),
          run: jest.fn((params, cb) => {
            cb.call({lastID: 0, changes: 0}, null);
          }),
          finalize: jest.fn((cb) => cb(null)),
        };
        cb.call(mockStatement, null);
      });

      const results = await handlers["sqlite:backgroundExecuteSqlBatch"](
        {},
        {
          dbargs: {dbname: "noid.db"},
          executes: [{qid: "4", sql: "UPDATE t SET x = 1 WHERE 0", params: []}],
        },
      );
      expect(results[0].result.insertId).toBeUndefined();
    });
  });

  describe("SQL type detection (#all routing)", () => {
    beforeEach(async () => {
      await handlers["sqlite:open"]({}, {name: "route.db"});
    });

    const testCases = [
      {sql: "SELECT 1", usesAll: true},
      {sql: "  select 1", usesAll: true},
      {sql: "PRAGMA table_info(t)", usesAll: true},
      {sql: "EXPLAIN SELECT 1", usesAll: true},
      {sql: "WITH cte AS (SELECT 1) SELECT * FROM cte", usesAll: true},
      {sql: "INSERT INTO t VALUES (1)", usesAll: false},
      {sql: "UPDATE t SET x = 1", usesAll: false},
      {sql: "DELETE FROM t", usesAll: false},
      {sql: "CREATE TABLE t (id INT)", usesAll: false},
    ];

    testCases.forEach(({sql, usesAll}) => {
      it(`routes "${sql}" to ${usesAll ? "all()" : "run()"}`, async () => {
        const mockAll = jest.fn((params, cb) => {
          cb.call({lastID: 0, changes: 0}, null, []);
        });
        const mockRun = jest.fn((params, cb) => {
          cb.call({lastID: 0, changes: 0}, null);
        });

        mockDbInstance.prepare.mockImplementation(function (s, cb) {
          const mockStatement = {
            all: mockAll,
            run: mockRun,
            finalize: jest.fn((cb) => cb(null)),
          };
          cb.call(mockStatement, null);
        });

        await handlers["sqlite:backgroundExecuteSqlBatch"](
          {},
          {
            dbargs: {dbname: "route.db"},
            executes: [{qid: "r", sql, params: []}],
          },
        );

        if (usesAll) {
          expect(mockAll).toHaveBeenCalled();
          expect(mockRun).not.toHaveBeenCalled();
        } else {
          expect(mockRun).toHaveBeenCalled();
          expect(mockAll).not.toHaveBeenCalled();
        }
      });
    });
  });

  describe("#dbKey extraction", () => {
    it("uses options.name for open and options.path for close", async () => {
      await handlers["sqlite:open"]({}, {name: "byname.db"});
      await handlers["sqlite:close"]({}, {path: "byname.db"});
      expect(mockDbInstance.close).toHaveBeenCalled();
    });
  });
});
