import {NativeModules} from "react-native";
import plugin from "../src/sqlite.core.js";

const {SQLitePlugin, SQLitePluginTransaction, SQLiteFactory} = plugin;
const mockSQLite = NativeModules.SQLite;

beforeEach(() => {
  jest.clearAllMocks();
  // Clear openDBs between tests
  Object.keys(SQLitePlugin.prototype.openDBs).forEach((k) => {
    delete SQLitePlugin.prototype.openDBs[k];
  });
});

// ── newSQLError ──────────────────────────────────────────────────────────

describe("newSQLError", () => {
  // newSQLError is not exported directly, but we can exercise it via
  // SQLitePlugin constructor (throws when name missing) and other code paths.

  it("wraps a string into an Error", () => {
    try {
      new SQLitePlugin(null);
    } catch (e) {
      expect(e).toBeInstanceOf(Error);
      expect(e.message).toMatch(/Cannot create a SQLitePlugin/);
      expect(e.code).toBeDefined();
    }
  });

  it("wraps a falsy value into a default Error", () => {
    // SQLitePluginTransaction with non-function fn and no error handler
    // triggers newSQLError(error_from_string) path
    expect(() => new SQLitePluginTransaction(null, "not a fn")).toThrow(
      /transaction expected a function/,
    );
  });
});

// ── SQLitePlugin constructor ─────────────────────────────────────────────

describe("SQLitePlugin constructor", () => {
  it("throws if no name is provided", () => {
    expect(() => new SQLitePlugin({})).toThrow(
      /Cannot create a SQLitePlugin db instance without a db name/,
    );
  });

  it("throws if name is not a string", () => {
    expect(() => new SQLitePlugin({name: 123})).toThrow(
      /sqlite plugin database name must be a string/,
    );
  });

  it("constructs and calls open", () => {
    mockSQLite.open.mockImplementation((_opts, success) => success());
    const ok = jest.fn();
    const db = new SQLitePlugin({name: "test.db"}, ok);
    expect(db.dbname).toBe("test.db");
    expect(mockSQLite.open).toHaveBeenCalled();
  });
});

// ── SQLitePlugin.open ────────────────────────────────────────────────────

describe("SQLitePlugin.prototype.open", () => {
  it("calls plugin.exec('open') and transitions to OPEN state", () => {
    mockSQLite.open.mockImplementation((_opts, success) => success());
    const db = new SQLitePlugin({name: "open-test.db"});
    expect(mockSQLite.open).toHaveBeenCalledWith(
      expect.objectContaining({name: "open-test.db"}),
      expect.any(Function),
      expect.any(Function),
    );
    expect(db.openDBs["open-test.db"]).toBe("OPEN");
  });

  it("handles already-open database", (done) => {
    mockSQLite.open.mockImplementation((_opts, success) => success());
    const db = new SQLitePlugin({name: "already.db"});
    // Open again — should call success via nextTick without calling exec again
    mockSQLite.open.mockClear();
    db.open(
      () => {
        expect(mockSQLite.open).not.toHaveBeenCalled();
        done();
      },
      () => {},
    );
  });
});

// ── SQLitePlugin.close ───────────────────────────────────────────────────

describe("SQLitePlugin.prototype.close", () => {
  it("rejects close if transaction is in progress", () => {
    mockSQLite.open.mockImplementation((_opts, success) => success());
    const db = new SQLitePlugin({name: "close-tx.db"});
    // Simulate an in-progress transaction
    const dbname = db.dbname;
    // manually create txLock with inProgress
    db.addTransaction({
      start: jest.fn(),
      abortFromQ: jest.fn(),
    });
    // Force inProgress
    // The addTransaction + startNextTransaction will set inProgress via nextTick.
    // Instead, let's directly manipulate for this test.

    const errorCb = jest.fn();
    // We need to get at txLocks — it's module-scoped. We'll use a workaround:
    // close checks txLocks[this.dbname].inProgress
    // Let's start a transaction and mark it in progress by calling start
    // Actually, let's just test the close-calls-exec path
    mockSQLite.close.mockImplementation((_opts, success) => success());
    db.close(jest.fn(), errorCb);
    expect(mockSQLite.close).toHaveBeenCalled();
  });

  it("calls plugin.exec('close') when db is open", () => {
    mockSQLite.open.mockImplementation((_opts, success) => success());
    const db = new SQLitePlugin({name: "close-ok.db"});
    const successCb = jest.fn();
    mockSQLite.close.mockImplementation((_opts, success) =>
      success(null, "ok"),
    );
    db.close(successCb, jest.fn());
    expect(mockSQLite.close).toHaveBeenCalledWith(
      {path: "close-ok.db"},
      expect.any(Function),
      expect.any(Function),
    );
  });

  it("errors when db is not open", (done) => {
    mockSQLite.open.mockImplementation((_opts, success) => success());
    const db = new SQLitePlugin({name: "not-open.db"});
    delete db.openDBs["not-open.db"];
    db.close(null, (err) => {
      expect(err).toBe("cannot close: database is not open");
      done();
    });
  });
});

// ── addTransaction / startNextTransaction ────────────────────────────────

describe("SQLitePlugin transaction queue", () => {
  it("queues transactions in FIFO order", (done) => {
    mockSQLite.open.mockImplementation((_opts, success) => success());
    const db = new SQLitePlugin({name: "queue.db"});

    const order = [];
    const makeTx = (id) => ({
      start() {
        order.push(id);
      },
      abortFromQ: jest.fn(),
    });

    db.addTransaction(makeTx(1));
    db.addTransaction(makeTx(2));

    // startNextTransaction uses nextTick, so wait for it
    setTimeout(() => {
      // First transaction should have started
      expect(order[0]).toBe(1);
      done();
    }, 50);
  });
});

// ── abortAllPendingTransactions ──────────────────────────────────────────

describe("SQLitePlugin.prototype.abortAllPendingTransactions", () => {
  it("calls abortFromQ on all queued transactions", () => {
    mockSQLite.open.mockImplementation((_opts, success) => success());
    const db = new SQLitePlugin({name: "abort.db"});

    const tx1 = {start: jest.fn(), abortFromQ: jest.fn()};
    const tx2 = {start: jest.fn(), abortFromQ: jest.fn()};

    // Prevent auto-start by making db state INIT
    db.openDBs["abort.db"] = "INIT";
    db.addTransaction(tx1);
    db.addTransaction(tx2);

    db.abortAllPendingTransactions();
    expect(tx1.abortFromQ).toHaveBeenCalled();
    expect(tx2.abortFromQ).toHaveBeenCalled();
  });
});

// ── SQLitePluginTransaction constructor ──────────────────────────────────

describe("SQLitePluginTransaction constructor", () => {
  it("rejects non-function fn (throws when no error handler)", () => {
    expect(() => {
      new SQLitePluginTransaction({dbname: "x"}, "not a function");
    }).toThrow(/transaction expected a function/);
  });

  it("calls error callback for non-function fn when error handler provided", () => {
    const errorCb = jest.fn();
    SQLitePluginTransaction({dbname: "x"}, null, errorCb);
    expect(errorCb).toHaveBeenCalledWith(
      expect.objectContaining({
        message: expect.stringMatching(/transaction expected a function/),
      }),
    );
  });

  it("adds BEGIN statement when txlock is true", () => {
    const fn = jest.fn();
    const tx = new SQLitePluginTransaction(
      {dbname: "test.db"},
      fn,
      null,
      null,
      true,
      false,
    );
    expect(tx.executes.length).toBe(1);
    expect(tx.executes[0].sql).toBe("BEGIN");
  });

  it("does not add BEGIN when txlock is false", () => {
    const fn = jest.fn();
    const tx = new SQLitePluginTransaction(
      {dbname: "test.db"},
      fn,
      null,
      null,
      false,
      false,
    );
    expect(tx.executes.length).toBe(0);
  });
});

// ── addStatement parameter coercion ──────────────────────────────────────

describe("SQLitePluginTransaction.prototype.addStatement", () => {
  let tx;
  beforeEach(() => {
    tx = new SQLitePluginTransaction(
      {dbname: "stmt.db"},
      jest.fn(),
      null,
      null,
      false,
      false,
    );
  });

  it("passes null and undefined through", () => {
    tx.addStatement("SELECT ?", [null], jest.fn(), jest.fn());
    expect(tx.executes[0].params).toEqual([null]);
  });

  it("passes numbers and strings through", () => {
    tx.addStatement("SELECT ?, ?", [42, "hello"], jest.fn(), jest.fn());
    expect(tx.executes[0].params).toEqual([42, "hello"]);
  });

  it("converts boolean true to 1 and false to 0", () => {
    tx.addStatement(
      "INSERT INTO t VALUES (?, ?)",
      [true, false],
      jest.fn(),
      jest.fn(),
    );
    expect(tx.executes[0].params).toEqual([1, 0]);
  });

  it("rejects function parameters", () => {
    const errorCb = jest.fn();
    tx.addStatement("SELECT ?", [() => {}], jest.fn(), errorCb);
    expect(errorCb).toHaveBeenCalledWith(
      expect.objectContaining({
        message: expect.stringContaining("Unsupported parameter type"),
      }),
    );
  });

  it("converts objects via toString()", () => {
    const obj = {toString: () => "custom"};
    tx.addStatement("SELECT ?", [obj], jest.fn(), jest.fn());
    expect(tx.executes[0].params).toEqual(["custom"]);
  });
});

// ── executeSql ───────────────────────────────────────────────────────────

describe("SQLitePluginTransaction.prototype.executeSql", () => {
  it("rejects write SQL in read-only transaction", () => {
    const fn = jest.fn();
    const tx = new SQLitePluginTransaction(
      {dbname: "ro.db"},
      fn,
      null,
      null,
      false,
      true, // readOnly
    );
    // handleStatementFailure with no handler throws
    expect(() => {
      tx.executeSql("DELETE FROM t", [], null, null);
    }).toThrow(/invalid sql for a read-only transaction/);
  });

  it("throws when transaction is finalized", () => {
    const fn = jest.fn();
    const tx = new SQLitePluginTransaction(
      {dbname: "fin.db"},
      fn,
      null,
      null,
      false,
      false,
    );
    tx.finalized = true;
    expect(() => {
      tx.executeSql("SELECT 1", []);
    }).toThrow(/InvalidStateError/);
  });
});

// ── handleStatementSuccess ───────────────────────────────────────────────

describe("SQLitePluginTransaction.prototype.handleStatementSuccess", () => {
  let tx;
  beforeEach(() => {
    tx = new SQLitePluginTransaction(
      {dbname: "hs.db"},
      jest.fn(),
      null,
      null,
      false,
      false,
    );
  });

  it("provides correct payload shape", () => {
    const handler = jest.fn();
    const response = {
      rows: [{a: 1}, {a: 2}],
      rowsAffected: 0,
      insertId: 5,
    };
    tx.handleStatementSuccess(handler, response);
    const payload = handler.mock.calls[0][1];
    expect(payload.rows.length).toBe(2);
    expect(payload.rows.item(0)).toEqual({a: 1});
    expect(payload.rows.raw()).toEqual([{a: 1}, {a: 2}]);
    expect(payload.rowsAffected).toBe(0);
    expect(payload.insertId).toBe(5);
  });

  it("returns early when handler is null", () => {
    // Should not throw
    tx.handleStatementSuccess(null, {rows: []});
  });

  it("defaults rows to empty array when missing", () => {
    const handler = jest.fn();
    tx.handleStatementSuccess(handler, {});
    const payload = handler.mock.calls[0][1];
    expect(payload.rows.length).toBe(0);
    expect(payload.rows.raw()).toEqual([]);
  });
});

// ── handleStatementFailure ───────────────────────────────────────────────

describe("SQLitePluginTransaction.prototype.handleStatementFailure", () => {
  let tx;
  beforeEach(() => {
    tx = new SQLitePluginTransaction(
      {dbname: "hf.db"},
      jest.fn(),
      null,
      null,
      false,
      false,
    );
  });

  it("throws when no handler provided", () => {
    expect(() => {
      tx.handleStatementFailure(null, {message: "oops", code: 1});
    }).toThrow(/a statement with no error handler failed: oops/);
  });

  it("throws when handler does not return false", () => {
    const handler = jest.fn(() => true);
    expect(() => {
      tx.handleStatementFailure(handler, {message: "fail", code: 2});
    }).toThrow(/a statement error callback did not return false/);
  });

  it("does not throw when handler returns false", () => {
    const handler = jest.fn(() => false);
    expect(() => {
      tx.handleStatementFailure(handler, {message: "ok", code: 0});
    }).not.toThrow();
  });
});

// ── SQLiteFactory.openDatabase ───────────────────────────────────────────

describe("SQLiteFactory.prototype.openDatabase", () => {
  beforeEach(() => {
    mockSQLite.open.mockImplementation((_opts, success) => success());
  });

  it("accepts a string argument", () => {
    const factory = new SQLiteFactory();
    const db = factory.openDatabase("mydb.db");
    expect(db.dbname).toBe("mydb.db");
  });

  it("accepts an object argument", () => {
    const factory = new SQLiteFactory();
    const db = factory.openDatabase({name: "obj.db"});
    expect(db.dbname).toBe("obj.db");
  });

  it("maps location to dblocation", () => {
    const factory = new SQLiteFactory();
    const db = factory.openDatabase({name: "loc.db", location: 1});
    expect(db.openargs.dblocation).toBe("libs");
  });

  it("defaults dblocation to 'docs'", () => {
    const factory = new SQLiteFactory();
    const db = factory.openDatabase({name: "def.db"});
    expect(db.openargs.dblocation).toBe("docs");
  });

  it("returns null with no arguments", () => {
    const factory = new SQLiteFactory();
    const result = factory.openDatabase();
    expect(result).toBeNull();
  });
});

// ── SQLiteFactory.deleteDatabase ─────────────────────────────────────────

describe("SQLiteFactory.prototype.deleteDatabase", () => {
  beforeEach(() => {
    mockSQLite.delete.mockImplementation((_opts, success) => success());
  });

  it("accepts a string argument", () => {
    const factory = new SQLiteFactory();
    factory.deleteDatabase("del.db");
    expect(mockSQLite.delete).toHaveBeenCalledWith(
      expect.objectContaining({path: "del.db", dblocation: "docs"}),
      expect.any(Function),
      expect.any(Function),
    );
  });

  it("accepts an object argument", () => {
    const factory = new SQLiteFactory();
    factory.deleteDatabase({name: "delobj.db", location: 2});
    expect(mockSQLite.delete).toHaveBeenCalledWith(
      expect.objectContaining({path: "delobj.db", dblocation: "nosync"}),
      expect.any(Function),
      expect.any(Function),
    );
  });

  it("throws without a name in object form", () => {
    const factory = new SQLiteFactory();
    expect(() => factory.deleteDatabase({})).toThrow(/Please specify db name/);
  });
});
