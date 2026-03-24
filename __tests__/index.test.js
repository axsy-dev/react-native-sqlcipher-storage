import {NativeModules} from "react-native";
import factory from "../src/index.js";
import plugin from "../src/sqlite.core.js";

const mockSQLite = NativeModules.SQLite;

beforeEach(() => {
  jest.clearAllMocks();
  // Reset to callback mode
  factory.enablePromise(false);
  // Clear openDBs
  Object.keys(plugin.SQLitePlugin.prototype.openDBs).forEach((k) => {
    delete plugin.SQLitePlugin.prototype.openDBs[k];
  });
});

describe("enablePromise", () => {
  it("methods return Promises when enabled", () => {
    factory.enablePromise(true);
    mockSQLite.open.mockImplementation((_opts, success) => success());

    const result = factory.openDatabase({name: "promise.db"});
    expect(result).toBeInstanceOf(Promise);
  });

  it("methods revert to callback style when disabled", () => {
    factory.enablePromise(true);
    factory.enablePromise(false);

    mockSQLite.open.mockImplementation((_opts, success) => success());
    const db = factory.openDatabase({name: "cb.db"});
    // In callback mode, openDatabase returns the SQLitePlugin instance directly
    expect(db).toBeDefined();
    expect(db.dbname).toBe("cb.db");
  });
});

describe("Promise wrapping", () => {
  beforeEach(() => {
    factory.enablePromise(true);
  });

  it("resolves on success", async () => {
    mockSQLite.open.mockImplementation((_opts, success) => success());
    mockSQLite.delete.mockImplementation((_opts, success) =>
      success("deleted"),
    );

    await expect(
      factory.deleteDatabase({name: "del-promise.db"}),
    ).resolves.toBeDefined();
  });

  it("rejects on error", async () => {
    mockSQLite.open.mockImplementation((_opts, _success, error) =>
      error("open failed"),
    );

    await expect(factory.openDatabase({name: "fail.db"})).rejects.toBeDefined();
  });
});

describe("Extra callback runtime (Cb aliases)", () => {
  it("creates Cb-suffixed method aliases when promises are enabled", () => {
    factory.enablePromise(true);

    // The Cb aliases should exist on prototypes
    expect(typeof plugin.SQLitePlugin.prototype.transactionCb).toBe("function");
    expect(typeof plugin.SQLitePlugin.prototype.closeCb).toBe("function");
    expect(typeof plugin.SQLitePlugin.prototype.executeSqlCb).toBe("function");
    expect(typeof plugin.SQLiteFactory.prototype.openDatabaseCb).toBe(
      "function",
    );
    expect(typeof plugin.SQLiteFactory.prototype.deleteDatabaseCb).toBe(
      "function",
    );
  });
});
