export type SQLParams = (string | number | boolean | null)[];
export type SQLSuccessCallback = (result: SuccessResult) => void;
export type SQLErrorCallback = (result: ErrorResult) => void;

export interface SuccessResult {
  rows: {
    length: number;
    item(i: number): Record<string, string | number | boolean>;
    raw(): Record<string, string | number | boolean>[];
  };
  insertId: number | undefined;
  rowsAffected: number;
}

export interface ErrorResult {
  message: string;
  code: number;
}

export interface Transaction {
  db: Database;
  executeSql(
    sql: string,
    params?: SQLParams
  ): Promise<[Transaction, SuccessResult]>;
}

export interface Database {
  executeSql(
    sql: string,
    params?: SQLParams
  ): Promise<[Transaction, SuccessResult]>;
  transaction(callback: (tx: Transaction) => void): Promise<Transaction>;
  readTransaction(callback: (tx: Transaction) => void): Promise<Transaction>;
  close(): Promise<void>;
}

export type SQLResultSet = SuccessResult[];

type OpenDatabaseOptions = {
  name: string;
  key?: string;
  location?: number;
  createFromLocation?: 1;
  androidDatabaseImplementation?: number;
  androidLockWorkaround?: number;
};

interface Sqlite {
  DEBUG(enable: boolean): void;
  enablePromise(enable: boolean): void;
  openDatabase(options: OpenDatabaseOptions): Promise<Database>;
  deleteDatabase(name: string | { name: string; location?: number }): Promise<void>;
}

declare const Sqlite: Sqlite;

export default Sqlite;
