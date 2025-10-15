export type SQLParams = (string | number | boolean)[];
export type SQLSuccessCallback = (result: SuccessResult) => void;
export type SQLErrorCallback = (result: ErrorResult) => void;

export interface SuccessResult {
  rows: {
    length: number;
    item(i: number): Record<string, string | number | boolean>;
    _array: Record<string, string | number | boolean>[];
  };
  insertId: number;
  rowsAffected: number;
}

export interface ErrorResult {
  message: string;
}

export interface Transaction {
  db: Database;
  executeSql(sql: string, params?: SQLParams): Promise<SuccessResult[]>;
}

export interface Database {
  executeSql(sql: string, params?: SQLParams): Promise<SuccessResult[]>;
  transaction(callback: (tx: Transaction) => void): void;
  close(): Promise<void>;
}

export type SQLResultSet = SuccessResult[];

type OpenDatabaseOptions = {
  name: string;
  key: string;
};

interface Sqlite {
  DEBUG(enable: boolean): void;
  enablePromise(enable: boolean): void;
  openDatabase(options: OpenDatabaseOptions): Promise<Database>;
  deleteDatabase(name: string): Promise<void>;
}

declare const Sqlite: Sqlite;

export default Sqlite;
