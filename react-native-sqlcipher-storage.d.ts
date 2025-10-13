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
  executeSql(
    sql: string,
    params?: SQLParams,
    successCallback?: SQLSuccessCallback,
    errorCallback?: SQLErrorCallback
  ): Promise<SuccessResult[]>;
}

export interface Database {
  executeSql(
    sql: string,
    params?: SQLParams,
    successCallback?: SQLSuccessCallback,
    errorCallback?: SQLErrorCallback
  ): Promise<SuccessResult[]>;

  transaction(callback: (tx: Transaction) => void): void;
}
