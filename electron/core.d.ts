export declare class AsyncStatement {
  all(
    params: unknown[],
  ): Promise<{lastID: number; changes: number; rows: unknown[]}>;
  run(params: unknown[]): Promise<{lastID: number; changes: number}>;
  finalize(): Promise<void>;
}

export declare class AsyncDatabase {
  static newDatabase(openPath: string): Promise<AsyncDatabase>;
  run(sql: string): Promise<{lastID: number; changes: number}>;
  close(): Promise<void>;
  prepare(sql: string): Promise<AsyncStatement>;
}

export declare class SQLite {
  constructor(dbLocation: string);
  open(options: {name: string; key?: string; migrate?: boolean}): Promise<void>;
  close(options: {
    name?: string;
    path?: string;
    dbargs?: {dbname: string};
  }): Promise<void>;
  delete(options: {
    name?: string;
    path?: string;
    dbargs?: {dbname: string};
  }): Promise<void>;
  backgroundExecuteSqlBatch(options: {
    dbargs: {dbname: string};
    executes: Array<{qid: string; sql: string; params: unknown[]}>;
  }): Promise<
    Array<{
      qid: string;
      type: "success" | "error";
      result:
        | string
        | {rowsAffected: number; rows: unknown[]; insertId?: number};
      message?: string;
    }>
  >;
}
