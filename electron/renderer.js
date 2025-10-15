export const electronAPI = {
  open: (options, success, error) => {
    return window.sqliteapi.open(options, success, error);
  },
  close: (options, success, error) => {
    return window.sqliteapi.close(options, success, error);
  },
  delete: (options, success, error) => {
    return window.sqliteapi.delete(options, success, error);
  },
  backgroundExecuteSqlBatch: (options, success, error) => {
    return window.sqliteapi.backgroundExecuteSqlBatch(options, success, error);
  }
};
