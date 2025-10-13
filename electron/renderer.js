export const electronAPI = {
  open: (options, success, error) => {
    window.sqliteapi.open(options, success, error);
  },
  close: (options, success, error) => {
    window.sqliteapi.close(options, success, error);
  },
  delete: (options, success, error) => {
    window.sqliteapi.delete(options, success, error);
  },
  backgroundExecuteSqlBatch: (options, success, error) => {
    window.sqliteapi.backgroundExecuteSqlBatch(options, success, error);
  }
};
