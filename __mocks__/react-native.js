export const NativeModules = {
  SQLite: {
    open: jest.fn(),
    close: jest.fn(),
    delete: jest.fn(),
    backgroundExecuteSqlBatch: jest.fn(),
  },
};

export const Platform = {
  OS: "ios",
};
