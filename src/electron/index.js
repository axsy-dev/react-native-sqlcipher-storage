const api = {
  open: (options, success, error) => {
    window.sqliteapi.open(options, success, error);
  },
  close: (options, success, error) => {},
};

export default api;
