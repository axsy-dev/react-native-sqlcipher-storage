using System;
using Microsoft.ReactNative;
using Microsoft.ReactNative.Managed;
using System.Collections.Generic;
using Windows.Storage;
using System.IO;
using System.Text;
using SQLitePCL;

namespace react_native_sqlcipher_storage
{

    static class SqliteException
    {
        public static Exception make(string message) { return new Exception(message); }
        public static Exception make(int rc) { return new Exception(raw.sqlite3_errstr(rc)); }

        public static Exception make(int rc, string message) { return new Exception($"{ message}: {raw.sqlite3_errstr(rc)}"); }

    }

    static class Init
    {
        static Init()
        {
            Batteries_V2.Init();
        }
    }

    class Statement
    {
        public sqlite3_stmt statement;

        public long BindParameterCount
        {
            get
            {
                return raw.sqlite3_bind_parameter_count(statement);
            }
        }

        public int ColumnCount
        {
            get
            {
                return raw.sqlite3_column_count(statement);
            }
        }

        public static Statement Prepare(Database db, string sql)
        {
            sqlite3_stmt statement;
            var ret = raw.sqlite3_prepare_v2(db.database, sql, out statement);
            if (ret != raw.SQLITE_OK)
            {
                raw.sqlite3_finalize(statement);
                throw SqliteException.make(ret, sql);
            }
            return new Statement(statement);
        }

        public Statement(sqlite3_stmt s)
        {
            statement = s;
        }

        ~Statement()
        {
            Close();
        }

        public void Close()
        {
            raw.sqlite3_finalize(statement);
        }
        public void Bind(IReadOnlyList<JSValue> p)
        {
            for (int i = 0; i < p.Count; i++)
            {
                BindParameter(i + 1, p[i]);
            }
        }

        private void BindParameter(int i, JSValue p)
        {
            int result;
            switch (p.Type)
            {

                case JSValueType.Int64:
                    result = raw.sqlite3_bind_int64(statement, i, p.To<long>());
                    break;
                case JSValueType.Double:
                    result = raw.sqlite3_bind_double(statement, i, p.To<double>());
                    break;
                case JSValueType.String:
                    result = raw.sqlite3_bind_text(statement, i, p.To<string>());
                    break;
                case JSValueType.Boolean:
                    result = raw.sqlite3_bind_int(statement, i, p.To<bool>() ? 1 : 0);
                    break;
                case JSValueType.Null:
                    result = raw.sqlite3_bind_null(statement, i);
                    break;
                case JSValueType.Array:
                case JSValueType.Object:
                default:
                    throw SqliteException.make("Unsupprted parameter value");

            }


            if (result != raw.SQLITE_OK)
            {
                throw SqliteException.make(result, "Failed to bind parameter");
            }
        }


        int Step()
        {
            int ret = raw.sqlite3_step(statement);
            if (ret != raw.SQLITE_ROW && ret != raw.SQLITE_DONE)
            {
                throw SqliteException.make(ret);
            }
            return ret;
        }

        public IReadOnlyList<JSValue> All()
        {
            List<JSValue> result = new List<JSValue>();
            int stepResult = Step();
            while (stepResult == raw.SQLITE_ROW)
            {
                result.Add(new JSValue(GetRow()));
                stepResult = Step();
            }
            Close();
            return result;

        }

        private int ColumnType(int i)
        {
            return raw.sqlite3_column_type(statement, i);
        }

        private IReadOnlyDictionary<string, JSValue> GetRow()
        {
            var result = new Dictionary<string, JSValue>();
            var columnCount = ColumnCount;
            for (int i = 0; i < columnCount; ++i)
            {
                var colName = raw.sqlite3_column_name(statement, i);
                var colType = ColumnType(i);

                switch (colType)
                {
                    case raw.SQLITE_TEXT:
                        result.Add(colName, new JSValue(raw.sqlite3_column_text(statement, i)));
                        break;
                    case raw.SQLITE_INTEGER:
                        result.Add(colName, new JSValue(raw.sqlite3_column_int64(statement, i)));
                        break;
                    case raw.SQLITE_FLOAT:
                        result.Add(colName, new JSValue(raw.sqlite3_column_double(statement, i)));
                        break;
                    case raw.SQLITE_NULL:
                    default:
                        result.Add(colName, new JSValue());
                        break;

                }

            }
            return result;

        }
    }

    class Database
    {
        public sqlite3 database;

        public int TotalChanges
        {
            get
            {
                return raw.sqlite3_total_changes(database);
            }
        }

        public long LastInsertRowId
        {
            get
            {
                return raw.sqlite3_last_insert_rowid(database);
            }
        }

        Statement PrepareAndBind(string s, IReadOnlyList<JSValue> p)
        {
            Statement result = Statement.Prepare(this, s);
            result.Bind(p);
            return result;
        }


        public Database(string path, string key = null)
        {
            var res = raw.sqlite3_open(path, out database);
            if (res != raw.SQLITE_OK)
            {
                throw SqliteException.make(res);
            }
            if (key != null)
            {
                // raw.sqlite3_key doesn't seem to exist in this version. Maybe would be safer to escape
                res = raw.sqlite3_exec(database, $"PRAGMA key = '{key}';");
                if (res != raw.SQLITE_OK)
                {
                    throw SqliteException.make(res, "Failed to open database");
                }
                // check all is good
                res = raw.sqlite3_exec(database, "SELECT count(*) FROM sqlite_master;");
                if (res != raw.SQLITE_OK)
                {
                    throw SqliteException.make(res, "Failed to validate database");
                }


            }
            if (raw.sqlite3_threadsafe() > 0)
            {
                Console.WriteLine(@"Good news: SQLite is thread safe!");
            }
            else
            {
                Console.WriteLine(@"Warning: SQLite is not thread safe.");
            }
        }
        ~Database()
        {
            raw.sqlite3_close_v2(database);
        }

        public IReadOnlyList<JSValue> All(string s, IReadOnlyList<JSValue> p)
        {
            Statement statement = PrepareAndBind(s, p);
            return statement.All();
        }

        public void close()
        {
            raw.sqlite3_close_v2(database);
        }

    }

    [ReactModule("SQLite")]
    internal sealed class SQLiteModule
    {

        static string version;
        static Dictionary<string, Database> databases = new Dictionary<string, Database>();
        static Dictionary<string, string> databaseKeys = new Dictionary<string, string>();



        int handleRetrievedVersion(object thing, string[] values, string[] names)
        {
            if (names.GetValue(0).Equals("version"))
            {
                version = values[0];
            }
            return 0;
        }


        [ReactMethod]
        public void open(
            JSValue config
            )
        {
            IReadOnlyDictionary<string, JSValue> cfg = config.To<IReadOnlyDictionary<string, JSValue>>();
            string dbname = cfg.ContainsKey("name") ? cfg["name"].To<string>() : "";
            string opendbname = ApplicationData.Current.LocalFolder.Path + "\\" + dbname;
            string key = cfg.ContainsKey("key") ? cfg["key"].To<string>() : null;
            var db = new Database(opendbname, key);

            if (version == null)
            {
                delegate_exec handler = handleRetrievedVersion;
                string errorMessage;
                raw.sqlite3_exec(db.database, "SELECT sqlite_version() || ' (' || sqlite_source_id() || ')' as version", handler, null, out errorMessage);
            }
            databases[dbname] = db;
            databaseKeys[dbname] = key;
        }

        [ReactMethod]
        public void close(
            JSValue config
        )
        {

            IReadOnlyDictionary<string, JSValue> cfg = config.To<IReadOnlyDictionary<string, JSValue>>();
            string dbname = cfg["path"].To<string>();
            Database db = databases[dbname];
            db.close();
            databases.Remove(dbname);
            databaseKeys.Remove(dbname);

        }


        [ReactMethod]
        public IReadOnlyList<JSValue> backgroundExecuteSqlBatch(
            JSValue config
        )
        {
            var dict = config.To<IReadOnlyDictionary<string, JSValue>>();
            var dbargs = dict["dbargs"].To<IReadOnlyDictionary<string, JSValue>>();
            string dbname = dbargs["dbname"].To<string>();

            if (!databaseKeys.ContainsKey(dbname))
            {
                throw new Exception("Database does not exist");
            }

            var executes = dict["executes"].To<IReadOnlyList<JSValue>>();

            Database db = databases[dbname];

            long totalChanges = db.TotalChanges;
            string q = "";
            var results = new List<JSValue>();
            foreach (JSValue e in executes)
            {
                try
                {
                    var execute = e.To<IReadOnlyDictionary<string, JSValue>>();
                    q = execute["qid"].To<string>();
                    string s = execute["sql"].To<string>();
                    var p = execute["params"].To<IReadOnlyList<JSValue>>();
                    var rows = db.All(s, p);
                    long rowsAffected = db.TotalChanges - totalChanges;
                    totalChanges = db.TotalChanges;
                    var result = new Dictionary<string, JSValue>();
                    result.Add("rowsAffected", new JSValue(rowsAffected));
                    result.Add("rows", new JSValue(rows));
                    result.Add("insertId", new JSValue(db.LastInsertRowId));
                    var resultInfo = new Dictionary<string, JSValue>();
                    resultInfo.Add("type", new JSValue("success"));
                    resultInfo.Add("qid", new JSValue(q));
                    resultInfo.Add("result", new JSValue(result));
                    results.Add(new JSValue(resultInfo));
                }
                catch (Exception err)
                {
                    var resultInfo = new Dictionary<string, JSValue>();
                    var result = new Dictionary<string, JSValue>();
                    result.Add("code", new JSValue(-1));
                    result.Add("message", new JSValue(err.Message));
                    resultInfo.Add("type", new JSValue("error"));
                    resultInfo.Add("qid", new JSValue(q));
                    resultInfo.Add("result", new JSValue(result));
                    results.Add(new JSValue(resultInfo));
                }
            }
            // TODO can we really return a JArray. If so how does that work?
            return results;

        }

        [ReactMethod]
        public async void delete(
            JSValue config
            )
        {
            IReadOnlyDictionary<string, JSValue> cfg = config.To<IReadOnlyDictionary<string, JSValue>>();
            string dbname = cfg["path"].To<string>();
            if (databases.ContainsKey(dbname))
            {
                Database db = databases[dbname];
                db.close();
                databases.Remove(dbname);
                databaseKeys.Remove(dbname);
            }
            StorageFile file = await ApplicationData.Current.LocalFolder.GetFileAsync(dbname);
            await file.DeleteAsync(StorageDeleteOption.PermanentDelete);

        }

        public void OnSuspend()
        {
            OnDestroy();
        }

        public void OnResume()
        {
            reOpenDatabases();
        }

        public void OnDestroy()
        {
            // close all databases

            foreach (KeyValuePair<String, Database> entry in databases)
            {
                entry.Value.close();
            }
            databases.Clear();


        }

        /*
        [ReactInitializer]
        public override void Initialize(IReactContext reactContext)
        {
            // TODo or we don't get OnSuspend and OnResume. Guessing this is in 0.41
            reactContext.AddLifecycleEventListener(this);
        }
        */


        void reOpenDatabases()
        {
            foreach (KeyValuePair<String, String> entry in databaseKeys)
            {
                string opendbname = ApplicationData.Current.LocalFolder.Path + "\\" + entry.Key;
                FileInfo fInfo = new FileInfo(opendbname);
                if (!fInfo.Exists)
                {
                    throw new Exception(opendbname + " not found");
                }
                Database db = new Database(opendbname, entry.Value);
                databases[entry.Key] = db;

            }

        }

    }
}
