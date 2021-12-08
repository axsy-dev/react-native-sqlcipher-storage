using System;
using Microsoft.ReactNative;
using Microsoft.ReactNative.Managed;
using System.Collections.Generic;
using Windows.Storage;
using System.IO;
using System.Text;
using SQLitePCL;
using SQLitePCL.Ugly;
using System.Threading.Tasks;

namespace react_native_sqlcipher_storage
{

    static class SqliteException
    {
        public static Exception make(string message) { return new Exception(message); }
        public static Exception make(int rc) { return new Exception(raw.sqlite3_errstr(rc).utf8_to_string()); }

        public static Exception make(int rc, string message) { return new Exception($"{ message}: {raw.sqlite3_errstr(rc).utf8_to_string()}"); }

    }


    class Statement
    {
        public sqlite3_stmt statement;

        public long BindParameterCount
        {
            get
            {
                return ugly.bind_parameter_count(statement);
            }
        }

        public int ColumnCount
        {
            get
            {
                return ugly.column_count(statement);
            }
        }

        public static Statement Prepare(Database db, string sql)
        {
            sqlite3_stmt statement = ugly.prepare(db.database, sql);
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
            ugly.sqlite3_finalize(statement);
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
            switch (p.Type)
            {

                case JSValueType.Int64:
                    ugly.bind_int64(statement, i, p.To<long>());
                    break;
                case JSValueType.Double:
                    ugly.bind_double(statement, i, p.To<double>());
                    break;
                case JSValueType.String:
                    ugly.bind_text(statement, i, p.To<string>());
                    break;
                case JSValueType.Boolean:
                    ugly.bind_int(statement, i, p.To<bool>() ? 1 : 0);
                    break;
                case JSValueType.Null:
                    ugly.bind_null(statement, i);
                    break;
                case JSValueType.Array:
                case JSValueType.Object:
                default:
                    throw SqliteException.make("Unsupprted parameter value");

            }
        }


        int Step()
        {
            return ugly.step(statement);
        }

        public IReadOnlyList<JSValue> All()
        {
            List<JSValue> result = new List<JSValue>();

            try
            {
                int stepResult = Step();
                while (stepResult == raw.SQLITE_ROW)
                {
                    result.Add(new JSValue(GetRow()));
                    stepResult = Step();
                }
            }
            finally
            {
                Close();
            }
            
            return result;

        }

        private int ColumnType(int i)
        {
            return ugly.column_type(statement, i);
        }

        private IReadOnlyDictionary<string, JSValue> GetRow()
        {
            var result = new Dictionary<string, JSValue>();
            var columnCount = ColumnCount;
            for (int i = 0; i < columnCount; ++i)
            {
                var colName = ugly.column_name(statement, i);
                var colType = ColumnType(i);

                switch (colType)
                {
                    case raw.SQLITE_TEXT:
                        result.Add(colName, new JSValue(ugly.column_text(statement, i)));
                        break;
                    case raw.SQLITE_INTEGER:
                        result.Add(colName, new JSValue(ugly.column_int64(statement, i)));
                        break;
                    case raw.SQLITE_FLOAT:
                        result.Add(colName, new JSValue(ugly.column_double(statement, i)));
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
                return database != null ? ugly.total_changes(database) : 0;
            }
        }

        public long LastInsertRowId
        {
            get
            {
                return database != null ? ugly.last_insert_rowid(database) : 0;
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
            try
            {
                database = ugly.open(path);
                if (key != null)
                {
                    ugly.key(database, Encoding.ASCII.GetBytes(key));

                    // check all is good
                    ugly.exec(database, "SELECT count(*) FROM sqlite_master;");

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
            catch(Exception e)
            {
                ugly.close(database);
                throw e;
            }

        }
        ~Database()
        {
            ugly.close_v2(database);
        }

        public IReadOnlyList<JSValue> All(string s, IReadOnlyList<JSValue> p)
        {
            Statement statement = PrepareAndBind(s, p);
            return statement.All();
        }

        public void close()
        {
            ugly.close(database);
        }

    }

    [ReactModule("SQLite")]
    internal sealed class SQLiteModule
    {
        void Initialise()
        {
            if (!initialized)
            {
                Batteries_V2.Init();
                raw.sqlite3_win32_set_directory(/*temp directory type*/2, ApplicationData.Current.TemporaryFolder.Path);
                initialized = true;
            }

        }

        static string version;
        static Dictionary<string, Database> databases = new Dictionary<string, Database>();
        static Dictionary<string, string> databaseKeys = new Dictionary<string, string>();
        static bool initialized = false;


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
            JSValue config,
            Action<int> onSuccess,
            Action<string> onError
            )
        {
            try
            {
                Initialise();
                IReadOnlyDictionary<string, JSValue> cfg = config.To<IReadOnlyDictionary<string, JSValue>>();
                string dbname = cfg.ContainsKey("name") ? cfg["name"].To<string>() : "";
                string opendbname = ApplicationData.Current.LocalFolder.Path + "\\" + dbname;
                string key = cfg.ContainsKey("key") ? cfg["key"].To<string>() : null;
                var db = new Database(opendbname, key);

                if (version == null)
                {
                    strdelegate_exec handler = handleRetrievedVersion;
                    string errorMessage;
                    raw.sqlite3_exec(db.database, "SELECT sqlite_version() || ' (' || sqlite_source_id() || ')' as version", handler, null, out errorMessage);
                }
                databases[dbname] = db;
                databaseKeys[dbname] = key;
                onSuccess(0);
            }
            catch (Exception e)
            {
                onError(e.Message);
            }
        }

        [ReactMethod]
        public void close(
            JSValue config,
            Action<int> onSuccess,
            Action<string> onError
        )
        {
            try
            {
                Initialise();
                IReadOnlyDictionary<string, JSValue> cfg = config.To<IReadOnlyDictionary<string, JSValue>>();
                string dbname = cfg["path"].To<string>();
                Database db = databases[dbname];
                db.close();
                databases.Remove(dbname);
                databaseKeys.Remove(dbname);
                onSuccess(0);

            }
            catch (Exception e)
            {
                onError(e.Message);
            }


        }


        [ReactMethod]
        public async void backgroundExecuteSqlBatch(
            JSValue config,
            Action<IReadOnlyList<JSValue>> onSuccess,
            Action<string> onError
        )
        {
           await Task.Run(() =>
           {
               try
               {
                   Initialise();
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
                    onSuccess(results);
               }
               catch (Exception e)
               {
                   onError(e.Message);
               }
           });

        }

        [ReactMethod]
        public async void delete(
            JSValue config,
            Action<int> onSuccess,
            Action<string> onError
            )
        {
            try
            {
                Initialise();
                IReadOnlyDictionary<string, JSValue> cfg = config.To<IReadOnlyDictionary<string, JSValue>>();
                string dbname = cfg["path"].To<string>();
                if (databases.ContainsKey(dbname))
                {
                    Database db = databases[dbname];
                    db.close();
                    databases.Remove(dbname);
                    databaseKeys.Remove(dbname);
                }
                // TODO this whole method was async but keeps causing a runtime error. Not sure why. So treating DeleteDatabase as fire & forget
                await DeleteDatabase(dbname);
                onSuccess(0);
            }
            catch (Exception e)
            {
                onError(e.Message);
            }

        }

        async Task DeleteDatabase(string dbname)
        {
            var file = await ApplicationData.Current.LocalFolder.TryGetItemAsync(dbname);
            if (file != null)
            {
                await file.DeleteAsync(StorageDeleteOption.PermanentDelete);
            }
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
