package com.axsy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import android.content.Context;

import androidx.test.ext.junit.runners.AndroidJUnit4;
import androidx.test.platform.app.InstrumentationRegistry;

import com.facebook.react.bridge.Callback;
import com.facebook.react.bridge.JavaOnlyArray;
import com.facebook.react.bridge.JavaOnlyMap;
import com.facebook.react.bridge.BridgeReactContext;
import com.facebook.react.bridge.ReadableArray;
import com.facebook.react.soloader.OpenSourceMergedSoMapping;
import com.facebook.soloader.SoLoader;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Drives SQLitePlugin through the same calls JS makes and checks what JS would get back. Every expected value here
 * was recorded from android-database-sqlcipher 4.4.2 (the plugin at d6af598^) on API 24 and API 35 4 KB-page
 * emulators, except the three marked "Known difference", which hold sqlcipher-android's values. A failure on
 * sqlcipher-android means its behaviour differs from 4.4.2.
 *
 * The legacy-*.db assets were written by the 4.4.2 plugin through this same API: open (with KEY for the keyed one),
 * then FIXTURE_SQL, then close.
 */
@RunWith(AndroidJUnit4.class)
public class LegacyParityTest
{
    /** Non-ASCII so the test also proves the key reaches SQLCipher as the same UTF-8 bytes 4.4.2 used. */
    static final String KEY = "parity-ключ-✓";

    static final String[] FIXTURE_SQL = {
            "CREATE TABLE t (id INTEGER PRIMARY KEY, i INTEGER, r REAL, s TEXT, b BLOB, n TEXT)",
            "INSERT INTO t (i, r, s, b, n) VALUES (42, 1.5, 'héllo ✓', x'00ff10', NULL)",
            "INSERT INTO t (i, r, s, b, n) VALUES (-9007199254740991, -0.25, '', x'', NULL)",
    };

    private static final int PAGE = 4096;
    private static final long TIMEOUT_S = 30;

    private static Context context;
    private static SQLitePlugin plugin;
    private final List<String> opened = new ArrayList<>();

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        context = InstrumentationRegistry.getInstrumentation().getTargetContext();
        SoLoader.init(context, OpenSourceMergedSoMapping.INSTANCE); // Arguments.createMap() needs libreactnative.so
        plugin = new SQLitePlugin(new BridgeReactContext(context));
    }

    @After
    public void tearDown() throws Exception
    {
        for (String name : opened)
        {
            close(name);
            context.deleteDatabase(name);
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // Existing databases
    // ------------------------------------------------------------------------------------------------------------

    @Test
    public void keyedDatabaseWrittenBy442Opens() throws Exception
    {
        installAsset("legacy-keyed.db", "keyed.db");
        assertOpenOk("keyed.db", KEY);
        assertFixtureRows("keyed.db");
    }

    @Test
    public void unkeyedDatabaseWrittenBy442Opens() throws Exception
    {
        installAsset("legacy-plain.db", "plain.db");
        assertOpenOk("plain.db", null);
        assertFixtureRows("plain.db");
    }

    @Test
    public void wrongKeyFailsAndKeepsTheFile() throws Exception
    {
        installAsset("legacy-keyed.db", "wrongkey.db");
        long size = dbFile("wrongkey.db").length();

        Reply open = open("wrongkey.db", "not the key");
        assertFalse("open with a wrong key should fail", open.ok);
        assertOpenError(open);
        assertEquals(size, dbFile("wrongkey.db").length());

        assertOpenOk("wrongkey.db", KEY);
        assertFixtureRows("wrongkey.db");
    }

    // ------------------------------------------------------------------------------------------------------------
    // Connection settings
    // ------------------------------------------------------------------------------------------------------------

    @Test
    public void connectionSettingsMatch442() throws Exception
    {
        for (String key : new String[]{KEY, null})
        {
            String name = key == null ? "settings-plain.db" : "settings-keyed.db";
            assertOpenOk(name, key);
            assertSettings(name);
            // A transaction must not reset them.
            batch(name, "BEGIN", "CREATE TABLE x (a)", "COMMIT");
            assertSettings(name);
        }
    }

    private void assertSettings(String name) throws Exception
    {
        assertEquals(2.0, scalar(name, "PRAGMA synchronous")); // FULL
        assertEquals(1048576.0, scalar(name, "PRAGMA journal_size_limit"));
        assertEquals(1000.0, scalar(name, "PRAGMA busy_timeout"));
        assertEquals(4194304.0, scalar(name, "PRAGMA soft_heap_limit"));
        assertEquals("delete", scalar(name, "PRAGMA journal_mode"));
        assertEquals(0.0, scalar(name, "PRAGMA foreign_keys"));
        // Known difference: 4.4.2 returned the number 4096 for keyed databases too. sqlcipher-android's SQLCipher handles
        // page_size itself on keyed databases and returns it as text. Accepted: nothing in react-app reads page_size.
        Object pageSize = name.contains("keyed") ? "4096" : 4096.0;
        assertEquals(name, pageSize, scalar(name, "PRAGMA page_size"));
    }

    // ------------------------------------------------------------------------------------------------------------
    // Corruption
    // ------------------------------------------------------------------------------------------------------------

    @Test
    public void corruptSchemaPageIsDeletedAtOpen() throws Exception
    {
        createPlainWithTable("corrupt-open.db");
        scribble("corrupt-open.db", 100, PAGE - 100); // page 1's b-tree, after the file header

        assertOpenOk("corrupt-open.db", null); // deleted, then opened as a new empty database
        assertEquals(0.0, scalar("corrupt-open.db", "SELECT count(*) FROM sqlite_master"));
    }

    @Test
    public void corruptTablePageIsDeletedBySelect() throws Exception
    {
        createPlainWithTable("corrupt-select.db");
        scribble("corrupt-select.db", PAGE, PAGE); // page 2, the root of t
        assertOpenOk("corrupt-select.db", null);

        Map<String, Object> r = single("corrupt-select.db", "SELECT * FROM t");
        assertError(SELECT_CORRUPT_MESSAGE, r);
        assertFalse("SELECT on a corrupt page deletes the file", dbFile("corrupt-select.db").exists());
    }

    @Test
    public void corruptTablePageIsKeptByUpdateAndInsert() throws Exception
    {
        createPlainWithTable("corrupt-write.db");
        scribble("corrupt-write.db", PAGE, PAGE);
        assertOpenOk("corrupt-write.db", null);

        assertError(UPDATE_CORRUPT_MESSAGE, single("corrupt-write.db", "UPDATE t SET s = 'x'"));
        assertError(INSERT_CORRUPT_MESSAGE, single("corrupt-write.db", "INSERT INTO t (s) VALUES (?)", "x"));
        assertTrue("UPDATE/INSERT on a corrupt page keep the file", dbFile("corrupt-write.db").exists());
    }

    @Test
    public void notADatabaseIsKept() throws Exception
    {
        createPlainWithTable("garbage.db");
        scribble("garbage.db", 0, 100); // the file header

        Reply open = open("garbage.db", null);
        assertFalse(open.ok);
        assertOpenError(open);
        assertTrue(dbFile("garbage.db").exists());
    }

    // ------------------------------------------------------------------------------------------------------------
    // Results and error text returned to JS
    // ------------------------------------------------------------------------------------------------------------

    @Test
    public void resultsAndErrorTextMatch442() throws Exception
    {
        String db = "errors.db";
        assertOpenOk(db, KEY);
        batch(db, "CREATE TABLE u (id INTEGER PRIMARY KEY, v TEXT NOT NULL UNIQUE)");

        Map<String, Object> ins = single(db, "INSERT INTO u (v) VALUES (?)", "a");
        assertSuccess(ins);
        assertEquals(1.0, result(ins).get("insertId"));
        assertEquals(1.0, result(ins).get("rowsAffected"));

        // statement path (INSERT with params, UPDATE, DELETE)
        assertError("error code 19: UNIQUE constraint failed: u.v",
                single(db, "INSERT INTO u (v) VALUES (?)", "a"));
        assertError("error code 19: NOT NULL constraint failed: u.v",
                single(db, "UPDATE u SET v = NULL"));
        assertError("no such table: missing: , while compiling: UPDATE missing SET a = 1",
                single(db, "UPDATE missing SET a = 1"));
        assertError("no such table: missing: , while compiling: DELETE FROM missing",
                single(db, "DELETE FROM missing"));
        Map<String, Object> del = single(db, "DELETE FROM u WHERE v = ?", "nope");
        assertSuccess(del);
        assertEquals(0.0, result(del).get("rowsAffected"));

        // query path (SELECT, PRAGMA, anything else)
        assertError("near \"SELEC\": syntax error: , while compiling: SELEC 1", single(db, "SELEC 1"));
        assertError("integer overflow", single(db, "SELECT abs(-9223372036854775808)"));

        // transactions
        assertError(NO_TRANSACTION_MESSAGE, single(db, "COMMIT"));
        assertError(NO_TRANSACTION_MESSAGE, single(db, "ROLLBACK"));
        assertSuccess(single(db, "BEGIN"));
        assertSuccess(single(db, "INSERT INTO u (v) VALUES (?)", "b"));
        assertSuccess(single(db, "ROLLBACK"));
        assertEquals(1.0, scalar(db, "SELECT count(*) FROM u"));
    }

    /** react-app reads columns with PRAGMA table_info (SearchManager, setupDatabase, coreSchemaMigration). */
    @Test
    public void tableInfoMatches442() throws Exception
    {
        for (String key : new String[]{KEY, null})
        {
            String db = key == null ? "tableinfo-plain.db" : "tableinfo-keyed.db";
            assertOpenOk(db, key);
            // Column types from react-app's schema_sql_core.dump, plus lower-case standard type names.
            batch(db, "CREATE TABLE 'Account' ('_id' INTEGER PRIMARY KEY AUTOINCREMENT, 'sor_id' char(36) NOT NULL UNIQUE, "
                    + "'object_type' varchar DEFAULT NULL, 'op' int(11) NOT NULL, 'created_at' datetime NOT NULL, "
                    + "'sync_status' TEXT CHECK( sync_status IN ('pending','rejected') ) NOT NULL DEFAULT 'pending', "
                    + "'deleted' tinyint(1) DEFAULT 0, 'count' int DEFAULT 0, 'lower_text' text, 'lower_integer' integer, "
                    + "'amount' real, 'data' blob, untyped)");
            for (String sql : new String[]{"PRAGMA table_info(Account);", "PRAGMA table_info(\"Account\");",
                    "PRAGMA table_info('Account');"})
            {
                Map<String, Object> r = single(db, sql);
                assertSuccess(r);
                assertEquals(sql, TABLE_INFO, withLowerCaseType(result(r).get("rows")));
            }
            // No such table: success with no rows key, which SearchManager relies on to fall back.
            Map<String, Object> none = single(db, "PRAGMA table_info(Missing);");
            assertSuccess(none);
            assertEquals(new HashMap<String, Object>(), result(none));
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // Values recorded from 4.4.2
    // ------------------------------------------------------------------------------------------------------------

    static final String SELECT_CORRUPT_MESSAGE = "database disk image is malformed";
    static final String UPDATE_CORRUPT_MESSAGE = "error code 11: database disk image is malformed";
    static final String INSERT_CORRUPT_MESSAGE = "error code 11: database disk image is malformed";
    /**
     * Known difference: SQLite 3.37+ reports a declared type that is exactly a standard name (int, integer, text, real,
     * blob) in upper case, so "int" comes back as "INT"; 4.4.2 returned it as written. Other types (int(11), varchar,
     * ...) are unchanged. Accepted: react-app's coreSchemaMigration lower-cases the type before comparing, and its
     * other table_info callers only read name. Types are compared lower-cased here for that reason.
     */
    static final List<Map<String, Object>> TABLE_INFO = Arrays.asList(
            column(0, "_id", "integer", 0, null, 1),
            column(1, "sor_id", "char(36)", 1, null, 0),
            column(2, "object_type", "varchar", 0, "NULL", 0),
            column(3, "op", "int(11)", 1, null, 0),
            column(4, "created_at", "datetime", 1, null, 0),
            column(5, "sync_status", "text", 1, "'pending'", 0),
            column(6, "deleted", "tinyint(1)", 0, "0", 0),
            column(7, "count", "int", 0, "0", 0),
            column(8, "lower_text", "text", 0, null, 0),
            column(9, "lower_integer", "integer", 0, null, 0),
            column(10, "amount", "real", 0, null, 0),
            column(11, "data", "blob", 0, null, 0),
            column(12, "untyped", "", 0, null, 0));

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> withLowerCaseType(Object rows)
    {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Object o : (List<Object>) rows)
        {
            Map<String, Object> row = new HashMap<>((Map<String, Object>) o);
            row.put("type", ((String) row.get("type")).toLowerCase(java.util.Locale.ROOT));
            out.add(row);
        }
        return out;
    }

    private static Map<String, Object> column(int cid, String name, String type, int notnull, String dflt, int pk)
    {
        Map<String, Object> m = new HashMap<>();
        m.put("cid", (double) cid);
        m.put("name", name);
        m.put("type", type);
        m.put("notnull", (double) notnull);
        m.put("dflt_value", dflt);
        m.put("pk", (double) pk);
        return m;
    }

    /**
     * Known difference: 4.4.2 said "no transaction pending" for COMMIT or ROLLBACK with no open transaction (both still
     * fail). This is sqlcipher-android's IllegalStateException text. Accepted: nothing in react-app matches on it.
     */
    static final String NO_TRANSACTION_MESSAGE = "Cannot perform this operation because there is no current transaction.";

    /**
     * Only the prefix is compared: the rest is the exception's toString(), which names the library's exception class.
     * 4.4.2 gave "net.sqlcipher.database.SQLiteException: unable to close due to unfinalized statements or unfinished
     * backups: sqlite3_close() failed" for both a wrong key and a non-database file.
     */
    private static void assertOpenError(Reply open)
    {
        assertTrue(String.valueOf(open.value), String.valueOf(open.value).startsWith("can't open database "));
    }

    // ------------------------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------------------------

    static final class Reply
    {
        final CountDownLatch latch = new CountDownLatch(1);
        volatile boolean ok;
        volatile Object value;
        final Callback success = args -> done(true, args);
        final Callback error = args -> done(false, args);

        private void done(boolean ok, Object[] args)
        {
            this.ok = ok;
            this.value = args.length > 0 ? args[0] : null;
            latch.countDown();
        }

        Reply await() throws InterruptedException
        {
            assertTrue("no callback within " + TIMEOUT_S + "s", latch.await(TIMEOUT_S, TimeUnit.SECONDS));
            return this;
        }
    }

    private File dbFile(String name)
    {
        return context.getDatabasePath(name);
    }

    private Reply open(String name, String key) throws Exception
    {
        opened.add(name);
        JavaOnlyMap args = JavaOnlyMap.of("name", name);
        if (key != null)
        {
            args.putString("key", key);
        }
        Reply reply = new Reply();
        plugin.open(args, reply.success, reply.error);
        reply.await();
        if (!reply.ok)
        {
            // The plugin calls back before its failed DBRunner leaves dbrmap; a reopen in that window attaches to the
            // dying runner and its queries never run. Same in 4.4.2. Wait it out.
            long deadline = System.currentTimeMillis() + TIMEOUT_S * 1000;
            while (SQLitePlugin.dbrmap.containsKey(name) && System.currentTimeMillis() < deadline)
            {
                Thread.sleep(10);
            }
        }
        return reply;
    }

    private void assertOpenOk(String name, String key) throws Exception
    {
        Reply r = open(name, key);
        assertTrue("open " + name + " failed: " + r.value, r.ok);
    }

    private void close(String name) throws Exception
    {
        Reply reply = new Reply();
        plugin.close(JavaOnlyMap.of("path", name), reply.success, reply.error);
        reply.await();
    }

    /** Runs each statement as its own batch entry, like a JS transaction does, and returns the per-statement results. */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> run(String name, List<Object[]> statements) throws Exception
    {
        JavaOnlyArray executes = new JavaOnlyArray();
        int qid = 0;
        for (Object[] st : statements)
        {
            JavaOnlyMap e = JavaOnlyMap.of("sql", st[0], "qid", String.valueOf(qid++));
            if (st[1] != null)
            {
                e.putArray("params", (ReadableArray) st[1]);
            }
            executes.pushMap(e);
        }
        JavaOnlyMap args = JavaOnlyMap.of("dbargs", JavaOnlyMap.of("dbname", name), "executes", executes);
        Reply reply = new Reply();
        plugin.backgroundExecuteSqlBatch(args, reply.success, reply.error);
        reply.await();
        assertTrue("batch failed: " + reply.value, reply.ok);
        List<Object> out = ((ReadableArray) reply.value).toArrayList();
        List<Map<String, Object>> results = new ArrayList<>();
        for (Object o : out)
        {
            results.add((Map<String, Object>) o);
        }
        return results;
    }

    private void batch(String name, String... sql) throws Exception
    {
        List<Object[]> statements = new ArrayList<>();
        for (String s : sql)
        {
            statements.add(new Object[]{s, new JavaOnlyArray()});
        }
        for (Map<String, Object> r : run(name, statements))
        {
            assertSuccess(r);
        }
    }

    private Map<String, Object> single(String name, String sql, Object... params) throws Exception
    {
        return run(name, Arrays.<Object[]>asList(new Object[]{sql, JavaOnlyArray.of(params)})).get(0);
    }

    private Object scalar(String name, String sql) throws Exception
    {
        Map<String, Object> r = single(name, sql);
        assertSuccess(r);
        List<?> rows = (List<?>) result(r).get("rows");
        Map<?, ?> row = (Map<?, ?>) rows.get(0);
        return row.values().iterator().next();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> result(Map<String, Object> r)
    {
        return (Map<String, Object>) r.get("result");
    }

    private static void assertSuccess(Map<String, Object> r)
    {
        assertEquals("expected success: " + r, "success", r.get("type"));
    }

    private static void assertError(String message, Map<String, Object> r)
    {
        assertEquals("expected error: " + r, "error", r.get("type"));
        assertEquals(message, result(r).get("message"));
    }

    private void assertFixtureRows(String name) throws Exception
    {
        Map<String, Object> r = single(name, "SELECT * FROM t ORDER BY id");
        assertSuccess(r);
        List<?> rows = (List<?>) result(r).get("rows");
        assertEquals(2, rows.size());
        Map<?, ?> a = (Map<?, ?>) rows.get(0);
        assertEquals(1.0, a.get("id"));
        assertEquals(42.0, a.get("i"));
        assertEquals(1.5, a.get("r"));
        assertEquals("héllo ✓", a.get("s"));
        assertEquals("AP8Q\n", a.get("b"));
        assertTrue(a.containsKey("n"));
        assertEquals(null, a.get("n"));
        Map<?, ?> b = (Map<?, ?>) rows.get(1);
        assertEquals(-9007199254740991.0, b.get("i"));
        assertEquals(-0.25, b.get("r"));
        assertEquals("", b.get("s"));
    }

    /** An unkeyed database whose table t has its root on page 2. */
    private void createPlainWithTable(String name) throws Exception
    {
        assertOpenOk(name, null);
        batch(name, FIXTURE_SQL);
        close(name);
        opened.remove(name);
    }

    private void scribble(String name, long offset, int length) throws Exception
    {
        byte[] junk = new byte[length];
        Arrays.fill(junk, (byte) 0xA5);
        try (RandomAccessFile f = new RandomAccessFile(dbFile(name), "rw"))
        {
            f.seek(offset);
            f.write(junk);
        }
    }

    private void installAsset(String asset, String name) throws Exception
    {
        File dest = dbFile(name);
        dest.getParentFile().mkdirs();
        Context testContext = InstrumentationRegistry.getInstrumentation().getContext();
        try (InputStream in = testContext.getAssets().open(asset); OutputStream out = new FileOutputStream(dest))
        {
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) > 0)
            {
                out.write(buf, 0, n);
            }
        }
    }
}
