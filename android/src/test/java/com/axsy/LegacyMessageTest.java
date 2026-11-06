package com.axsy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * SQLitePlugin.legacyMessage() turns sqlcipher-android 4.13.0's exception text ("errmsg (code N)" plus ": detail")
 * back into what android-database-sqlcipher 4.4.2 returned to JS. Inputs use 4.13.0's native format
 * "%s (code %d)%s%s"; expected values are 4.4.2's, also checked on a device by LegacyParityTest.
 */
public class LegacyMessageTest
{
    @Test
    public void queryPrepareErrorKeepsWhileCompiling()
    {
        assertEquals("near \"SELEC\": syntax error: , while compiling: SELEC 1",
                SQLitePlugin.legacyMessage("near \"SELEC\": syntax error (code 1): , while compiling: SELEC 1",
                        SQLitePlugin.MESSAGE_QUERY, null));
    }

    @Test
    public void statementPrepareErrorKeepsWhileCompiling()
    {
        assertEquals("no such table: missing: , while compiling: UPDATE missing SET a = 1",
                SQLitePlugin.legacyMessage("no such table: missing (code 1): , while compiling: UPDATE missing SET a = 1",
                        SQLitePlugin.MESSAGE_STATEMENT, null));
    }

    @Test
    public void statementStepErrorUsesPrimaryCode()
    {
        // 2067 is SQLITE_CONSTRAINT_UNIQUE; 4.4.2 reported the primary code, SQLITE_CONSTRAINT (19).
        assertEquals("error code 19: UNIQUE constraint failed: t.id",
                SQLitePlugin.legacyMessage("UNIQUE constraint failed: t.id (code 2067)",
                        SQLitePlugin.MESSAGE_STATEMENT, null));
    }

    @Test
    public void queryStepErrorIsBareMessage()
    {
        assertEquals("integer overflow",
                SQLitePlugin.legacyMessage("integer overflow (code 1)", SQLitePlugin.MESSAGE_QUERY, null));
    }

    @Test
    public void execErrorAppendsSql()
    {
        assertEquals("database is locked: BEGIN EXCLUSIVE;",
                SQLitePlugin.legacyMessage("database is locked (code 5)", SQLitePlugin.MESSAGE_EXEC,
                        "BEGIN EXCLUSIVE;"));
        assertEquals("cannot commit - no transaction is active: COMMIT;",
                SQLitePlugin.legacyMessage("cannot commit - no transaction is active (code 1)",
                        SQLitePlugin.MESSAGE_EXEC, "COMMIT;"));
    }

    @Test
    public void multiLineMessageStillMatches()
    {
        assertEquals("line one\nline two",
                SQLitePlugin.legacyMessage("line one\nline two (code 1)", SQLitePlugin.MESSAGE_QUERY, null));
    }

    @Test
    public void otherMessagesAreUnchanged()
    {
        String msg = "Cannot perform this operation because there is no current transaction.";
        assertEquals(msg, SQLitePlugin.legacyMessage(msg, SQLitePlugin.MESSAGE_QUERY, null));
        assertEquals(msg, SQLitePlugin.legacyMessage(msg, SQLitePlugin.MESSAGE_EXEC, "COMMIT;"));
        assertNull(SQLitePlugin.legacyMessage(null, SQLitePlugin.MESSAGE_STATEMENT, null));
    }
}
