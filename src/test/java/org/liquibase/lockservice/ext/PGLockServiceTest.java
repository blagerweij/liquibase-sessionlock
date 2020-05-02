/*
 * This module, both source code and documentation,
 * is in the Public Domain, and comes with NO WARRANTY.
 */
package org.liquibase.lockservice.ext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import liquibase.database.core.DerbyDatabase;
import liquibase.database.core.PostgresDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LockException;
import liquibase.lockservice.DatabaseChangeLogLock;

public class PGLockServiceTest {

    private PGLockService lockService;

    private Connection dbCon;

    @Before
    public void setUp() {
        dbCon = mock(Connection.class);

        PostgresDatabase database = new PostgresDatabase();
        database.setDefaultSchemaName("test_schema");
        database = spy(database);
        doReturn(new JdbcConnection(dbCon)).when(database).getConnection();

        lockService = new PGLockService();
        lockService.setDatabase(database);
    }

    @Test
    public void supports() throws Exception {
        assertEquals("supports", true, lockService.supports(pgDb(9, 1)));
    }

    @Test
    public void supportsNot() throws Exception {
        assertEquals("supports", false, lockService.supports(pgDb(9, 0)));
    }

    @Test
    public void supportsNot2() throws Exception {
        assertEquals("supports", false, lockService.supports(new DerbyDatabase()));
    }

    private static PostgresDatabase pgDb(int majorVersion, int minorVersion) throws DatabaseException {
        PostgresDatabase database = spy(new PostgresDatabase());
        when(database.getDatabaseMajorVersion()).thenReturn(majorVersion);
        when(database.getDatabaseMinorVersion()).thenReturn(minorVersion);
        return database;
    }

    @Test
    public void supportsFailure() throws Exception {
        PostgresDatabase pgDb = spy(new PostgresDatabase());
        when(pgDb.getDatabaseMajorVersion()).thenThrow(new DatabaseException("test"));
        assertEquals("supports", false, lockService.supports(pgDb));
    }

    @Test
    @SuppressWarnings("resource")
    public void acquireSuccess() throws Exception {
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = booleanResult(true);
        when(stmt.executeQuery()).thenReturn(rs);
        when(dbCon.prepareStatement(PGLockService.SQL_TRY_LOCK)).thenReturn(stmt);

        assertEquals("acquireLock", true, lockService.acquireLock());
        // test_schema.databasechangeloglock
        final long lockId = 0xF0B205EE_A6BFFB24L;
        verify(stmt).setLong(1, lockId);
    }

    @Test
    @SuppressWarnings("resource")
    public void acquireUnsuccessful() throws Exception {
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = booleanResult(false);
        when(stmt.executeQuery()).thenReturn(rs);
        when(dbCon.prepareStatement(PGLockService.SQL_TRY_LOCK)).thenReturn(stmt);

        assertEquals("acquireLock", false, lockService.acquireLock());
    }

    @Test
    @SuppressWarnings("resource")
    public void releaseSuccess() throws Exception {
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = booleanResult(true);
        when(stmt.executeQuery()).thenReturn(rs);
        when(dbCon.prepareStatement(PGLockService.SQL_UNLOCK)).thenReturn(stmt);

        lockService.releaseLock();
        // test_schema.databasechangeloglock
        final long lockId = 0xF0B205EE_A6BFFB24L;
        verify(stmt).setLong(1, lockId);
    }

    @Test
    @SuppressWarnings("resource")
    public void releaseFailure() throws Exception {
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = booleanResult(false);
        when(stmt.executeQuery()).thenReturn(rs);
        when(dbCon.prepareStatement(PGLockService.SQL_UNLOCK)).thenReturn(stmt);

        assertThrows(LockException.class, () -> lockService.releaseLock());
    }

    @Test
    public void noLockInfo() throws Exception {
        DatabaseChangeLogLock[] lockList = lockService.listLocks();
        assertNotNull("Null array reference", lockList);
        assertEquals("Lock list length", 0, lockList.length);
    }

    // Single row / single column
    private static ResultSet booleanResult(Boolean value) throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(false);
        when(rs.getObject(1)).thenReturn(value);
        return rs;
    }

}
