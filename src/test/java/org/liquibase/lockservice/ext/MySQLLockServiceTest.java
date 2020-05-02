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

import liquibase.database.core.MySQLDatabase;
import liquibase.database.core.PostgresDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LockException;
import liquibase.lockservice.DatabaseChangeLogLock;

public class MySQLLockServiceTest {

    private MySQLLockService lockService;

    private Connection dbCon;

    @Before
    public void setUp() {
        dbCon = mock(Connection.class);

        MySQLDatabase database = new MySQLDatabase();
        database.setDefaultCatalogName("test_schema");
        database = spy(database);
        doReturn(new JdbcConnection(dbCon)).when(database).getConnection();

        lockService = new MySQLLockService();
        lockService.setDatabase(database);
    }

    @Test
    public void supports() throws Exception {
        assertEquals("supports", true, lockService.supports(new MySQLDatabase()));
    }

    @Test
    public void supportsNot() throws Exception {
        assertEquals("supports", false, lockService.supports(new PostgresDatabase()));
    }

    @Test
    @SuppressWarnings("resource")
    public void acquireSuccess() throws Exception {
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = intResult(1);
        when(stmt.executeQuery()).thenReturn(rs);
        when(dbCon.prepareStatement(MySQLLockService.SQL_GET_LOCK)).thenReturn(stmt);

        assertEquals("acquireLock", true, lockService.acquireLock());
        verify(stmt).setString(1, "TEST_SCHEMA.DATABASECHANGELOGLOCK");
    }

    @Test
    @SuppressWarnings("resource")
    public void acquireUnsuccessful() throws Exception {
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = intResult(0);
        when(stmt.executeQuery()).thenReturn(rs);
        when(dbCon.prepareStatement(MySQLLockService.SQL_GET_LOCK)).thenReturn(stmt);

        assertEquals("acquireLock", false, lockService.acquireLock());
    }

    @Test
    @SuppressWarnings("resource")
    public void acquireFailure() throws Exception {
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery()).thenReturn(rs);
        when(dbCon.prepareStatement(MySQLLockService.SQL_GET_LOCK)).thenReturn(stmt);

        assertThrows(LockException.class, () -> lockService.acquireLock());
    }

    @Test
    @SuppressWarnings("resource")
    public void releaseSuccess() throws Exception {
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = intResult(1);
        when(stmt.executeQuery()).thenReturn(rs);
        when(dbCon.prepareStatement(MySQLLockService.SQL_RELEASE_LOCK)).thenReturn(stmt);

        lockService.releaseLock();
        verify(stmt).setString(1, "TEST_SCHEMA.DATABASECHANGELOGLOCK");
    }

    @Test
    @SuppressWarnings("resource")
    public void releaseFailure() throws Exception {
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = intResult(0);
        when(stmt.executeQuery()).thenReturn(rs);
        when(dbCon.prepareStatement(MySQLLockService.SQL_RELEASE_LOCK)).thenReturn(stmt);

        assertThrows(LockException.class, () -> lockService.releaseLock());
    }

    @Test
    @SuppressWarnings("resource")
    public void usedLockInfo() throws Exception {
        ResultSet infoResult = mock(ResultSet.class);
        when(infoResult.next()).thenReturn(true).thenReturn(false);
        when(infoResult.getObject("PROCESSLIST_ID")).thenReturn(123);
        when(infoResult.getString("HOST")).thenReturn("192.168.254.254:12345");
        when(infoResult.getString("STATE")).thenReturn("testing");
        when(infoResult.getInt("TIME")).thenReturn(15);

        PreparedStatement stmt = mock(PreparedStatement.class);
        when(stmt.executeQuery()).thenReturn(infoResult);
        when(dbCon.prepareStatement(MySQLLockService.SQL_LOCK_INFO)).thenReturn(stmt);

        DatabaseChangeLogLock[] lockList = lockService.listLocks();
        verify(stmt).setString(1, "TEST_SCHEMA.DATABASECHANGELOGLOCK");
        assertEquals("Lock list length", 1, lockList.length);
        assertNotNull("Null lock element", lockList[0]);
        assertEquals("lockedBy", "192.168.254.254 (testing)", lockList[0].getLockedBy());
    }

    @Test
    @SuppressWarnings("resource")
    public void noLockInfo() throws Exception {
        ResultSet infoResult = mock(ResultSet.class);
        when(infoResult.next()).thenReturn(false);
        PreparedStatement stmt = mock(PreparedStatement.class);
        when(stmt.executeQuery()).thenReturn(infoResult);
        when(dbCon.prepareStatement(MySQLLockService.SQL_LOCK_INFO)).thenReturn(stmt);

        DatabaseChangeLogLock[] lockList = lockService.listLocks();
        assertNotNull("Null array reference", lockList);
        assertEquals("Lock list length", 0, lockList.length);
    }

    // Single row / single column
    private static ResultSet intResult(int value) throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(false);
        when(rs.getObject(1)).thenReturn(value);
        return rs;
    }

}
