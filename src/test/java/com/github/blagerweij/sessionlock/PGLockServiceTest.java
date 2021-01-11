/*
 * This module, both source code and documentation,
 * is in the Public Domain, and comes with NO WARRANTY.
 */
package com.github.blagerweij.sessionlock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import liquibase.database.core.DerbyDatabase;
import liquibase.database.core.PostgresDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LockException;
import liquibase.lockservice.DatabaseChangeLogLock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PGLockServiceTest {

  private PGLockService lockService;

  private Connection dbCon;

  @BeforeEach
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
    assertThat(lockService.supports(pgDb(9, 1))).isTrue();
  }

  @Test
  public void supportsNot() throws Exception {
    assertThat(lockService.supports(pgDb(9, 0))).isFalse();
  }

  @Test
  public void supportsNot2() throws Exception {
    assertThat(lockService.supports(new DerbyDatabase())).isFalse();
  }

  private static PostgresDatabase pgDb(int majorVersion, int minorVersion)
      throws DatabaseException {
    PostgresDatabase database = spy(new PostgresDatabase());
    when(database.getDatabaseMajorVersion()).thenReturn(majorVersion);
    when(database.getDatabaseMinorVersion()).thenReturn(minorVersion);
    return database;
  }

  @Test
  public void supportsFailure() throws Exception {
    PostgresDatabase pgDb = spy(new PostgresDatabase());
    when(pgDb.getDatabaseMajorVersion()).thenThrow(new DatabaseException("test"));
    assertThat(lockService.supports(pgDb)).isFalse();
  }

  @Test
  @SuppressWarnings("resource")
  public void acquireSuccess() throws Exception {
    PreparedStatement stmt = mock(PreparedStatement.class);
    ResultSet rs = booleanResult(true);
    when(stmt.executeQuery()).thenReturn(rs);
    when(dbCon.prepareStatement(PGLockService.SQL_TRY_LOCK)).thenReturn(stmt);

    assertThat(lockService.acquireLock()).isTrue();
    verifyLockParameters(stmt);
  }

  @Test
  @SuppressWarnings("resource")
  public void acquireUnsuccessful() throws Exception {
    PreparedStatement stmt = mock(PreparedStatement.class);
    ResultSet rs = booleanResult(false);
    when(stmt.executeQuery()).thenReturn(rs);
    when(dbCon.prepareStatement(PGLockService.SQL_TRY_LOCK)).thenReturn(stmt);

    assertThat(lockService.acquireLock()).isFalse();
  }

  @Test
  @SuppressWarnings("resource")
  public void releaseSuccess() throws Exception {
    PreparedStatement stmt = mock(PreparedStatement.class);
    ResultSet rs = booleanResult(true);
    when(stmt.executeQuery()).thenReturn(rs);
    when(dbCon.prepareStatement(PGLockService.SQL_UNLOCK)).thenReturn(stmt);

    lockService.releaseLock();
    verifyLockParameters(stmt);
  }

  @Test
  @SuppressWarnings("resource")
  public void releaseFailure() throws Exception {
    PreparedStatement stmt = mock(PreparedStatement.class);
    ResultSet rs = booleanResult(false);
    when(stmt.executeQuery()).thenReturn(rs);
    when(dbCon.prepareStatement(PGLockService.SQL_UNLOCK)).thenReturn(stmt);

    assertThatThrownBy(() -> lockService.releaseLock()).isInstanceOf(LockException.class);
  }

  @Test
  @SuppressWarnings("resource")
  public void usedLockInfo() throws Exception {
    ResultSet infoResult = mock(ResultSet.class);
    when(infoResult.next()).thenReturn(true).thenReturn(false);
    when(infoResult.getInt("pid")).thenReturn(123);
    when(infoResult.getString("client_hostname")).thenReturn("some-body.example.net");
    when(infoResult.getTimestamp("backend_start")).thenReturn(new Timestamp(987654));
    when(infoResult.getString("state")).thenReturn("testing");

    PreparedStatement stmt = mock(PreparedStatement.class);
    when(stmt.executeQuery()).thenReturn(infoResult);
    when(dbCon.prepareStatement(PGLockService.SQL_LOCK_INFO)).thenReturn(stmt);

    DatabaseChangeLogLock[] lockList = lockService.listLocks();

    assertThat(lockList).hasSize(1);
    assertThat(lockList[0]).isNotNull();
    assertThat(lockList[0].getLockedBy()).isEqualTo("some-body.example.net (testing)");
    verifyLockParameters(stmt);
  }

  @Test
  @SuppressWarnings("resource")
  public void noLockInfo() throws Exception {
    PreparedStatement stmt = mock(PreparedStatement.class);
    ResultSet infoResult = mock(ResultSet.class);
    when(infoResult.next()).thenReturn(false);
    when(stmt.executeQuery()).thenReturn(infoResult);
    when(dbCon.prepareStatement(PGLockService.SQL_LOCK_INFO)).thenReturn(stmt);

    DatabaseChangeLogLock[] lockList = lockService.listLocks();

    assertThat(lockList).isEmpty();
  }

  @SuppressWarnings("resource")
  private void verifyLockParameters(PreparedStatement stmt) throws SQLException {
    // test_schema.databasechangeloglock
    final int[] lockId = {0xA6BFFB24, 0xF0B205EE};
    verify(stmt).setInt(1, lockId[0]);
    verify(stmt).setInt(2, lockId[1]);
    verify(stmt).executeQuery();
    verify(stmt).close();
    verifyNoMoreInteractions(stmt);
  }

  // Single row / single column
  private static ResultSet booleanResult(Boolean value) throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.next()).thenReturn(true).thenReturn(false);
    when(rs.getObject(1)).thenReturn(value);
    return rs;
  }
}
