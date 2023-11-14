/*
 * This module, both source code and documentation,
 * is in the Public Domain, and comes with NO WARRANTY.
 */
package com.github.blagerweij.sessionlock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import liquibase.database.core.MySQLDatabase;
import liquibase.database.core.PostgresDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LockException;
import liquibase.lockservice.DatabaseChangeLogLock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class MySQLLockServiceTest {

  private MySQLLockService lockService;

  private Connection dbCon;
  private MySQLDatabase database;

    @BeforeEach
  public void setUp() {
    dbCon = mock(Connection.class);

    database = new MySQLDatabase();
    database.setDefaultCatalogName("test_schema");
    database = spy(database);
    when(database.getConnection()).thenReturn(new JdbcConnection(dbCon));

    lockService = new MySQLLockService();
    lockService.setDatabase(database);
  }

  @Test
  public void supports() throws Exception {
    assertThat(lockService.supports(new MySQLDatabase())).isTrue();
  }

  @Test
  public void supportsNot() throws Exception {
    assertThat(lockService.supports(new PostgresDatabase())).isFalse();
  }

  @Test
  @SuppressWarnings("resource")
  public void acquireSuccess() throws Exception {
    PreparedStatement stmt = mock(PreparedStatement.class);
    ResultSet rs = intResult(1);
    when(stmt.executeQuery()).thenReturn(rs);
    when(dbCon.prepareStatement(MySQLLockService.SQL_GET_LOCK)).thenReturn(stmt);

    assertThat(lockService.acquireLock()).isTrue();
    verify(stmt).setString(1, "TEST_SCHEMA.DATABASECHANGELOGLOCK");
  }

  @Test
  @SuppressWarnings("resource")
  public void acquireUnsuccessful() throws Exception {
    PreparedStatement stmt = mock(PreparedStatement.class);
    ResultSet rs = intResult(0);
    when(stmt.executeQuery()).thenReturn(rs);
    when(dbCon.prepareStatement(MySQLLockService.SQL_GET_LOCK)).thenReturn(stmt);

    assertThat(lockService.acquireLock()).isFalse();
  }

  @Test
  @SuppressWarnings("resource")
  public void acquireFailure() throws Exception {
    PreparedStatement stmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(stmt.executeQuery()).thenReturn(rs);
    when(dbCon.prepareStatement(MySQLLockService.SQL_GET_LOCK)).thenReturn(stmt);

    assertThatThrownBy(() -> lockService.acquireLock()).isInstanceOf(LockException.class);
  }

    @Test
    @SuppressWarnings("resource")
    public void lockNameTooLongShouldFail() throws Exception {
      database.setDefaultCatalogName("this_name_is_too_long_and_should_be_truncated_since_mysql_does_not_like_long_locknames");
      PreparedStatement stmt = mock(PreparedStatement.class);
      ResultSet rs = mock(ResultSet.class);
      when(stmt.executeQuery()).thenReturn(rs);
      when(dbCon.prepareStatement(MySQLLockService.SQL_GET_LOCK)).thenReturn(stmt);
      ArgumentCaptor<String> lockNameCaptor = ArgumentCaptor.forClass(String.class);
      doNothing().when(stmt).setString(eq(1), lockNameCaptor.capture());
      when(rs.next()).thenReturn(true, false);
      when(rs.getObject(1)).thenReturn(1);
      assertThat(lockService.acquireLock()).isTrue();
      assertThat(lockNameCaptor.getValue()).isEqualTo("THIS_NAME_IS_TOO_LONG_AND_SHOULD_BE_TRUNCATED_SINCE_MYSQL_DOES_N");
    }

  @Test
  @SuppressWarnings("resource")
  public void releaseSuccess() throws Exception {
    lockService.hasChangeLogLock = true;

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
    lockService.hasChangeLogLock = true;

    PreparedStatement stmt = mock(PreparedStatement.class);
    ResultSet rs = intResult(0);
    when(stmt.executeQuery()).thenReturn(rs);
    when(dbCon.prepareStatement(MySQLLockService.SQL_RELEASE_LOCK)).thenReturn(stmt);

    assertThatThrownBy(() -> lockService.releaseLock()).isInstanceOf(LockException.class);
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
    assertThat(lockList).hasSize(1);
    assertThat(lockList[0]).isNotNull();
    assertThat(lockList[0].getLockedBy()).isEqualTo("192.168.254.254 (testing)");
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
    assertThat(lockList).isEmpty();
  }

  // Single row / single column
  private static ResultSet intResult(int value) throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.next()).thenReturn(true).thenReturn(false);
    when(rs.getObject(1)).thenReturn(value);
    return rs;
  }
}
