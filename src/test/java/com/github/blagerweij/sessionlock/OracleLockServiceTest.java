/*
 * This module, both source code and documentation,
 * is in the Public Domain, and comes with NO WARRANTY.
 */
package com.github.blagerweij.sessionlock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import liquibase.database.core.MariaDBDatabase;
import liquibase.database.core.MySQLDatabase;
import liquibase.database.core.OracleDatabase;
import liquibase.database.core.PostgresDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LockException;
import liquibase.lockservice.DatabaseChangeLogLock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

public class OracleLockServiceTest {

  public static final String LOCK_ID = "1073741824";

  private OracleLockService lockService;

  private Connection dbCon;

  @BeforeEach
  public void setUp() {
    dbCon = mock(Connection.class);

    OracleDatabase database = new OracleDatabase();
    database.setDefaultCatalogName("test_schema");
    database = spy(database);
    when(database.getConnection()).thenReturn(new JdbcConnection(dbCon));

    lockService = new OracleLockService();
    lockService.setDatabase(database);
  }

  @Test
  public void supports() {
    assertThat(lockService.supports(new OracleDatabase())).isTrue();
  }

  @Test
  public void supportsNot() {
    assertThat(lockService.supports(new PostgresDatabase())).isFalse();
    assertThat(lockService.supports(new MySQLDatabase())).isFalse();
    assertThat(lockService.supports(new MariaDBDatabase())).isFalse();
  }

  @Test
  @SuppressWarnings("resource")
  public void acquireSuccess() throws Exception {
    mockAllocateLock(dbCon);

    CallableStatement stmt = mock(CallableStatement.class);
    when(stmt.getInt(1)).thenReturn(0);
    when(dbCon.prepareCall(OracleLockService.SQL_GET_LOCK)).thenReturn(stmt);

    assertThat(lockService.acquireLock()).isTrue();
    InOrder inOrder = inOrder(stmt);
    inOrder.verify(stmt).registerOutParameter(1, Types.INTEGER);
    inOrder.verify(stmt).setString(2, LOCK_ID);
    inOrder.verify(stmt).setInt(3, 6);
    inOrder.verify(stmt).setInt(4, 5);
    inOrder.verify(stmt).executeQuery();
    inOrder.verify(stmt).getInt(1);
    inOrder.verify(stmt).close();
  }

  @Test
  @SuppressWarnings("resource")
  public void acquireUnsuccessful() throws Exception {
    mockAllocateLock(dbCon);

    CallableStatement stmt = mock(CallableStatement.class);
    when(stmt.getInt(1)).thenReturn(1);
    when(dbCon.prepareCall(OracleLockService.SQL_GET_LOCK)).thenReturn(stmt);

    assertThat(lockService.acquireLock()).isFalse();
  }

  @Test
  @SuppressWarnings("resource")
  public void acquireFailure() throws Exception {
    mockAllocateLock(dbCon);

    CallableStatement stmt = mock(CallableStatement.class);
    when(stmt.getInt(1)).thenReturn(2);
    when(dbCon.prepareCall(OracleLockService.SQL_GET_LOCK)).thenReturn(stmt);

    assertThatThrownBy(() -> lockService.acquireLock()).isInstanceOf(LockException.class);
  }

  @Test
  @SuppressWarnings("resource")
  public void releaseSuccess() throws Exception {
    mockAllocateLock(dbCon);

    CallableStatement stmt = mock(CallableStatement.class);
    when(stmt.getInt(1)).thenReturn(0);
    when(dbCon.prepareCall(OracleLockService.SQL_RELEASE_LOCK)).thenReturn(stmt);

    lockService.releaseLock();
    InOrder inOrder = inOrder(stmt);
    inOrder.verify(stmt).registerOutParameter(1, Types.INTEGER);
    inOrder.verify(stmt).setString(2, LOCK_ID);
    inOrder.verify(stmt).executeQuery();
    inOrder.verify(stmt).getInt(1);
    inOrder.verify(stmt).close();
  }

  @Test
  @SuppressWarnings("resource")
  public void releaseFailure() throws Exception {
    mockAllocateLock(dbCon);

    CallableStatement stmt = mock(CallableStatement.class);
    when(stmt.getInt(1)).thenReturn(3);
    when(dbCon.prepareCall(OracleLockService.SQL_RELEASE_LOCK)).thenReturn(stmt);

    assertThatThrownBy(() -> lockService.releaseLock()).isInstanceOf(LockException.class);
  }

  @Test
  @SuppressWarnings("resource")
  public void usedLockInfo() throws Exception {
    mockAllocateLock(dbCon);

    ResultSet infoResult = mock(ResultSet.class);
    when(infoResult.next()).thenReturn(true).thenReturn(false);
    when(infoResult.getInt(1)).thenReturn(123);
    when(infoResult.getTimestamp(2)).thenReturn(new Timestamp(new Date().getTime()));
    when(infoResult.getString(3)).thenReturn("dbuser");
    when(infoResult.getString(4)).thenReturn("osuser");
    when(infoResult.getString(5)).thenReturn("host");

    PreparedStatement stmt = mock(PreparedStatement.class);
    when(stmt.executeQuery()).thenReturn(infoResult);
    when(dbCon.prepareStatement(OracleLockService.SQL_LOCK_INFO)).thenReturn(stmt);

    DatabaseChangeLogLock[] lockList = lockService.listLocks();
    verify(stmt).setInt(1, 1073741824);
    verify(stmt).executeQuery();
    verify(stmt).close();
    assertThat(lockList).hasSize(1);
    assertThat(lockList[0]).isNotNull();
    assertThat(lockList[0].getId()).isEqualTo(Integer.parseInt(LOCK_ID));
    assertThat(lockList[0].getLockGranted()).isInSameDayAs(new Date());
    assertThat(lockList[0].getLockedBy())
        .isEqualTo("(session_id=123)(current_user=dbuser)(os_user=osuser)(host=host)");
    verifyLockParameters(stmt);
  }

  private void verifyLockParameters(PreparedStatement stmt) throws SQLException {
    verify(stmt).executeQuery();
    verify(stmt).close();
    verifyNoMoreInteractions(stmt);
  }

  @Test
  @SuppressWarnings("resource")
  public void noLockInfo() throws Exception {
    mockAllocateLock(dbCon);

    ResultSet infoResult = mock(ResultSet.class);
    when(infoResult.next()).thenReturn(false);
    PreparedStatement stmt = mock(PreparedStatement.class);
    when(stmt.executeQuery()).thenReturn(infoResult);
    when(dbCon.prepareStatement(OracleLockService.SQL_LOCK_INFO)).thenReturn(stmt);

    DatabaseChangeLogLock[] lockList = lockService.listLocks();
    assertThat(lockList).isEmpty();
  }

  private void mockAllocateLock(Connection c) throws SQLException {
    CallableStatement stmtAllocate = mock(CallableStatement.class);
    when(stmtAllocate.getString(2)).thenReturn(LOCK_ID);
    when(c.prepareCall(OracleLockService.SQL_ALLOCATE_LOCK)).thenReturn(stmtAllocate);
  }
}
