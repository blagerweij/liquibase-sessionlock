/*
 * This module, both source code and documentation,
 * is in the Public Domain, and comes with NO WARRANTY.
 */
package com.github.blagerweij.sessionlock;

import static liquibase.servicelocator.PrioritizedService.PRIORITY_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import liquibase.database.Database;
import liquibase.database.core.MySQLDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LockException;
import liquibase.lockservice.DatabaseChangeLogLock;
import liquibase.lockservice.StandardLockService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SessionLockServiceTest {

  private SessionLockService lockService;

  private Database database;

  @BeforeEach
  @SuppressWarnings("resource")
  public void setUp() {
    database = mock(Database.class);
    when(database.getConnection()).thenReturn(new JdbcConnection(mock(Connection.class)));

    MockService service = new MockService();
    service.setDatabase(database);
    lockService = service;
  }

  @Test
  public void noOpInit() throws Exception {
    lockService.init();
    verifyNoInteractions(database);
  }

  @Test
  @SuppressWarnings("resource")
  public void defaultUsedLock() throws LockException, SQLException {
    SessionLockService service =
        new SessionLockService() {
          @Override
          protected boolean acquireLock(Connection con) {
            throw new UnsupportedOperationException();
          }

          @Override
          protected void releaseLock(Connection con) {
            throw new UnsupportedOperationException();
          }
        };
    assertThat(service.usedLock(mock(Connection.class))).isNull();
  }

  @Test
  public void priorityAboveDefault() {
    assertThat(lockService.getPriority()).isGreaterThan(PRIORITY_DEFAULT);
    assertThat(lockService.getPriority()).isGreaterThan(new StandardLockService().getPriority());
  }

  @Test
  public void defaultSupports() throws Exception {
    assertThat(lockService.supports(new MySQLDatabase())).isFalse();
  }

  @Test
  public void acquireReleaseState() throws Exception {
    assertThat(lockService.acquireLock()).isTrue();
    assertThat(lockService.hasChangeLogLock()).isTrue();
    lockService.releaseLock();
    assertThat(lockService.hasChangeLogLock()).isFalse();
  }

  @Test
  public void acquireBeforeRelease() throws Exception {
    assertThat(lockService.acquireLock()).isTrue();
    assertThat(lockService.acquireLock()).isTrue();
    assertThat(lockService.hasChangeLogLock()).isTrue();
  }

  @Test
  public void acquireFalse() throws Exception {
    ((MockService) lockService).acquireResult = false;
    assertThat(lockService.acquireLock()).isFalse();
    assertThat(lockService.hasChangeLogLock()).isFalse();
  }

  @Test
  public void listLocks() throws Exception {
    ((MockService) lockService).usedLockResult = new DatabaseChangeLogLock(2, new Date(1), "foo");
    DatabaseChangeLogLock[] locks = lockService.listLocks();
    assertThat(locks).hasSize(1);
    assertThat(locks[0]).isNotNull();
    assertThat(locks[0].getLockedBy()).isEqualTo("foo");
  }

  @Test
  public void listNoLocks() throws Exception {
    DatabaseChangeLogLock[] locks = lockService.listLocks();
    assertThat(locks).isEmpty();
  }

  @Test
  @SuppressWarnings("resource")
  public void acquireShouldPropagate() throws Exception {
    SessionLockService spyService = spy(lockService);
    SQLException cause = new SQLException("bar");
    doThrow(cause).when(spyService).acquireLock(any(Connection.class));
    assertThatThrownBy(() -> spyService.acquireLock())
        .isInstanceOf(LockException.class)
        .hasCause(cause);
  }

  @Test
  @SuppressWarnings("resource")
  public void releaseShouldPropagate() throws Exception {
    SessionLockService spyService = spy(lockService);
    SQLException cause = new SQLException("baz");
    doThrow(cause).when(spyService).releaseLock(any(Connection.class));
    assertThatThrownBy(() -> spyService.releaseLock())
        .isInstanceOf(LockException.class)
        .hasCause(cause);
  }

  @Test
  @SuppressWarnings("resource")
  public void listLocksShouldPropagate() throws Exception {
    SessionLockService spyService = spy(lockService);
    SQLException cause = new SQLException("qux");
    doThrow(cause).when(spyService).usedLock(any(Connection.class));
    assertThatThrownBy(() -> spyService.listLocks())
        .isInstanceOf(LockException.class)
        .hasCause(cause);
  }

  static class MockService extends SessionLockService {

    boolean acquireResult = true;
    DatabaseChangeLogLock usedLockResult;

    @Override
    protected boolean acquireLock(Connection con) throws SQLException, LockException {
      return acquireResult;
    }

    @Override
    protected void releaseLock(Connection con) throws SQLException, LockException {
      // no-op
    }

    @Override
    protected DatabaseChangeLogLock usedLock(Connection con) throws SQLException, LockException {
      return usedLockResult;
    }
  }
}
