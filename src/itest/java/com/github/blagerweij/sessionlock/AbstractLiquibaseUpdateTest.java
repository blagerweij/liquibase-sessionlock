package com.github.blagerweij.sessionlock;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.lockservice.LockService;
import liquibase.lockservice.LockServiceFactory;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
abstract class AbstractLiquibaseUpdateTest {
  private final List<Throwable> exceptions = new ArrayList<>();

  @Test
  void concurrentUpdate() throws Throwable {
    Thread updateThread = startLiquibaseThread("update", new UpdateTask());
    Thread noopThread = startLiquibaseThread("noop", new NoopTask(getExpectedLockServiceClass()));
    updateThread.join();
    noopThread.join();
    if (!exceptions.isEmpty()) {
      throw exceptions.get(0);
    }
  }

  protected abstract Class<? extends SessionLockService> getExpectedLockServiceClass();

  private Thread startLiquibaseThread(String name, LiquibaseConsumer task) {
    Thread th = new Thread(() -> withLiquibase(task), name);
    th.start();
    return th;
  }

  private void withLiquibase(LiquibaseConsumer task) {
    final JdbcDatabaseContainer db = getDatabaseContainer();
    try (Connection connection =
        DriverManager.getConnection(db.getJdbcUrl(), db.getUsername(), db.getPassword())) {
      Liquibase liquibase =
          new Liquibase(
              "liquibase/ext/changelog.xml",
              new ClassLoaderResourceAccessor(),
              new JdbcConnection(connection));
      task.accept(liquibase);
      liquibase.getDatabase().close();
    } catch (AssertionError | Exception e) {
      e.fillInStackTrace();
      e.printStackTrace();
      exceptions.add(e);
    }
  }

  protected abstract JdbcDatabaseContainer getDatabaseContainer();

  @FunctionalInterface
  private interface LiquibaseConsumer {
    void accept(Liquibase liquibase) throws LiquibaseException, InterruptedException;
  }

  static class NoopTask implements LiquibaseConsumer {

    private Class<? extends SessionLockService> expectedLockServiceClass;

    public NoopTask(Class<? extends SessionLockService> expectedLockServiceClass) {
      this.expectedLockServiceClass = expectedLockServiceClass;
    }

    @Override
    public void accept(Liquibase liquibase) throws LiquibaseException, InterruptedException {
      Thread.sleep(1000L); // first wait 1 sec, so update thread runs first
      LockService lockService =
          LockServiceFactory.getInstance().getLockService(liquibase.getDatabase());
      lockService.waitForLock();
      try {
        assertThat(lockService).isInstanceOf(expectedLockServiceClass);
        assertThat(liquibase.getDatabase().getRanChangeSetList()).isNotEmpty();
        assertThat(lockService.listLocks()).hasSize(1);
      } finally {
        lockService.releaseLock();
      }
    }
  }

  static class UpdateTask implements LiquibaseConsumer {
    @Override
    public void accept(Liquibase liquibase) throws LiquibaseException {
      liquibase.update(new Contexts());
      assertThat(liquibase.getDatabase().getRanChangeSetList()).isNotEmpty();
    }
  }
}
