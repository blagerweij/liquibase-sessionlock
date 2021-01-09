package liquibase.ext;

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
  private List<Throwable> exceptions = new ArrayList<>();

  @Test
  void concurrentUpdate() throws Exception {
    Thread updateThread = startLiquibaseThread("update", new Updatetask());
    Thread noopThread = startLiquibaseThread("noop", new NoopTask());
    updateThread.join();
    noopThread.join();
    assertThat(exceptions).isEmpty();
  }

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
    } catch (Exception e) {
      exceptions.add(e);
    }
  }

  protected abstract JdbcDatabaseContainer getDatabaseContainer();

  @FunctionalInterface
  private interface LiquibaseConsumer {
    void accept(Liquibase liquibase) throws LiquibaseException, InterruptedException;
  }

  static class NoopTask implements LiquibaseConsumer {
    @Override
    public void accept(Liquibase liquibase) throws LiquibaseException, InterruptedException {
      Thread.sleep(1000L); // first wait 1 sec, so update thread runs first
      LockService lockService =
          LockServiceFactory.getInstance().getLockService(liquibase.getDatabase());
      lockService.waitForLock();
      try {
        assertThat(liquibase.getDatabase().getRanChangeSetList()).isNotEmpty();
      } finally {
        lockService.releaseLock();
      }
    }
  }

  static class Updatetask implements LiquibaseConsumer {
    @Override
    public void accept(Liquibase liquibase) throws LiquibaseException {
      liquibase.update(new Contexts());
      assertThat(liquibase.getDatabase().getRanChangeSetList()).isNotEmpty();
    }
  }
}
