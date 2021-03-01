package liquibase.ext;

import com.github.blagerweij.sessionlock.MariaDBLockService;
import com.github.blagerweij.sessionlock.SessionLockService;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;

class MariaDBLiquibaseUpdateTest extends AbstractLiquibaseUpdateTest {
  @Container private static final MariaDBContainer MARIA_DB_CONTAINER = new MariaDBContainer("mariadb");

  @Override
  protected Class<? extends SessionLockService> getExpectedLockServiceClass() {
    return MariaDBLockService.class;
  }

  @Override
  protected JdbcDatabaseContainer getDatabaseContainer() {
    return MARIA_DB_CONTAINER;
  }
}
