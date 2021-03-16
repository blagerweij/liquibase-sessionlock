package com.github.blagerweij.sessionlock;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;

class MariaDBLiquibaseUpdateTest extends AbstractLiquibaseUpdateTest {
  @Container
  private static final MariaDBContainer MARIA_DB_CONTAINER = new MariaDBContainer("mariadb");

  @Override
  protected Class<? extends SessionLockService> getExpectedLockServiceClass() {
    return MySQLLockService.class;
  }

  @Override
  protected JdbcDatabaseContainer getDatabaseContainer() {
    return MARIA_DB_CONTAINER;
  }
}
