package com.github.blagerweij.sessionlock;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;

class MySQLLiquibaseUpdateTest extends AbstractLiquibaseUpdateTest {
  @Container private static final MySQLContainer MY_SQL_CONTAINER = new MySQLContainer("mysql");

  @Override
  protected Class<? extends SessionLockService> getExpectedLockServiceClass() {
    return MySQLLockService.class;
  }

  @Override
  protected JdbcDatabaseContainer getDatabaseContainer() {
    return MY_SQL_CONTAINER;
  }
}
