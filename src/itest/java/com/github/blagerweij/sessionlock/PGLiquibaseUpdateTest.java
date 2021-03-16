package com.github.blagerweij.sessionlock;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;

class PGLiquibaseUpdateTest extends AbstractLiquibaseUpdateTest {
  @Container
  private static final PostgreSQLContainer POSTGRES_SQL_CONTAINER =
      new PostgreSQLContainer("postgres");

  @Override
  protected Class<? extends SessionLockService> getExpectedLockServiceClass() {
    return PGLockService.class;
  }

  @Override
  protected JdbcDatabaseContainer getDatabaseContainer() {
    return POSTGRES_SQL_CONTAINER;
  }
}
