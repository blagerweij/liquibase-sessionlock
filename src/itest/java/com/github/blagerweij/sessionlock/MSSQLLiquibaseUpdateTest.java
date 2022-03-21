package com.github.blagerweij.sessionlock;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;

class MSSQLLiquibaseUpdateTest extends AbstractLiquibaseUpdateTest {
  @Container private static final MSSQLServerContainer MS_SQL_CONTAINER = new MSSQLServerContainer(MSSQLServerContainer.IMAGE).acceptLicense();

  @Override
  protected Class<? extends SessionLockService> getExpectedLockServiceClass() {
    return MSSQLLockService.class;
  }

  @Override
  protected JdbcDatabaseContainer getDatabaseContainer() {
    return MS_SQL_CONTAINER;
  }
}
