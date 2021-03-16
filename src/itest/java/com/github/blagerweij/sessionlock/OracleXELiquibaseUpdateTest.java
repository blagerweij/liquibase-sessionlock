package com.github.blagerweij.sessionlock;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;

class OracleXELiquibaseUpdateTest extends AbstractLiquibaseUpdateTest {
  public OracleXELiquibaseUpdateTest() {
    // Somehow Timezone UTC is not recognized in Github Actions
    System.setProperty("oracle.jdbc.timezoneAsRegion", "false");
  }

  @Container
  private static final OracleContainer ORACLE_DB_CONTAINER =
      new OracleContainer("oracleinanutshell/oracle-xe-11g");

  @Override
  protected Class<? extends SessionLockService> getExpectedLockServiceClass() {
    return OracleLockService.class;
  }

  @Override
  protected JdbcDatabaseContainer getDatabaseContainer() {
    return ORACLE_DB_CONTAINER;
  }
}
