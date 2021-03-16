package com.github.blagerweij.sessionlock;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;

class OracleXELiquibaseUpdateTest extends AbstractLiquibaseUpdateTest {
  public OracleXELiquibaseUpdateTest() {
    // Workaround for Timezone problem (see
    // https://github.com/testcontainers/testcontainers-java/issues/2313)
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
