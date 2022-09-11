package com.github.blagerweij.sessionlock;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.Test;
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

  @Test
  void dropAllShouldWork() throws SQLException, LiquibaseException {
    final JdbcDatabaseContainer db = getDatabaseContainer();
    try (Connection connection =
        DriverManager.getConnection(db.getJdbcUrl(), db.getUsername(), db.getPassword())) {
      Liquibase liquibase =
          new Liquibase(
              "liquibase/ext/changelog.xml",
              new ClassLoaderResourceAccessor(),
              new JdbcConnection(connection));
      liquibase.dropAll();
      liquibase.update(new Contexts());
      liquibase.dropAll();
      liquibase.update(new Contexts());
      liquibase.getDatabase().close();
    }
  }
}
