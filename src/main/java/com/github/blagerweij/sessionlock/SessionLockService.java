/*
 * This module, both source code and documentation,
 * is in the Public Domain, and comes with NO WARRANTY.
 */
package com.github.blagerweij.sessionlock;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LockException;
import liquibase.lockservice.DatabaseChangeLogLock;
import liquibase.lockservice.StandardLockService;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;

/**
 * Abstract base for {@code LockService} implementations that provide <i>session-level</i>
 * (vs.&#x20;<i>transaction-level</i>) locking. Session-level locks get automatically released if
 * the database connection drops, and overcome the shortcoming of the <a
 * href="https://docs.liquibase.com/concepts/basic/databasechangeloglock-table.html">{@code
 * StandardLockService}</a>:
 *
 * <blockquote>
 *
 * <p>If Liquibase does not exit cleanly, the lock row may be left as locked. You can clear out the
 * current lock by running <code>liquibase releaseLocks</code> which runs <code>
 * UPDATE DATABASECHANGELOGLOCK SET LOCKED=0</code>
 *
 * </blockquote>
 *
 * <p>Running <code>liquibase releaseLocks</code> in a micro-service production environment is not
 * really feasible.
 *
 * <p>Subclasses need to override {@link #supports(Database)}. If {@code listLocks} necessary to
 * provide actual info, {@link #usedLock(Connection)} has to be overridden, also.
 */
public abstract class SessionLockService extends StandardLockService {

  /** This implementation returns {@code super.getPriority() + 1}. */
  @Override
  public int getPriority() {
    // REVISIT: PRIORITY_DATABASE?
    return super.getPriority() + 1;
  }

  /** This implementation returns {@code false}. */
  @Override
  public boolean supports(Database database) {
    return false;
  }

  /**
   * This implementation is a <i>no-op</i>. Suppresses creating the {@code DATABASECHANGELOGLOCK}
   * table by the {@code StandardLockService} implementation.
   */
  @Override
  public void init() throws DatabaseException {
    // no-op
  }

  private Connection getConnection() throws LockException {
    DatabaseConnection dbCon = database.getConnection();
    if (dbCon instanceof JdbcConnection) {
      return ((JdbcConnection) dbCon).getUnderlyingConnection();
    }
    throw new LockException("Not a JdbcConnection: " + dbCon);
  }

  @Override
  public boolean acquireLock() throws LockException {
    if (hasChangeLogLock) {
      return true;
    }

    try {
      if (acquireLock(getConnection())) {
        hasChangeLogLock = true;
        getLog(getClass()).info("Successfully acquired change log lock");
        database.setCanCacheLiquibaseTableInfo(true);
        return true;
      }
      return false;
    } catch (SQLException e) {
      throw new LockException(e);
    }
  }

  /**
   * Attempts to acquire lock for the associated {@link #database} (schema) using the given
   * connection.
   *
   * @param con the connection identifying and used by the database session.
   * @return {@code true} if lock successfully obtained, or {@code false} if lock is held by another
   *     session.
   * @throws SQLException if a database access error occurs;
   * @throws LockException if other logical error happens, preventing the operation from completing
   *     normally.
   * @see #acquireLock()
   */
  protected abstract boolean acquireLock(Connection con) throws SQLException, LockException;

  @Override
  public void releaseLock() throws LockException {
    try {
      releaseLock(getConnection());
      getLog(getClass()).info("Successfully released change log lock");
    } catch (SQLException e) {
      throw new LockException(e);
    } finally {
      hasChangeLogLock = false;
      database.setCanCacheLiquibaseTableInfo(false);
    }
  }

  /**
   * Releases the lock previously obtained by {@code acquireLock()}.
   *
   * @param con the connection identifying and used by the database session.
   * @throws SQLException if a database access error occurs;
   * @throws LockException if other logical error happens, preventing the operation from completing
   *     normally.
   * @see #releaseLock()
   * @see #acquireLock(Connection)
   */
  protected abstract void releaseLock(Connection con) throws SQLException, LockException;

  @Override
  public DatabaseChangeLogLock[] listLocks() throws LockException {
    try {
      DatabaseChangeLogLock usedLock = usedLock(getConnection());
      return (usedLock == null)
          ? new DatabaseChangeLogLock[0]
          : new DatabaseChangeLogLock[] {usedLock};
    } catch (SQLException e) {
      throw new LockException(e);
    }
  }

  /**
   * This implementation returns {@code null}.
   *
   * @param con the connection identifying and used by the database session.
   * @return Information about the database changelog lock, or {@code null}.
   * @throws SQLException if a database access error occurs;
   * @throws LockException if other logical error happens, preventing the operation from completing
   *     normally.
   * @see #listLocks()
   */
  protected DatabaseChangeLogLock usedLock(Connection con) throws SQLException, LockException {
    return null;
  }

  /** Backwards compatibility for Liquibase 3.x */
  private static final LogSupplier LOG_SUPPLIER;

  static {
    LogSupplier logSupplier = null;
    try {
      final Class<?> scopeClass = Class.forName("liquibase.Scope"); // since 3.8.0
      final Method getCurrentScope = scopeClass.getMethod("getCurrentScope");
      Object scope = getCurrentScope.invoke(null);
      final Method getLog = scope.getClass().getMethod("getLog", Class.class);
      logSupplier = c -> (Logger) getLog.invoke(scope, c);
    } catch (NoSuchMethodException
        | ClassNotFoundException
        | IllegalAccessException
        | InvocationTargetException ignored) {
      try {
        final Class<?> logServiceClass = Class.forName("liquibase.logging.LogService");
        final Method getLog = logServiceClass.getMethod("getLog", Class.class);
        logSupplier = c -> (Logger) getLog.invoke(null, c);
      } catch (NoSuchMethodException | ClassNotFoundException e) {
        logSupplier = c -> LogFactory.getLogger();
      }
    }
    LOG_SUPPLIER = logSupplier;
  }

  protected static Logger getLog(Class<?> clazz) {
    try {
      return LOG_SUPPLIER.getLog(clazz);
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  interface LogSupplier {
    Logger getLog(Class<?> clazz) throws InvocationTargetException, IllegalAccessException;
  }
}
