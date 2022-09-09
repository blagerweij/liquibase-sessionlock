/*
 * This module, both source code and documentation,
 * is in the Public Domain, and comes with NO WARRANTY.
 */
package com.github.blagerweij.sessionlock;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.Date;
import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LockException;
import liquibase.lockservice.DatabaseChangeLogLock;
import liquibase.lockservice.LockService;
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
 * current lock by running <code>liquibase releaseLocks</code> which runs <code> UPDATE
 * DATABASECHANGELOGLOCK SET LOCKED=0</code>
 *
 * </blockquote>
 *
 * <p>Running <code>liquibase releaseLocks</code> in a micro-service production environment is not
 * really feasible.
 *
 * <p>Subclasses need to override {@link #supports(Database)}. If {@code listLocks} necessary to
 * provide actual info, {@link #usedLock(Connection)} has to be overridden, also.
 */
public abstract class SessionLockService implements LockService {

  protected long changeLogLockWaitTime = 5; // max wait time in minutes
  protected long changeLogLockRecheckTime = 5; // recheck interval in seconds
  protected boolean hasChangeLogLock;
  protected Database database;

  /** This implementation returns {@code super.getPriority() + 1}. */
  @Override
  public int getPriority() {
    return PRIORITY_DEFAULT + 1;
  }

  @Override
  public void setChangeLogLockWaitTime(long changeLogLockWaitTime) {
    this.changeLogLockWaitTime = changeLogLockWaitTime;
  }

  @Override
  public void setChangeLogLockRecheckTime(long changeLogLocRecheckTime) {
    this.changeLogLockRecheckTime = changeLogLocRecheckTime;
  }

  /** This implementation returns {@code false}. */
  @Override
  public boolean supports(Database database) {
    return false;
  }

  @Override
  public void waitForLock() throws LockException {
    boolean locked = false;
    final long timeToGiveUp = new Date().getTime() + (changeLogLockWaitTime * 1000 * 60);
    while (!locked && (new Date().getTime() < timeToGiveUp)) {
      locked = acquireLock();
      if (!locked) {
        getLog(getClass()).info("Waiting for changelog lock....");
        try {
          Thread.sleep(changeLogLockRecheckTime * 1000);
        } catch (InterruptedException e) {
          // Restore thread interrupt status
          Thread.currentThread().interrupt();
        }
      }
    }

    if (!locked) {
      DatabaseChangeLogLock[] locks = listLocks();
      String lockedBy;
      if (locks.length > 0) {
        DatabaseChangeLogLock lock = locks[0];
        lockedBy =
            lock.getLockedBy()
                + " since "
                + DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT)
                    .format(lock.getLockGranted());
      } else {
        lockedBy = "UNKNOWN";
      }
      throw new LockException(
          "Could not acquire change log lock.  Currently locked by " + lockedBy);
    }
  }

  @Override
  public void forceReleaseLock() throws LockException {
    this.init();
    releaseLock();
  }

  @Override
  public void reset() {
    hasChangeLogLock = false;
  }

  @Override
  public void destroy() {
    reset();
  }

  @Override
  public void setDatabase(Database database) {
    this.database = database;
  }

  @Override
  public void init() {
    this.hasChangeLogLock = false;
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

  @Override
  public boolean hasChangeLogLock() {
    return hasChangeLogLock;
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
