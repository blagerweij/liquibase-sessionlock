/*
 * This module, both source code and documentation,
 * is in the Public Domain, and comes with NO WARRANTY.
 */
package com.github.blagerweij.sessionlock;

import liquibase.database.Database;
import liquibase.database.core.MSSQLDatabase;
import liquibase.exception.LockException;
import liquibase.lockservice.DatabaseChangeLogLock;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Locale;

import static com.github.blagerweij.sessionlock.util.StringUtils.toUpperCase;

/**
 * Employs MSSQL application resource locks.
 *
 * <blockquote>
 *
 * <p>A lock obtained with <code>sp_getapplock()</code> is released explicitly by executing <code>
 * sp_releaseapplock()</code> or implicitly when your session terminates (either normally or abnormally).
 * Locks obtained with <code>sp_getapplock(@LockOwner = 'Session')</code> are not released when transactions
 * commit or roll back.
 *
 * </blockquote>
 *
 * @see <a href="https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-getapplock-transact-sql?view=sql-server-ver15"
 *    >Application resource locking function</a> <i>(Microsoft SQL documentation)</i>
 * @see <a href="https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-releaseapplock-transact-sql?view=sql-server-ver15"
 *    >Release applicaiton resource lock function</a> <i>(Microsoft SQL documentation)</i>
 */
public class MSSQLLockService extends SessionLockService {

  static final String SQL_GET_LOCK = "DECLARE @lockResult int;" +
          " EXEC @lockResult = sp_getapplock" +
          "    @Resource = ?," +
          "    @LockMode = 'Exclusive'," +
          "    @LockOwner = 'Session'," +
          "    @LockTimeout = ?;" +
          " SELECT @lockResult;";
  static final String SQL_RELEASE_LOCK = "DECLARE @releaseLockResult int;" +
          "EXEC @releaseLockResult = sp_releaseapplock" +
          "    @Resource = ?," +
          "    @LockOwner = 'Session';" +
          "select @releaseLockResult;";
  static final String SQL_LOCK_INFO =
      "SELECT SP.hostname, SP.login_time, SP.status" +
              "    FROM sys.dm_tran_locks DTL INNER JOIN sys.sysprocesses SP" +
              "       ON DTL.request_session_id = SP.spid" +
              "    WHERE DTL.resource_description like ?";

  @Override
  public boolean supports(Database database) {
    return (database instanceof MSSQLDatabase);
  }

  private String getChangeLogLockName() {
    return toUpperCase(database.getDefaultSchemaName() + "." + database.getDatabaseChangeLogLockTableName());
  }

  private static Integer getIntegerResult(PreparedStatement stmt) throws SQLException {
    try (ResultSet rs = stmt.executeQuery()) {
      rs.next();
      Number locked = (Number) rs.getObject(1);
      return (locked == null) ? null : locked.intValue();
    }
  }

  /**
   * Return code values for <code>sp_getapplock()</code>
   * 0    - The lock was successfully granted synchronously.
   * 1    - The lock was granted successfully after waiting for other incompatible locks to be released.
   * -1   - The lock request timed out.
   * -2   - The lock request was canceled.
   * -3   - The lock request was chosen as a deadlock victim.
   * -999 - Indicates a parameter validation or other call error.
   * In current implementation exception will be thrown only with null and -999 code values.
   * @see <a href="https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-getapplock-transact-sql?view=sql-server-ver15"
   *    >Application resource locking function</a> <i>(Microsoft SQL documentation)</i>
   */
  @Override
  protected boolean acquireLock(final Connection con) throws SQLException, LockException {
    try (final PreparedStatement stmt = con.prepareStatement(SQL_GET_LOCK)) {
      stmt.setString(1, getChangeLogLockName());
      final int timeoutMillis = 5000;
      stmt.setInt(2, timeoutMillis);

      Integer locked = getIntegerResult(stmt);
      if (locked == null) {
        throw new LockException("GET_LOCK() returned NULL");
      } else if (locked == -999) {
        throw new LockException("GET_LOCK() returned " + locked + ". Indicates a parameter validation or other call error.");
      } else if (locked == -1 || locked == -2 || locked == -3) {
        return false;
      }
      return true;
    }
  }

  /**
   * Return code values for <code>sp_getapplock()</code>
   * 0    - Lock was successfully released.
   * -999 - Indicates parameter validation or other call error.
   * @see <a href="https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-releaseapplock-transact-sql?view=sql-server-ver15"
   *    >Release applicaiton resource lock function</a> <i>(Microsoft SQL documentation)</i>
   */
  @Override
  protected void releaseLock(final Connection con) throws SQLException, LockException {
    try (final PreparedStatement stmt = con.prepareStatement(SQL_RELEASE_LOCK)) {
      stmt.setString(1, getChangeLogLockName());

      Integer unlocked = getIntegerResult(stmt);
      if (!Integer.valueOf(0).equals(unlocked)) {
        throw new LockException(
            "RELEASE_LOCK() returned " + toUpperCase(String.valueOf(unlocked)));
      }
    }
  }

  /**
   * Obtains information about the database changelog lock.
   *
   * @see <a href="https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-tran-locks-transact-sql?view=sql-server-ver15"
   *    >The sys.dm_tran_locks table</a> <i>(Microsoft SQL documentation)</i>
   * @see <a href="https://docs.microsoft.com/en-us/sql/relational-databases/system-compatibility-views/sys-sysprocesses-transact-sql?view=sql-server-ver15"
   *    >The sys.sysprocesses table</a> <i>(Microsoft SQL documentation)</i>
   */
  @Override
  protected DatabaseChangeLogLock usedLock(final Connection con) throws SQLException, LockException {
    try (final PreparedStatement stmt = con.prepareStatement(SQL_LOCK_INFO)) {
      stmt.setString(1, "%" + getChangeLogLockName() + "%");

      try (final ResultSet rs = stmt.executeQuery()) {
        if (!rs.next()) {
          return null;
        }

        // This is not really the time the lock has been obtained but gives
        // insight on session login time.
        final Date sessionLoginTime = rs.getDate("login_time");
        return new DatabaseChangeLogLock(1, sessionLoginTime, lockedBy(rs));
      }
    }
  }

  private static String lockedBy(ResultSet rs) throws SQLException {
    final String host = rs.getString("hostname");
    if (host == null) {
      return "system_process_id#" + rs.getString("spid");
    }
    return host + " (" + rs.getString("status") + ")";
  }
}
