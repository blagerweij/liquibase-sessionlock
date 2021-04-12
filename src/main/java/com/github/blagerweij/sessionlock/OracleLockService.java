/*
 * This module, both source code and documentation,
 * is in the Public Domain, and comes with NO WARRANTY.
 */
package com.github.blagerweij.sessionlock;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Locale;
import liquibase.database.Database;
import liquibase.database.core.OracleDatabase;
import liquibase.exception.LockException;
import liquibase.lockservice.DatabaseChangeLogLock;

/**
 * Employs Oracle user-level (a.k.a.&#x20;application-level or advisory) locks.
 *
 * <p>See {@link MySQLLockService} for a very similar implementation.
 *
 * @see "<a
 *     href='https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_LOCK.html'>
 *     <code>DBMS_LOCK</code> Package</a> (Oracle PL/SQL Reference Manual)"
 */
public class OracleLockService extends SessionLockService {

  static final String SQL_ALLOCATE_LOCK = "{ call dbms_lock.allocate_unique(?, ?) }";
  static final String SQL_GET_LOCK = "{ ? = call dbms_lock.request(?, ?, ?) }";
  static final String SQL_RELEASE_LOCK = "{ ? = call dbms_lock.release(?) }";
  static final String SQL_LOCK_INFO =
      "select l.sid, current_timestamp - numToDSInterval(l.ctime,'second'), s.USERNAME, s.OSUSER,"
          + " s.MACHINE from v$lock l join v$session s on l.sid = s.SID where l.type = 'UL'   and"
          + " l.id1 = ?";

  @Override
  public boolean supports(Database database) {
    return (database instanceof OracleDatabase);
  }

  private String getChangeLogLockName() {
    return (database.getDefaultSchemaName() + "." + database.getDatabaseChangeLogLockTableName())
        .toUpperCase(Locale.ROOT);
  }

  /**
   * @see "<a
   *     href='https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_LOCK.html#GUID-73F03FA6-04B3-4341-AB4F-8BECCF898D13'>
   *     <code>DBMS_LOCK.ALLOCATE_UNIQUE</code> Procedure</a> (Oracle PL/SQL Reference Manual)"
   */
  private String allocateLock(Connection con) throws SQLException {
    // Allocate lock
    try (CallableStatement stmt = con.prepareCall(SQL_ALLOCATE_LOCK)) {
      stmt.setString(1, getChangeLogLockName());
      stmt.registerOutParameter(2, Types.VARCHAR);
      stmt.executeQuery();

      return stmt.getString(2);
    }
  }

  /**
   * @see "<a
   *     href='https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_LOCK.html#GUID-CC3AEC00-CBFF-45DD-99C3-C7A312C0213E'>
   *     <code>DBMS_LOCK.REQUEST</code> Procedure</a> (Oracle PL/SQL Reference Manual)"
   */
  @Override
  protected boolean acquireLock(Connection con) throws SQLException, LockException {
    String lockHandle = allocateLock(con);
    try (CallableStatement stmt = con.prepareCall(SQL_GET_LOCK)) {
      stmt.registerOutParameter(1, Types.INTEGER);
      stmt.setString(2, lockHandle);
      stmt.setInt(3, 6); // X_MODE -> Exclusive lock mode
      final int timeoutSeconds = 5;
      stmt.setInt(4, timeoutSeconds);
      stmt.executeQuery();

      final int rc = stmt.getInt(1);
      switch (rc) {
        case 0:
          return true;
        case 1:
          return false;
        case 2:
          throw new LockException("deadlock");
        case 3:
          throw new LockException("parameter error");
        case 4:
          throw new LockException("already own lock");
        case 5:
          throw new LockException("illegal lock handle");
        default:
          throw new LockException("uknown error: " + rc);
      }
    }
  }

  /**
   * @see "<a
   *     href='https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_LOCK.html#GUID-1007B402-15D3-4447-9FF3-219DF113A47B'>
   *     <code>DBMS_LOCK.RELEASE</code> Procedure</a> (Oracle PL/SQL Reference Manual)"
   */
  @Override
  protected void releaseLock(Connection con) throws SQLException, LockException {
    String lockHandle = allocateLock(con);
    try (CallableStatement stmt = con.prepareCall(SQL_RELEASE_LOCK)) {
      stmt.registerOutParameter(1, Types.INTEGER);
      stmt.setString(2, lockHandle);
      stmt.executeQuery();

      int rc = stmt.getInt(1);
      switch (rc) {
        case 0:
          return;
        case 3:
          throw new LockException("parameter error");
        case 4:
          throw new LockException("does not own lock");
        case 5:
          throw new LockException("illegal lock handle");
        default:
          throw new LockException("uknown error: " + rc);
      }
    }
  }

  /**
   * Obtains information about the database changelog lock. <br>
   * Oracle does not provide a way to retrieve lock details (e.g. who owns a lock) without elevated
   * privileges. However, it's possible to get some information about the current session, so that's
   * what gets returned by this method.<br>
   * You can use the following query to get lock details:
   *
   * <pre><code>
   * SELECT LOCKS_ALLOCATED.*, LOCKS.*
   * FROM DBA_LOCKS LOCKS, SYS.DBMS_LOCK_ALLOCATED LOCKS_ALLOCATED
   * WHERE LOCKS.LOCK_ID1 = LOCKS_ALLOCATED.LOCKID
   * AND LOCKS_ALLOCATED.NAME = 'lockName';
   * </code></pre>
   *
   * @see "<a
   *     href='https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/DBMS_LOCK_ALLOCATED.html'>The
   *     DBMS_LOCK_ALLOCATED Table</a> (Oracle PL/SQL Reference Manual)"
   * @see "<a
   *     href='https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/DBA_LOCK.html'>The
   *     DBA_LOCK View</a> (Oracle PL/SQL Reference Manual)"
   */
  @Override
  protected DatabaseChangeLogLock usedLock(Connection con) throws SQLException {
    String lockHandle = allocateLock(con);
    // lock handles are integers between 1073741824 and 1999999999 but they are returned as longer
    // value here
    if (lockHandle.length() >= 10) {
      try {
        int lockId = Integer.parseInt(lockHandle.substring(0, 10));
        try (PreparedStatement stmt = con.prepareStatement(SQL_LOCK_INFO)) {
          stmt.setInt(1, lockId);
          try (ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
              return new DatabaseChangeLogLock(lockId, rs.getTimestamp(2), lockedBy(rs));
            }
          }
        }
      } catch (NumberFormatException e) {
        getLog(getClass()).warning("could not parse lock handle " + lockHandle, e);
      }
    }
    return null;
  }

  private String lockedBy(ResultSet rs) throws SQLException {
    return String.format(
        "(session_id=%d)(current_user=%s)(os_user=%s)(host=%s)",
        rs.getInt(1), rs.getString(3), rs.getString(4), rs.getString(5));
  }
}
