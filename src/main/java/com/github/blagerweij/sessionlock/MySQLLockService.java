/*
 * This module, both source code and documentation,
 * is in the Public Domain, and comes with NO WARRANTY.
 */
package com.github.blagerweij.sessionlock;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Locale;
import liquibase.database.Database;
import liquibase.database.core.MySQLDatabase;
import liquibase.exception.LockException;
import liquibase.lockservice.DatabaseChangeLogLock;

/**
 * Employs MySQL user-level (a.k.a.&#x20;application-level or advisory) locks.
 *
 * <blockquote>
 *
 * <p>A lock obtained with <code>GET_LOCK()</code> is released explicitly by executing <code>
 * RELEASE_LOCK()</code> or implicitly when your session terminates (either normally or abnormally).
 * Locks obtained with <code>GET_LOCK()</code> are not released when transactions commit or roll
 * back.
 *
 * </blockquote>
 *
 * @see "<a href='https://dev.mysql.com/doc/refman/5.7/en/locking-functions.html'>Locking
 *     Functions</a> (MySQL 5.7 Reference Manual)"
 * @see "<a href='https://dev.mysql.com/doc/refman/8.0/en/locking-functions.html'>Locking
 *     Functions</a> (MySQL 8.0 Reference Manual)"
 */
public class MySQLLockService extends SessionLockService {

  static final String SQL_GET_LOCK = "SELECT get_lock(?, ?)";
  static final String SQL_RELEASE_LOCK = "SELECT release_lock(?)";
  static final String SQL_LOCK_INFO =
      "SELECT l.processlist_id, p.host, p.time, p.state"
          + " FROM (SELECT is_used_lock(?) AS processlist_id) AS l"
          + " LEFT JOIN information_schema.processlist p"
          + " ON p.id = l.processlist_id";

  @Override
  public boolean supports(Database database) {
    return (database instanceof MySQLDatabase);
  }

  private String getChangeLogLockName() {
    // MySQL 5.7 and later enforces a maximum length on lock names of 64 characters.
    return (database.getDefaultSchemaName() + "." + database.getDatabaseChangeLogLockTableName())
        .toUpperCase(Locale.ROOT);
  }

  private static Integer getIntegerResult(PreparedStatement stmt) throws SQLException {
    try (ResultSet rs = stmt.executeQuery()) {
      rs.next();
      Number locked = (Number) rs.getObject(1);
      return (locked == null) ? null : locked.intValue();
    }
  }

  /**
   * @see "<a
   *     href='https://dev.mysql.com/doc/refman/5.7/en/locking-functions.html#function_get-lock'>
   *     <code>GET_LOCK</code></a> (Locking Functions)"
   */
  @Override
  protected boolean acquireLock(Connection con) throws SQLException, LockException {
    try (PreparedStatement stmt = con.prepareStatement(SQL_GET_LOCK)) {
      stmt.setString(1, getChangeLogLockName());
      final int timeoutSeconds = 5;
      stmt.setInt(2, timeoutSeconds);

      Integer locked = getIntegerResult(stmt);
      if (locked == null) {
        throw new LockException("GET_LOCK() returned NULL");
      } else if (locked == 0) {
        return false;
      } else if (locked != 1) {
        throw new LockException("GET_LOCK() returned " + locked);
      }
      return true;
    }
  }

  /**
   * @see "<a
   *     href='https://dev.mysql.com/doc/refman/5.7/en/locking-functions.html#function_release-lock'>
   *     <code>RELEASE_LOCK</code></a> (Locking Functions)"
   */
  @Override
  protected void releaseLock(Connection con) throws SQLException, LockException {
    try (PreparedStatement stmt = con.prepareStatement(SQL_RELEASE_LOCK)) {
      stmt.setString(1, getChangeLogLockName());

      Integer unlocked = getIntegerResult(stmt);
      if (!Integer.valueOf(1).equals(unlocked)) {
        throw new LockException(
            "RELEASE_LOCK() returned " + String.valueOf(unlocked).toUpperCase(Locale.ROOT));
      }
    }
  }

  /**
   * Obtains information about the database changelog lock.
   *
   * @see "<a
   *     href='https://dev.mysql.com/doc/refman/5.7/en/locking-functions.html#function_is-used-lock'>
   *     <code>IS_USED_LOCK</code></a> (Locking Functions)"
   * @see "<a href='https://dev.mysql.com/doc/refman/5.7/en/processlist-table.html'>The
   *     INFORMATION_SCHEMA PROCESSLIST Table</a> (MySQL Reference Manual)"
   */
  @Override
  protected DatabaseChangeLogLock usedLock(Connection con) throws SQLException, LockException {
    try (PreparedStatement stmt = con.prepareStatement(SQL_LOCK_INFO)) {
      stmt.setString(1, getChangeLogLockName());

      try (ResultSet rs = stmt.executeQuery()) {
        if (!rs.next() || rs.getObject("PROCESSLIST_ID") == null) {
          return null;
        }

        long timestamp = rs.getInt("TIME");
        if (timestamp > 0) {
          // This is not really the time the lock has been obtained but gives
          // insight on how long the owning session is doing what it is doing.
          timestamp = System.currentTimeMillis() - timestamp * 1000;
        }
        return new DatabaseChangeLogLock(1, new Date(timestamp), lockedBy(rs));
      }
    }
  }

  private static String lockedBy(ResultSet rs) throws SQLException {
    String host = rs.getString("HOST");
    if (host == null) {
      return "connection_id#" + rs.getLong("PROCESSLIST_ID");
    }

    int colonIndex = host.lastIndexOf(':');
    if (colonIndex > 0) {
      host = host.substring(0, colonIndex);
    }
    return host + " (" + rs.getString("STATE") + ")";
  }
}
