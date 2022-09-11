/*
 * This module, both source code and documentation,
 * is in the Public Domain, and comes with NO WARRANTY.
 */
package com.github.blagerweij.sessionlock;

import liquibase.database.Database;
import liquibase.database.core.H2Database;
import liquibase.exception.LockException;
import liquibase.lockservice.DatabaseChangeLogLock;

import java.sql.*;

/**
 * Employs PostgreSQL <i>advisory locks</i>.
 *
 * <blockquote>
 *
 * <p>While a flag stored in a table could be used for the same purpose, advisory locks are faster,
 * avoid table bloat, and are automatically cleaned up by the server at the end of the session.
 *
 * <p>There are two ways to acquire an advisory lock in PostgreSQL: at session level or at
 * transaction level. Once acquired at session level, an advisory lock is held until explicitly
 * released or the session ends. Unlike standard lock requests, session-level advisory lock requests
 * do not honor transaction semantics: a lock acquired during a transaction that is later rolled
 * back will still be held following the rollback, and likewise an unlock is effective even if the
 * calling transaction fails later.
 *
 * </blockquote>
 *
 * @see "<a href='https://www.postgresql.org/docs/9.6/explicit-locking.html#ADVISORY-LOCKS'>Advisory
 *     Locks</a> (PostgreSQL Documentation)"
 */
public class H2LockService extends SessionLockService {

  static final String SQL_TRY_LOCK = "SET EXCLUSIVE 1";
  static final String SQL_UNLOCK = "SET EXCLUSIVE 0";

  @Override
  public boolean supports(Database database) {
    return (database instanceof H2Database);
  }


  private int[] getChangeLogLockId() throws LockException {
    String defaultSchemaName = database.getDefaultSchemaName();
    if (defaultSchemaName == null) {
      throw new LockException("Default schema name is not set for current DB user/connection");
    }
    // Unlike the general Object.hashCode() contract,
    // String.hashCode() should be stable across VM instances and Java versions.
    return new int[] {
      database.getDatabaseChangeLogLockTableName().hashCode(),
      defaultSchemaName.hashCode()
    };
  }


  /**
   * @see "<a
   *     href='https://www.h2database.com/html/commands.html#set_exclusive'>
   */
  @Override
  protected boolean acquireLock(Connection con) throws SQLException, LockException {
    try (PreparedStatement stmt = con.prepareStatement(SQL_TRY_LOCK)) {
      stmt.setQueryTimeout(10);
      try{
        int res = stmt.executeUpdate();
        return res == 0;
      } catch (SQLTimeoutException ex ){
        return false;
      } catch (SQLException ex){
        ex.printStackTrace();
        return false;
      }
    }
  }

  /**
   * @see "<a
   *     href='https://www.h2database.com/html/commands.html#set_exclusive'>
   */
  @Override
  protected void releaseLock(Connection con) throws SQLException, LockException {
    try (PreparedStatement stmt = con.prepareStatement(SQL_UNLOCK)) {

      Boolean unlocked = stmt.executeUpdate() == 0;
      if (!Boolean.TRUE.equals(unlocked)) {
        throw new LockException("pg_advisory_unlock() returned " + unlocked);
      }
    }
  }

  /**
   * Obtains information about the database changelog lock.
   *
   * <blockquote>
   *
   * Like all locks in PostgreSQL, a complete list of advisory locks currently held by any session
   * can be found in the <code>pg_locks</code> system view.
   *
   * </blockquote>
   *
   * @see "<a href='https://www.postgresql.org/docs/9.6/view-pg-locks.html'><code>pg_locks</code>
   *     </a> (PostgreSQL Documentation)"
   * @see "<a
   *     href='https://www.postgresql.org/docs/9.6/monitoring-stats.html#PG-STAT-ACTIVITY-VIEW'>
   *     <code>pg_stat_activity</code> View</a> (PostgreSQL Documentation)"
   */
  @Override
  protected DatabaseChangeLogLock usedLock(Connection con) throws SQLException, LockException {
    throw new RuntimeException("Unsupported");
  }

  private static String lockedBy(ResultSet rs) throws SQLException {
    String host = rs.getString("client_hostname");
    if (host == null) {
      return "pid#" + rs.getInt("pid");
    }
    return host + " (" + rs.getString("state") + ")";
  }
}
