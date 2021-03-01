/*
 * This module, both source code and documentation,
 * is in the Public Domain, and comes with NO WARRANTY.
 */
package com.github.blagerweij.sessionlock;

import java.sql.Connection;
import java.sql.SQLException;

import liquibase.database.Database;
import liquibase.database.core.MariaDBDatabase;
import liquibase.exception.LockException;
import liquibase.lockservice.DatabaseChangeLogLock;

/**
 * Employs MariaDB user-level (a.k.a.&#x20;application-level or advisory) locks.
 *
 * See {@link MySQLLockService} for more details.
 *
 * @see "<a href='https://mariadb.com/kb/en/miscellaneous-functions/'>Locking
 *     Functions</a> (MariaDB Reference Manual)"
 */
public class MariaDBLockService extends MySQLLockService {

  @Override
  public boolean supports(Database database) {
    return (database instanceof MariaDBDatabase);
  }

  /**
   * @see "<a
   *     href='https://mariadb.com/kb/en/get_lock/'>
   *     <code>GET_LOCK</code></a> (MariaDB Documentation)"
   */
  @Override
  protected boolean acquireLock(Connection con) throws SQLException, LockException {
    return super.acquireLock(con);
  }

  /**
   * @see "<a
   *     href='https://mariadb.com/kb/en/release_lock/'>
   *     <code>RELEASE_LOCK</code></a> (MariaDB Documentation)"
   */
  @Override
  protected void releaseLock(Connection con) throws SQLException, LockException {
    super.releaseLock(con);
  }

  /**
   * Obtains information about the database changelog lock.
   *
   * @see "<a
   *     href='https://mariadb.com/kb/en/is_used_lock/'>
   *     <code>IS_USED_LOCK</code></a> (MariaDB Documentation)"
   * @see "<a href='https://mariadb.com/kb/en/information-schema-processlist-table/'>The
   *     INFORMATION_SCHEMA PROCESSLIST Table</a> (MariaDB Documentation)"
   */
  @Override
  protected DatabaseChangeLogLock usedLock(Connection con) throws SQLException, LockException {
    return super.usedLock(con);
  }

}
