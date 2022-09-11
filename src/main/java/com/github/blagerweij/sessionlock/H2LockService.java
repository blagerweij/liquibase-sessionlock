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
import java.util.Date;

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
        throw new LockException(SQL_UNLOCK + " returned " + unlocked);
      }
    }
  }

  /**
   * Obtains information about the database changelog lock.
   *
   */
  @Override
  protected DatabaseChangeLogLock usedLock(Connection con) throws SQLException, LockException {
    // Sadly there is no way to determine the actual Lockdata from the connection
    return new DatabaseChangeLogLock(1,new Date(),"liquibase");
  }

  private static String lockedBy(ResultSet rs) throws SQLException {
    String host = rs.getString("client_hostname");
    if (host == null) {
      return "pid#" + rs.getInt("pid");
    }
    return host + " (" + rs.getString("state") + ")";
  }
}
