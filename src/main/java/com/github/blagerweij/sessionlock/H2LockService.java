/*
 * This module, both source code and documentation,
 * is in the Public Domain, and comes with NO WARRANTY.
 */
package com.github.blagerweij.sessionlock;

import liquibase.database.Database;
import liquibase.database.core.H2Database;
import liquibase.exception.LockException;
import liquibase.lockservice.DatabaseChangeLogLock;
import liquibase.repackaged.org.apache.commons.lang3.NotImplementedException;

import java.sql.*;
import java.util.Date;

public class H2LockService extends SessionLockService {

  private enum LockState {
    NONE, FORBIDDEN, LOCKED
  }

  static final String SQL_CHECK_LOCK = "SELECT TABLE_SCHEMA, TABLE_NAME, SESSION_ID, LOCK_TYPE FROM INFORMATION_SCHEMA.LOCKS WHERE TABLE_SCHEMA = ? AND TABLE_NAME= ? AND SESSION_ID != ? AND LOCK_TYPE = ?";

  static final String SQL_TRY_LOCK = "INSERT INTO INFORMATION_SCHEMA.LOCKS (TABLE_SCHEMA, TABLE_NAME, SESSION_ID, LOCK_TYPE) VALUES (?,?,?,?)";
  static final String SQL_UNLOCK = "DELETE FROM INFORMATION_SCHEMA.LOCKS WHERE TABLE_SCHEMA = ? AND TABLE_NAME= ? AND SESSION_ID = ? AND LOCK_TYPE = ?";

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
    return new int[]{
      database.getDatabaseChangeLogLockTableName().hashCode(),
      defaultSchemaName.hashCode()
    };
  }


  @Override
  protected boolean acquireLock(Connection con) throws SQLException, LockException {
    int sessionId = getSessionID(con);
    boolean lock_aquired = writeSessionLock(con, sessionId);
    return lock_aquired;
  }

  private boolean writeSessionLock(Connection con, int sessionId) throws SQLException {


    LockState lockState = checkCurrentSession(con, sessionId);
    if (lockState.equals(LockState.LOCKED)) {
      return true;
    } else if (lockState.equals(LockState.FORBIDDEN)) {
      removeStaleSessions(con);
    }

    tryLock(con, sessionId);

    lockState = checkCurrentSession(con, sessionId);
    if (lockState.equals(LockState.LOCKED)) {
      return true;
    }
    return false;
  }

  private void tryLock(Connection con, int sessionId) throws SQLException {
    String schema = getCurrentSchema(con);
    PreparedStatement stmt = con.prepareStatement(SQL_CHECK_LOCK);
    stmt.setString(1, schema);
    stmt.setString(2, database.getDatabaseChangeLogTableName());
    stmt.setInt(3, sessionId);
    stmt.setString(4, "liquibase");
    ResultSet results = stmt.executeQuery();
    if (results.next()) {
      return;
    }
    stmt = con.prepareStatement(SQL_TRY_LOCK);
    stmt.setString(1, schema);
    stmt.setString(2, database.getDatabaseChangeLogTableName());
    stmt.setInt(3, sessionId);
    stmt.setString(4, "liquibase");
    stmt.executeUpdate();
  }

  private void removeStaleSessions(Connection con) {
    throw new NotImplementedException();
  }

  private LockState checkCurrentSession(Connection con, int sessionId) throws SQLException {
    String schema = getCurrentSchema(con);
    String query = "SELECT TABLE_SCHEMA, TABLE_NAME, SESSION_ID, LOCK_TYPE FROM INFORMATION_SCHEMA.LOCKS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND  LOCK_TYPE = ?";
    PreparedStatement stmt = con.prepareStatement(query);
    stmt.setString(1, schema);
    stmt.setString(2, database.getDatabaseChangeLogTableName());
    stmt.setString(3, "liquibase");
    ResultSet results = stmt.executeQuery();
    if (!results.next()) {
      return LockState.NONE;
    }
    while (results.next()) {
      int granted_session = results.getInt("SESSION_ID");
      if (granted_session == sessionId) {
        return LockState.LOCKED;
      }
    }
    return LockState.FORBIDDEN;
  }

  private int getSessionID(Connection con) throws SQLException {
    try (PreparedStatement stmt = con.prepareStatement("CALL SESSION_ID()")) {
      stmt.setQueryTimeout(10);
      ResultSet resultSet = stmt.executeQuery();
      int result = resultSet.getInt("SESSION_ID()");
      return result;
    }
  }

  private String getCurrentSchema(Connection con) throws SQLException {
    try (PreparedStatement stmt = con.prepareStatement("CALL CURRENT_SCHEMA")) {
      stmt.setQueryTimeout(10);
      ResultSet resultSet = stmt.executeQuery();
      String result = resultSet.getString("CURRENT_SCHEMA");
      return result;
    }
  }


  @Override
  protected void releaseLock(Connection con) throws SQLException, LockException {
    int sessionId = getSessionID(con);
    String schema = getCurrentSchema(con);
    try (PreparedStatement stmt = con.prepareStatement(SQL_UNLOCK)) {
      stmt.setString(1, schema);
      stmt.setString(2, "databasechangelog");
      stmt.setInt(3, sessionId);
      stmt.setString(4, "liquibase");
      Boolean unlocked = stmt.executeUpdate() == 0;
      if (!Boolean.TRUE.equals(unlocked)) {
        throw new LockException(SQL_UNLOCK + " returned " + unlocked);
      }
    }
  }

  /**
   * Obtains information about the database changelog lock.
   */
  @Override
  protected DatabaseChangeLogLock usedLock(Connection con) throws SQLException, LockException {
    // Sadly there is no way to determine the actual Lockdata from the connection
    return new DatabaseChangeLogLock(1, new Date(), "liquibase");
  }

  private static String lockedBy(ResultSet rs) throws SQLException {
    String host = rs.getString("client_hostname");
    if (host == null) {
      return "pid#" + rs.getInt("pid");
    }
    return host + " (" + rs.getString("state") + ")";
  }
}
