/*
 * This module, both source code and documentation,
 * is in the Public Domain, and comes with NO WARRANTY.
 */
package org.liquibase.lockservice.ext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import liquibase.database.Database;
import liquibase.database.core.PostgresDatabase;
import liquibase.exception.DatabaseException;
import liquibase.exception.LockException;
import liquibase.lockservice.DatabaseChangeLogLock;
import liquibase.logging.LogService;

/**
 * Employs PostgreSQL <i>advisory locks</i>.
 * <blockquote>
 * <p>While a flag stored in a table could be used for the same purpose,
 * advisory locks are faster, avoid table bloat, and are automatically
 * cleaned up by the server at the end of the session.</p>
 * <p>
 * There are two ways to acquire an advisory lock in PostgreSQL: at session
 * level or at transaction level. Once acquired at session level, an advisory
 * lock is held until explicitly released or the session ends. Unlike standard
 * lock requests, session-level advisory lock requests do not honor transaction
 * semantics: a lock acquired during a transaction that is later rolled back
 * will still be held following the rollback, and likewise an unlock is
 * effective even if the calling transaction fails later.</p>
 * </blockquote>
 *
 * @see  "<a href='https://www.postgresql.org/docs/9.6/explicit-locking.html#ADVISORY-LOCKS'>Advisory
 *              Locks</a> (PostgreSQL Documentation)"
 */
public class PGLockService extends SessionLockService {

    static final String SQL_TRY_LOCK = "SELECT pg_try_advisory_lock(?)";
    static final String SQL_UNLOCK = "SELECT pg_advisory_unlock(?)";

    @Override
    public boolean supports(Database database) {
        return (database instanceof PostgresDatabase) && isAtLeastPostgres91(database);
    }

    private static boolean isAtLeastPostgres91(Database database) {
        try {
            return (database.getDatabaseMajorVersion() > 9)
                    || (database.getDatabaseMajorVersion() == 9
                            && database.getDatabaseMinorVersion() >= 1);
        } catch (DatabaseException e) {
            LogService.getLog(PGLockService.class)
                    .warning("Problem querying database version", e);
            return false;
        }
    }

    private long getChangeLogLockId() {
        // Unlike the general Object.hashCode() contract,
        // String.hashCode() should be stable across VM instances and Java versions.
        long high = database.getDefaultSchemaName().hashCode();
        high <<= Integer.SIZE;
        long low = database.getDatabaseChangeLogLockTableName().hashCode();
        low &= 0x00000000_FFFFFFFFL;
        return high | low;
    }

    private static Boolean getBooleanResult(PreparedStatement stmt) throws SQLException {
        try (ResultSet rs = stmt.executeQuery()) {
            rs.next();
            return (Boolean) rs.getObject(1);
        }
    }

    /**
     * @see  "<a href='https://www.postgresql.org/docs/9.6/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS'><code>pg_try_advisory_lock</code></a>
     *          (Advisory Lock Functions)"
     */
    @Override
    protected boolean acquireLock(Connection con) throws SQLException, LockException {
        try (PreparedStatement stmt = con.prepareStatement(SQL_TRY_LOCK)) {
            stmt.setLong(1, getChangeLogLockId());

            return Boolean.TRUE.equals(getBooleanResult(stmt));
        }
    }

    /**
     * @see  "<a href='https://www.postgresql.org/docs/9.6/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS'><code>pg_advisory_unlock</code></a>
     *          (Advisory Lock Functions)"
     */
    @Override
    protected void releaseLock(Connection con) throws SQLException, LockException {
        try (PreparedStatement stmt = con.prepareStatement(SQL_UNLOCK)) {
            stmt.setLong(1, getChangeLogLockId());

            Boolean unlocked = getBooleanResult(stmt);
            if (!Boolean.TRUE.equals(unlocked)) {
                throw new LockException("pg_advisory_unlock() returned " + unlocked);
            }
        }
    }

    /**
     * Obtains information about the database changelog lock.
     * <blockquote>
     * Like all locks in PostgreSQL, a complete list of advisory locks currently
     * held by any session can be found in the <code>pg_locks</code> system view.
     * </blockquote>
     *
     * @see  "<a href='https://www.postgresql.org/docs/9.6/view-pg-locks.html'><code>pg_locks</code></a>
     *          (PostgreSQL Documentation)"
     * @see  "<a href='https://www.postgresql.org/docs/9.6/monitoring-stats.html#PG-STAT-ACTIVITY-VIEW'><code>pg_stat_activity</code>
     *          View</a> (PostgreSQL Documentation)"
     */
    @Override
    protected DatabaseChangeLogLock usedLock(Connection con)
            throws SQLException, LockException
    {
        // TODO: Provide meaningful implementation.
        return null;
    }

}
