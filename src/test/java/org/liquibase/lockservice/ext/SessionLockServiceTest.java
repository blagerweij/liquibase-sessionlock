/*
 * This module, both source code and documentation,
 * is in the Public Domain, and comes with NO WARRANTY.
 */
package org.liquibase.lockservice.ext;

import static liquibase.servicelocator.PrioritizedService.PRIORITY_DEFAULT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

import liquibase.database.Database;
import liquibase.database.core.MySQLDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LockException;
import liquibase.lockservice.DatabaseChangeLogLock;
import liquibase.lockservice.StandardLockService;

public class SessionLockServiceTest {

    private SessionLockService lockService;

    private Database database;

    @Before
    @SuppressWarnings("resource")
    public void setUp() {
        database = mock(Database.class);
        when(database.getConnection())
                .thenReturn(new JdbcConnection(mock(Connection.class)));

        MockService service = new MockService();
        service.setDatabase(database);
        lockService = service;
    }

    @Test
    public void noOpInit() throws Exception {
        lockService.init();
        verifyNoInteractions(database);
    }

    @Test
    @SuppressWarnings("resource")
    public void defaultUsedLock() throws Exception {
        SessionLockService service = new SessionLockService() {
            @Override protected boolean acquireLock(Connection con) {
                throw new UnsupportedOperationException();
            }
            @Override protected void releaseLock(Connection con) {
                throw new UnsupportedOperationException();
            }
        };
        assertThat(service.usedLock(mock(Connection.class)), nullValue());
    }

    @Test
    public void priorityAboveDefault() {
        assertThat("Service priority",
                lockService.getPriority(), greaterThan(PRIORITY_DEFAULT));
        assertThat("Service priority", lockService.getPriority(),
                greaterThan(new StandardLockService().getPriority()));
    }

    @Test
    public void defaultSupports() throws Exception {
        assertEquals("Supports database", false, lockService.supports(new MySQLDatabase()));
    }

    @Test
    public void acquireReleaseState() throws Exception {
        assertEquals("acquireLock", true, lockService.acquireLock());
        assertEquals("hasChangeLogLock", true, lockService.hasChangeLogLock());
        lockService.releaseLock();
        assertEquals("hasChangeLogLock", false, lockService.hasChangeLogLock());
    }

    @Test
    public void acquireBeforeRelease() throws Exception {
        assertEquals("acquireLock", true, lockService.acquireLock());
        assertEquals("acquireLock", true, lockService.acquireLock());
        assertEquals("hasChangeLogLock", true, lockService.hasChangeLogLock());
    }

    @Test
    public void acquireFalse() throws Exception {
        ((MockService) lockService).acquireResult = false;
        assertEquals("acquireLock", false, lockService.acquireLock());
        assertEquals("hasChangeLogLock", false, lockService.hasChangeLogLock());
    }

    @Test
    public void listLocks() throws Exception {
        ((MockService) lockService).usedLockResult = new DatabaseChangeLogLock(2, new Date(1), "foo");
        DatabaseChangeLogLock[] locks = lockService.listLocks();
        assertEquals("Lock list length", 1, locks.length);
        assertNotNull("Null lock element", locks[0]);
        assertEquals("lockedBy", "foo", locks[0].getLockedBy());
    }

    @Test
    public void listNoLocks() throws Exception {
        DatabaseChangeLogLock[] locks = lockService.listLocks();
        assertEquals("Lock list length", 0, locks.length);
    }

    @Test
    @SuppressWarnings("resource")
    public void acquireShouldPropagate() throws Exception {
        SessionLockService spyService = spy(lockService);
        SQLException cause = new SQLException("bar");
        doThrow(cause).when(spyService).acquireLock(any(Connection.class));
        Exception thrown = assertThrows(LockException.class, () -> spyService.acquireLock());
        assertThat("Exception cause", thrown.getCause(), sameInstance(cause));
    }

    @Test
    @SuppressWarnings("resource")
    public void releaseShouldPropagate() throws Exception {
        SessionLockService spyService = spy(lockService);
        SQLException cause = new SQLException("baz");
        doThrow(cause).when(spyService).releaseLock(any(Connection.class));
        Exception thrown = assertThrows(LockException.class, () -> spyService.releaseLock());
        assertThat("Exception cause", thrown.getCause(), sameInstance(cause));
    }

    @Test
    @SuppressWarnings("resource")
    public void listLocksShouldPropagate() throws Exception {
        SessionLockService spyService = spy(lockService);
        SQLException cause = new SQLException("qux");
        doThrow(cause).when(spyService).usedLock(any(Connection.class));
        Exception thrown = assertThrows(LockException.class, () -> spyService.listLocks());
        assertThat("Exception cause", thrown.getCause(), sameInstance(cause));
    }


    static class MockService extends SessionLockService {

        boolean acquireResult = true;
        DatabaseChangeLogLock usedLockResult;

        @Override
        protected boolean acquireLock(Connection con) throws SQLException, LockException {
            return acquireResult;
        }

        @Override
        protected void releaseLock(Connection con) throws SQLException, LockException {
            // no-op
        }

        @Override
        protected DatabaseChangeLogLock usedLock(Connection con) throws SQLException, LockException {
            return usedLockResult;
        }

    }


}
