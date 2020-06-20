/*
 * This module, both source code and documentation,
 * is in the Public Domain, and comes with NO WARRANTY.
 */
package org.liquibase.lockservice.ext.test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assume.assumeThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.model.MultipleFailureException;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.lockservice.LockService;
import liquibase.lockservice.LockServiceFactory;
import liquibase.resource.ClassLoaderResourceAccessor;

public class ITMigration {

    private List<Throwable> exceptions = new ArrayList<>();

    private String dbUrl;

    @Before
    public void setUp() {
        dbUrl = System.getProperty("test.dbUrl");
        assumeThat("test.dbUrl system property", dbUrl, not(isEmptyOrNullString()));
    }

    @Test
    public void concurrentUpdate() throws Exception {
        Thread updateThread = startLiquibaseThread(liquibase -> {
            liquibase.update(new Contexts());
            assertThat("database.ranChangeSetList",
                       liquibase.getDatabase().getRanChangeSetList(), not(empty()));
        });
        Thread noopThread = startLiquibaseThread(liquibase -> {
            // Give greater chance of obtaining the lock first to the other thread.
            Thread.currentThread().wait(100);
            LockService lockService = LockServiceFactory
                    .getInstance().getLockService(liquibase.getDatabase());
            lockService.waitForLock();
            try {
                assertThat("database.ranChangeSetList",
                           liquibase.getDatabase().getRanChangeSetList(), not(empty()));
            } finally {
                lockService.releaseLock();
            }
        });

        synchronized (updateThread) {
            updateThread.notify();
        }
        synchronized (noopThread) {
            noopThread.notify();
        }
        updateThread.join();
        noopThread.join();

        MultipleFailureException.assertEmpty(exceptions);
    }

    private Thread startLiquibaseThread(LiquibaseConsumer task) {
        Thread th = new Thread(() -> {
            synchronized (Thread.currentThread()) {
                withLiquibase(liquibase -> {
                    // Warm up, if necessary.
                    LockServiceFactory.getInstance()
                            .getLockService(liquibase.getDatabase()).init();
                    Thread.currentThread().wait();
                    task.accept(liquibase);
                });
            }
        });
        th.start();
        Thread.yield();
        return th;
    }

    private void withLiquibase(LiquibaseConsumer task) {
        try (Connection connection = DriverManager.getConnection(dbUrl)) {
            Liquibase liquibase = new Liquibase("org/liquibase/lockservice/ext/test/changelog.xml",
                                                new ClassLoaderResourceAccessor(),
                                                new JdbcConnection(connection));
            task.accept(liquibase);
            liquibase.getDatabase().close();
        } catch (Exception | AssertionError e) {
            exceptions.add(e);
        }
    }

    @After
    public void tearDown() {
        if (dbUrl != null) {
            withLiquibase(liquibase -> liquibase.rollback(1, (String) null));
        }
    }


    private static interface LiquibaseConsumer {
        void accept(Liquibase liquibase) throws Exception;
    }


}