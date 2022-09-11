/*
 * This module, both source code and documentation,
 * is in the Public Domain, and comes with NO WARRANTY.
 */
package com.github.blagerweij.sessionlock;

import liquibase.database.core.H2Database;
import liquibase.database.core.OracleDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LockException;
import liquibase.lockservice.DatabaseChangeLogLock;
import org.h2.tools.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.h2.jdbcx.JdbcDataSource;

import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

public class H2LockServiceTest {

    static Server server;

    private H2LockService lockService;
    private H2LockService deniedService;

    private Connection dbCon;

    private ExecutorService executor;

    public H2LockServiceTest() {
        executor = Executors.newSingleThreadExecutor();
    }

    @BeforeAll
    public static void prepare() {
        try {
            server = Server.createTcpServer().start();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public static void cleanup() {
        server.stop();

    }

    @BeforeEach
    public void setUp() throws SQLException {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:default");
        ds.setUser("sa");
        ds.setPassword("sa");
        Connection conn = ds.getConnection();
        dbCon = conn;
        //int res = conn.prepareStatement("ALTER USER sa ADMIN TRUE").executeUpdate();

        H2Database database = new H2Database();
        database.setDefaultSchemaName("test_schema");
        database = spy(database);
        doReturn(new JdbcConnection(dbCon)).when(database).getConnection();

        H2Database denied = new H2Database();
        denied.setDefaultSchemaName("test_schema");
        denied = spy(denied);
        doReturn(new JdbcConnection(ds.getConnection())).when(denied).getConnection();

        lockService = new H2LockService();
        lockService.setDatabase(database);
        deniedService = new H2LockService();
        deniedService.setDatabase(denied);
    }

    @Test
    public void supports() throws Exception {
        assertThat(lockService.supports(new H2Database())).isTrue();

    }

    @Test
    public void supportsNot() throws Exception {
        assertThat(lockService.supports(new OracleDatabase())).isFalse();
    }


    @Test
    @SuppressWarnings("resource")
    public void acquireSuccess() throws Exception {
        assertThat(lockService.acquireLock()).isTrue();
    }

    @Test
    @SuppressWarnings("resource")
    public void acquireUnsuccessful() throws Exception {
        Future<Boolean> aquiredLock = executor.submit(() -> {
            try {
                return lockService.acquireLock();
            } catch (LockException e) {
                throw new RuntimeException(e);
            }
        });
        assertThat(aquiredLock.get()).isTrue();
        Future<Boolean> deniedLock = executor.submit(() -> {
            try {
                return deniedService.acquireLock();
            } catch (LockException e) {
                throw new RuntimeException(e);
            }
        });
        lockService.releaseLock();
        assertThat(deniedLock.get()).isTrue();
        deniedService.releaseLock();
        System.out.println("I am done");
    }

    @Test
    @SuppressWarnings("resource")
    public void releaseSuccess() throws Exception {
        assertThat(lockService.acquireLock()).isTrue();

        lockService.releaseLock();
    }
}
