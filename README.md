# Liquibase Extension: Session Lock Support

Provides _session-level_ (vs. _transaction-level_)
[`LockService`](http://www.liquibase.org/javadoc/liquibase/lockservice/LockService.html)
implementations.  Session-level locks get automatically released if the database
connection drops, and overcome the shortcoming of the
[`StandardLockService`](https://docs.liquibase.com/concepts/basic/databasechangeloglock-table.html):

>   If Liquibase does not exit cleanly, the lock row may be left as locked.
>   You can clear out the current lock by running `liquibase releaseLocks`
>   which runs `UPDATE DATABASECHANGELOGLOCK SET LOCKED=0`.

Running `liquibase releaseLocks` in a micro-service production environment
may not be really feasible.

## Supported Databases

-   MySQL
-   MariaDB
-   PostgreSQL
-   Oracle

Support for other databases may be conveniently added by extending `SessionLockService`.

### MySQL / MariaDB

The MySQL and MariaDB implementation rely on user locks: `get_lock` and `is_used_lock` are builtin functions for MySQL and MariaDB. The lock is automatically released when the connection is dropped unexpectedly.

### PostgreSQL

The Postgres implementation used `pg_try_advisory_lock` and `pg_try_advisory_unlock`

### Oracle

The Oracle implementation relies on [`DBMS_LOCK`](https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_LOCK.html).
The user that executes liquibase must have `EXECUTE` privilege on `DBMS_LOCK`.
```sql
grant execute on SYS.DBMS_LOCK to <user>;
```

To read lock information, the user needs permissions to read from `V$LOCK` and `V$SESSION`.
```sql
grant select on SYS.V_$LOCK to <user>;
grant select on SYS.V_$SESSION to <user>;
```

## Usage
To use the new lockservice, simply add a dependency to the library. Because the priority is higher
than the StandardLockService, it will automatically be used (provided the database is supported). The library supports Liquibase v3.x and v4.x.

### Maven
```xml
<dependency>
    <groupId>com.github.blagerweij</groupId>
    <artifactId>liquibase-sessionlock</artifactId>
    <version>1.4.0</version>
</dependency>
```
### Gradle
`implementation 'com.github.blagerweij:liquibase-sessionlock:1.4.0'`

## Disclaimer

_This module, both source code and documentation, is in the Public Domain, and comes with **NO WARRANTY**._

## License
This module is using the Apache Software License, version 2.0. See http://www.apache.org/licenses/LICENSE-2.0.txt