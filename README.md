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

### Oracle

The Oracle implementation relies on [`DBMS_LOCK`](https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_LOCK.html).
The user that executes liquibase must have `EXECUTE` privilege on `DBMS_LOCK`.

```sql
grant execute on SYS.DBMS_LOCK to <user>;  
```

Oracle does not provide a way to retrieve lock details (e.g. who owns a lock) without elevated privileges.
You can use the following query to get lock details:

```sql
select locks_allocated.*, locks.*
from dba_locks locks, sys.dbms_lock_allocated locks_allocated
where locks.lock_id1 = locks_allocated.lockid
and locks_allocated.name = 'lockname';
```

However, it's possible to get some information about the current session from `SYS_CONTEXT`, so that's what gets returned by the `DatabaseChangeLogLock` object.

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