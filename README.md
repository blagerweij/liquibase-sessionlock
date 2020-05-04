# Liquibase Extension: Session Lock Support

Provides _session-level_ (vs. _transaction-level_)
[`LockService`](http://www.liquibase.org/javadoc/liquibase/lockservice/LockService.html)
implementations.  Session-level locks get automatically released if the database
connection drops, and overcome the shortcoming of the
[`StandardLockService`](https://www.liquibase.org/documentation/databasechangeloglock_table.html):

>   If Liquibase does not exit cleanly, the lock row may be left as locked.
>   You can clear out the current lock by running `liquibase releaseLocks`
>   which runs `UPDATE DATABASECHANGELOGLOCK SET LOCKED=0`.

Running `liquibase releaseLocks` in a micro-service production environment
may not be really feasible.

## Supported Databases

-   MySQL
-   PostgreSQL (pending [`listLocks`](https://www.liquibase.org/documentation/command_line.html) support)

Support for other databases may be conveniently added by extending `SessionLockService`.

## Disclaimer

_This module, both source code and documentation, is in the Public Domain, and comes with **NO WARRANTY**._
