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
-   PostgreSQL

Support for other databases may be conveniently added by extending `SessionLockService`.  For Oracle one may look at:

-   [DBMS_LOCK](https://docs.oracle.com/en/database/oracle/oracle-database/12.2/arpls/DBMS_LOCK.html) (Database PL/SQL Packages and Types Reference)
-   [Using Oracle Lock Management Services (User Locks)](https://www.oracle.com/pls/topic/lookup?ctx=en/database/oracle/oracle-database/12.2/arpls&id=ADFNS-GUID-57365E45-5F85-471B-81D9-F52EA16F1E85)

## Disclaimer

_This module, both source code and documentation, is in the Public Domain, and comes with **NO WARRANTY**._
