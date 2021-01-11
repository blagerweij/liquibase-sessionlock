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

## Usage
To use the new lockservice, simply add a dependency to the library. Because the priority is higher
than the StandardLockService, it will automatically be used (provided the database is supported). The library supports Liquibase v3.x and v4.x.

### Maven
```xml
<dependency>
    <groupId>com.github.blagerweij</groupId>
    <artifactId>liquibase-sessionlock</artifactId>
    <version>1.2.5</version>
</dependency>
```
### Gradle
`implementation 'com.github.blagerweij:liquibase-sessionlock:1.2.5'`

## Disclaimer

_This module, both source code and documentation, is in the Public Domain, and comes with **NO WARRANTY**._

## License
This module is using the Apache Software License, version 2.0. See http://www.apache.org/licenses/LICENSE-2.0.txt