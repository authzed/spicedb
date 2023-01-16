# PostgreSQL Datastore

**Minimum required version** can be found here defined as `MinimumSupportedPostgresVersion` in [version.go](version/version.go)

PostgreSQL is a traditional relational database management system that is very popular.
This datastore implementation allows you to use a PostgreSQL database as the backing durable storage for SpiceDB.
Recommended usage: when you are comfortable with having all permissions data stored in a single region.

## Configuration

`track_commit_timestamp` must be set to `on` for the Watch API to be enabled.

## Implementation Caveats

While PostgreSQL uses MVCC to implement its ACID properties, it doesn't offer users the ability to read dirty data without adding an extension.
For that reason, the PostgreSQL datastore driver implements a second layer of MVCC where we can manually control all writes to the database.
This allows us to track all revisions of the database explicitly and perform point-in-time snapshot queries.
