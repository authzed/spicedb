# PostgreSQL Datastore

PostgreSQL is a traditional relational database management system that is very popular.
This datastore implementation allows you to use a PostgreSQL database as the backing durable storage for SpiceDB.
Recommended usage: when you are comfortable with having all permissions data stored in a single region.

## Implementation Caveats

While PostgreSQL uses MVCC to implement its ACID properties, it doesn't offer users the ability to read dirty data without adding an extension.
For that reason, the PostgreSQL datastore driver implements a second layer of MVCC where we can manually control all writes to the database.
This allows us to track all revisions of the database explicitly and perform point-in-time snapshot queries.
