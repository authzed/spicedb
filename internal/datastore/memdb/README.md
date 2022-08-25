# MemDB Datastore Implementation

The `memdb` datastore implementation is based on Hashicorp's [go-memdb library](https://github.com/hashicorp/go-memdb).
Its implementation most closely mimics that of `spanner`, or `crdb`, where there is a single immutable datastore that supports querying at any point in time.
The `memdb` datastore is used for validating and rapidly iterating on concepts from consumers of other datastores.
It is 100% compliant with the datastore acceptance test suite and it should be possible to use it in place of any other datastore for development purposes.
Differences between the `memdb` datastore and other implementations that manifest themselves as differences visible to the caller should be reported as bugs.

**The memdb datastore can NOT be used in a production setting!**

## Implementation Caveats

### No Garbage Collection

This implementation of the datastore has no garbage collection, meaning that memory usage will grow monotonically with mutations.

### No Durable Storage

The `memdb` datastore, as its name implies, stores information entirely in memory, and therefore will lose all data when the host process terminates.

### Cannot be used for multi-node dispatch

If you attempt to run SpiceDB with multi-node dispatch enabled using the memory datastore, each independent node will get a separate copy of the datastore, and you will end up very confused.
