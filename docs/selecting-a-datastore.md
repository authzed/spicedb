# Selecting a datastore

SpiceDB ships with a number of [datastores](/internal/datastore), drivers used to store and retrieve the relationship data used for computing permissions.

The currently available datastores are:

- [Cockroach DB](#cockroach-db) - Best for large or scalable installations
- [Postgres](#postgres) - Best for smaller or isolated applications
- [memdb](#memdb) - For development and testing

## Cockroach DB

[Implementation](/internal/datastore/crdb)

Datastore for storing and accessing data in a [Cockroach DB](https://github.com/cockroachdb/cockroach) cluster.

- Best for large or scalable installations
- Fully distributed and resilient to node failures
- Queries are sharded over the breadth of the cluster

Caveats:

- Setup and maintenance complexity of the Cockroach DB cluster

### Running with CockroachDB datastore

```sh
--datastore-kind=cockroachdb --datastore-conn-uri="connection uri here"
```

Optional Parameters:

| Parameter                             | Description                                                                         | Example                                      |
|---------------------------------------|-------------------------------------------------------------------------------------|----------------------------------------------|
| `datastore-max-tx-retries`            | Maximum number of times to retry a query before raising an error                    | `--datastore-max-tx-retries=50`              |
| `datastore-tx-overlap-strategy`       | The overlap strategy to prevent New Enemy on CRDB (see below)                       | `--datastore-tx-overlap-strategy=static`     |
| `datastore-tx-overlap-key`            | The key to use for the overlap strategy (see below)                                 | `--datastore-tx-overlap-key="foo"`           |
| `datastore-conn-max-idletime`         | Maximum idle time for a connection before it is recycled                            | `--datastore-conn-max-idletime=60s`          |
| `datastore-conn-max-lifetime`         | Maximum lifetime for a connection before it is recycled                             | `--datastore-conn-max-lifetime=300s`         |
| `datastore-conn-max-open`             | Maximum number of concurrent connections to open                                    | `--datastore-conn-max-open=10`               |
| `datastore-conn-min-open`             | Minimum number of concurrent connections to open                                    | `--datastore-conn-min-open=1`                |
| `datastore-query-split-size`          | The (estimated) query size at which to split a query into multiple queries          | `--datastore-query-split-size=5kb`           |
| `datastore-gc-window`                 | Sets the window outside of which overwritten relationships are no longer accessible | `--datastore-gc-window=1s`                   |
| `datastore-revision-fuzzing-duration` | Sets a fuzzing window on all zookies/zedtokens                                      | `--datastore-revision-fuzzing-duration=50ms` |
| `datastore-readonly`                  | Places the datastore into readonly mode                                             | `--datastore-readonly=true`                  |

### Overlap Strategies

In order to mitigate the [New Enemy Problem](https://authzed.com/blog/new-enemies/), the Cockroach DB datastore makes use of an overlap strategy, which ensures that data ranges are read in the correct order.

Strategies available:

- `static` - A single key (`datastore-tx-overlap-key`) is used in all writes to ensure proper consistency (**Recommended**)
- `prefix` - A prefix of the object type is used for all writes (Do not use unless **ALL object types** are prefixed, e.g. `myapp/` and `anotherapp/`)
- `insecure` - Disables the overlap strategy entirely (**WARNING: Can open up to the New Enemy problem**)

## Postgres

[Implementation](/internal/datastore/postgres)

Datastore for storing and accessing data in a [Postgres](https://www.postgresql.org/) database.

- Best for standard or isolated applications
- Simple to setup and maintain
- Full consistency out of the box

Caveats:

- Single point of failure (Postgres)
- Harder to scale

### Running with Postgres datastore

```sh
--datastore-kind=postgres --datastore-conn-uri="connection string here"
```

Optional Parameters:

Markdown Table Formatter
This tool formats basic MultiMarkdown style tables for easier plain text reading. It adds padding to all the cells to line up the pipe separators when using a mono-space font.

To see what it's all about, try one of these examples, or format your own.

Load: Example 1 - Example 2 - Example 3

| Parameter                             | Description                                                                         | Example                                      |
|---------------------------------------|-------------------------------------------------------------------------------------|----------------------------------------------|
| `datastore-conn-max-idletime`         | Maximum idle time for a connection before it is recycled                            | `--datastore-conn-max-idletime=60s`          |
| `datastore-conn-max-lifetime`         | Maximum lifetime for a connection before it is recycled                             | `--datastore-conn-max-lifetime=300s`         |
| `datastore-conn-max-open`             | Maximum number of concurrent connections to open                                    | `--datastore-conn-max-open=10`               |
| `datastore-conn-min-open`             | Minimum number of concurrent connections to open                                    | `--datastore-conn-min-open=1`                |
| `datastore-query-split-size`          | The (estimated) query size at which to split a query into multiple queries          | `--datastore-query-split-size=5kb`           |
| `datastore-gc-window`                 | Sets the window outside of which overwritten relationships are no longer accessible | `--datastore-gc-window=1s`                   |
| `datastore-revision-fuzzing-duration` | Sets a fuzzing window on all zookies/zedtokens                                      | `--datastore-revision-fuzzing-duration=50ms` |
| `datastore-readonly`                  | Places the datastore into readonly mode                                             | `--datastore-readonly=true`                  |

## memdb

[Implementation](/internal/datastore/memdb)

DEVELOPMENT AND TESTING ONLY

The memdb datastore is an *in-memory* datastore implemented via the [github.com/hashicorp/go-memdb](github.com/hashicorp/go-memdb) package.

All data stored in the datastore is *transient* and will be deleted once the SpiceDB process has terminated.

**NOTE:** *Cannot* be used in any form of multi-node deployment

### Running with memdb datastore

```sh
--datastore-kind=memdb
```

Optional Parameters:

| Parameter                             | Description                                                                         | Example                                      |
|---------------------------------------|-------------------------------------------------------------------------------------|----------------------------------------------|
| `datastore-revision-fuzzing-duration` | Sets a fuzzing window on all zookies/zedtokens                                      | `--datastore-revision-fuzzing-duration=50ms` |
| `datastore-gc-window`                 | Sets the window outside of which overwritten relationships are no longer accessible | `--datastore-gc-window=1s`                   |
| `datastore-readonly`                  | Places the datastore into readonly mode                                             | `--datastore-readonly=true`                  |
