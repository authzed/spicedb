# New Enemy Test

This test suite performs testing for the New Enemy problem on CockroachDB, including verification that they occur without mitigations and ensuring that when present, those mitigations work to prevent the issue from occurring.

## The Test

This is the schema:

```zed
definition user {}
definition resource {
	relation direct: user
	relation excluded: user
	permission allowed = direct - excluded
}
```

These are the operations:

1. `exclude` write: `resource:thegoods#excluded@user:1#...`
2. `direct` write: `resource:thegoods#direct@user:1#...`
3. Check: `resource:thegoods#allowed@user:1#...`
    1. Read tuples from (1) and (2) at the revision returned by (2)

## In SQL

This is how each operation is translated to SQL

1. Write exclusion tuple

   ```sql
   INSERT INTO relation_tuple (namespace,object_id,relation,userset_namespace,userset_object_id,userset_relation) VALUES ("resource","thegoods","direct","user","1","...") ON CONFLICT (namespace,object_id,relation,userset_namespace,userset_object_id,userset_relation) DO UPDATE SET timestamp = now() RETURNING cluster_logical_timestamp()
   ```

2. Write direct tuple

   ```sql
   INSERT INTO relation_tuple (namespace,object_id,relation,userset_namespace,userset_object_id,userset_relation) VALUES ("resource","thegoods","excluded","user","1","...") ON CONFLICT (namespace,object_id,relation,userset_namespace,userset_object_id,userset_relation) DO UPDATE SET timestamp = now() RETURNING cluster_logical_timestamp()
   ```

3. Check

   ```sql
   SET TRANSACTION AS OF SYSTEM TIME 1631462510162458000;

   SELECT namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation FROM relation_tuple WHERE namespace = "resource" AND object_id = "thegoods" AND relation = "excluded";


   SET TRANSACTION AS OF SYSTEM TIME 1631462510162458000;

   SELECT namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation FROM relation_tuple WHERE namespace = "resource" AND object_id = "thegoods" AND relation = "direct";
   ```

## Triggering a "New Enemy"

The new enemy problem occurs when a client can observe test steps `exclude write` and `direct write` in sequence, request a check with the revision returned by `direct write`, but still be granted access.
This should only happen if the timestamp returned by `direct write` is below the timestamp returned by `exclude write`.

In Zanzibar, this is prevented by Spanner's TrueTime:
> Spanner’s TrueTime mechanism assigns each ACL write a microsecond-resolution timestamp, such that the timestamps of writes reflect the causal ordering between writes, and thereby provide external consistency.

CockroachDB doesn't provide the same guarantees, instead choosing to wait on subsequent reads of overlapping keys.

This means that in SpiceDB backed by CockroachDB, the new enemy problem is possible, but requires the following conditions:

1. The writes in the `exclude write` and `direct write` must land in different ranges.
2. The leader of the range for write 2 must not be on the same node as any follower for the range for write 1.
    - Otherwise, the timestamp cache for the two nodes will be in sync and the logical clock will properly order the transactions
3. The writes are received by two different nodes in the cluster, one of which has a slower clock.

This is possible because the keys that are written in the transactions do not overlap.

It's easier to force these conditions by configuring cockroach with:

```sql
ALTER DATABASE spicedb CONFIGURE ZONE USING range_min_bytes = 0, range_max_bytes = 65536, num_replicas = 1;"
```

This makes ranges as small as possible (increasing the likelihood keys will land in different ranges) and reduces the replica count to 1 (making it impossible for a node to have a follower of the raft leader).

Even under these conditions, to trigger the new enemy problem we have to:

- Generate a large set of tuples to spread them across ranges
- Artificially introduce large time skew and network latency (much larger than we typically see in crdb clusters)

_Note: timechaos only works on amd64 and ptrace calls don't work in qemu, which means there is no way to run this test suite on an arm machine (like m1 mac)._

## Build notes

This runs in CI and builds spicedb from head.
The go.mod/go.sum may get out of sync.
If they do, they can be fixed with:

```bash
cd e2e
go get -d github.com/authzed/spicedb/cmd/spicedb/...
go build github.com/authzed/spicedb/cmd/spicedb/...
go mod tidy
```
