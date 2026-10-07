# embedded

`embedded` runs SpiceDB's permission engine **in-process, without a gRPC server**. It is
intended for callers that embed SpiceDB as a library and want to issue permission checks
directly against a datastore — paying neither network nor gRPC-serialization cost, and
passing caveat context as native Go values rather than `structpb`.

It is a thin, focused wrapper over SpiceDB's dispatch engine (`computed.ComputeCheck` + a
local dispatcher), with committed permission checks, revisioned reads, and validated schema and
relationship writes. It can also share a SQL transaction with application writes so both commit
together; see [Application-owned SQL transactions](#application-owned-sql-transactions).

## When to use it

- You already have (or can construct) a `datastore.Datastore` in your process and want fast,
  allocation-light permission checks against it.
- You want to avoid the overhead of standing up an embedded gRPC server + in-process client
  (bufconn), the full server middleware chain, and the `structpb`/base64 caveat-context
  round-trip.

## When **not** to use it

- You need the full SpiceDB v1 API surface, including bulk operations, watch, lookup,
  or reflection.
- You need remote access or multiple processes sharing one logical SpiceDB. Run a real
  SpiceDB server instead.

`Permissions.Check` evaluates at the committed datastore head.
`Permissions.SnapshotReader(revision).Check` evaluates at the supplied committed revision.

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	dscfg "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/embedded"
)

const bootstrap = `
schema: |-
  definition user {}

  caveat is_tuesday(day string) {
    day == "tuesday"
  }

  definition document {
    relation viewer: user
    relation caveated_viewer: user with is_tuesday

    permission view = viewer
    permission caveated_view = caveated_viewer
  }
relationships: |-
  document:readme#viewer@user:alice

  document:readme#caveated_viewer@user:bob[is_tuesday]
`

func main() {
	ctx := context.Background()

	// Any datastore works. Here we use an in-memory datastore populated from a bootstrap
	// document and storing schema in the unified ("single store") format.
	ds, err := dscfg.NewDatastore(ctx,
		dscfg.DefaultDatastoreConfig().ToOption(),
		dscfg.SetBootstrapFileContents(map[string][]byte{"bootstrap.yaml": []byte(bootstrap)}),
		dscfg.WithCaveatTypeSet(caveattypes.Default.TypeSet),
		dscfg.WithBootstrapSchemaMode(datalayer.SchemaModeReadNewWriteNew),
	)
	if err != nil {
		log.Fatal(err)
	}

	perms, err := embedded.NewPermissions(embedded.Config{
		Datastore: ds,
		// Read schema from the unified store, and cache it across checks so schema-derived
		// caches (e.g. compiled caveats) persist and are not rebuilt per check.
		SchemaMode:              datalayer.SchemaModeReadNewWriteNew,
		SchemaCacheMaxCostBytes: 16 << 20, // 16 MiB
	})
	if err != nil {
		log.Fatal(err)
	}
	defer perms.Close()

	// Plain check.
	res, err := perms.Check(ctx, embedded.CheckRequest{
		ResourceType: "document", ResourceID: "readme", Permission: "view",
		SubjectType: "user", SubjectID: "alice",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("alice can view:", res.HasPermission) // true

	// Caveated check — caveat context is passed as native Go values.
	res, err = perms.Check(ctx, embedded.CheckRequest{
		ResourceType: "document", ResourceID: "readme", Permission: "caveated_view",
		SubjectType: "user", SubjectID: "bob",
		CaveatContext: map[string]any{"day": "tuesday"},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("bob can view on tuesday:", res.HasPermission) // true

	// Without the required context, the result is conditional rather than allowed/denied.
	res, _ = perms.Check(ctx, embedded.CheckRequest{
		ResourceType: "document", ResourceID: "readme", Permission: "caveated_view",
		SubjectType: "user", SubjectID: "bob",
	})
	fmt.Println("conditional:", res.IsConditional, "missing:", res.MissingContext)
	// conditional: true missing: [day]
}
```

You are not limited to bootstrap documents — pass any `datastore.Datastore` you have
populated however you like (the relationships/schema must already be written).

## Configuration

`embedded.Config`:

| Field | Required | Default | Notes |
|---|---|---|---|
| `Datastore` | **yes** | — | The datastore to check against. The caller owns its lifecycle (`Close` does not close it). |
| `CaveatTypeSet` | no | `caveattypes.Default` | Must match the type set the schema/caveats were written with. |
| `SchemaMode` | no | legacy (per-definition) | Use `datalayer.SchemaModeReadNewWriteNew` (or `*Both`) to read the unified schema. Must match how the datastore's schema was written. |
| `SchemaCacheMaxCostBytes` | no | `0` (disabled) | When `> 0`, caches the unified stored schema across checks. This is what lets schema-derived caches (compiled caveats, etc.) persist; strongly recommended whenever `SchemaMode` reads from the unified schema. |
| `DispatchConcurrencyLimit` | no | `10` | Max concurrent sub-dispatches per check. |
| `DispatchChunkSize` | no | `100` | Datastore query / dispatch chunk size. |
| `MaxDepth` | no | `50` | Maximum dispatch recursion depth. |

## The `Check` API

```go
type CheckRequest struct {
	ResourceType    string
	ResourceID      string
	Permission      string
	SubjectType     string
	SubjectID       string
	SubjectRelation string         // optional; defaults to the "..." (ellipsis) relation
	CaveatContext   map[string]any // native Go values; no structpb / base64
}

type CheckResult struct {
	HasPermission  bool     // definitively a member of the permission
	IsConditional  bool     // membership depends on a caveat that lacked required context
	MissingContext []string // the caveat context fields that were required but not provided
}
```

- `HasPermission == true` → allowed.
- `HasPermission == false && IsConditional == false` → denied.
- `IsConditional == true` → a caveat could not be fully evaluated; supply the values named in
  `MissingContext` and check again.

## Caveat context

Because checks run in-process, caveat context is supplied directly as `map[string]any` and
consumed by the caveat engine without conversion. For a caveat parameter typed `bytes`, the
value must still be a base64-encoded string (the caveat type system decodes it); all other
types accept their natural Go representation.

## Lifecycle

Call `Close` when finished to release the dispatcher. `Close` does **not** close the
datastore you passed in — you own that.

A `Permissions` value is safe for concurrent use.


## Revisioned reads and writes

These methods write schema and relationships, then let you read or check the resulting state
at a fixed committed revision.

### `WriteSchema(ctx, text)`

`WriteSchema(ctx context.Context, text string) (WriteSchemaResult, error)` compiles, validates,
and commits a complete schema. The result contains the committed `Revision`. Changes that
invalidate existing relationships are rejected.

### `WriteRelationships(ctx, updates)`

`WriteRelationships(ctx context.Context, updates []tuple.RelationshipUpdate) (WriteRelationshipsResult, error)`
validates and commits a batch of CREATE, TOUCH, or DELETE updates. The result contains the
committed `Revision`. The default limit is 1,000 updates per call and 25,000 bytes per caveat
context; configure `MaxUpdatesPerWrite` or `MaxRelationshipContextSize` to change these limits.
Expiring relationships require `ExpiringRelationshipsEnabled`.

### `HeadRevision(ctx)`

`HeadRevision(ctx context.Context) (HeadRevisionResult, error)` returns a fresh committed
revision in `HeadRevisionResult.Revision`. Use it to read the current datastore state at one
consistent revision.

### `SnapshotReader(revision)`

`SnapshotReader(revision datastore.Revision) *RevisionedReader` creates a reader for a committed
revision. It performs no I/O; the revision is validated when a reader method runs. Use a revision
from `HeadRevision`, a successful write, or a committed transaction.

### `RevisionedReader.Check(ctx, req)`

`Check(ctx context.Context, req CheckRequest) (CheckResult, error)` checks permission at the
reader's revision. `req` contains the resource type, ID, and permission; subject type and ID; and
optional subject relation and caveat context. See [The `Check` API](#the-check-api) for all
fields and result values.

### `RevisionedReader.ReadSchema(ctx)`

`ReadSchema(ctx context.Context) (ReadSchemaResult, error)` returns `SchemaText`, the schema
visible at the reader's revision.

### `RevisionedReader.ReadRelationships(ctx, filter, opts...)`

`ReadRelationships(ctx context.Context, filter datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (ReadRelationshipsResult, error)`
reads relationships matching `filter`; optional query options customize the read. The result's
`Relationships` iterator yields `(tuple.Relationship, error)` pairs. Check each error and finish
iteration before closing the parent permissions instance.

Revisions apply only to the originating datastore and remain usable only within its garbage
collection window. A reader holds no connection and does not prevent history collection. Each
read validates the revision and returns an error if it is no longer valid; it never advances an
expired reader to the head.

## Application-owned SQL transactions

Use an existing SQL database when your application needs to update its own rows and SpiceDB
relationships as one unit. This keeps both changes together: either the database commits both,
or neither.

To get started, run `MigrateIfNeeded` on the config, then construct permissions with your
existing pool and write your authorization schema:

```go
// Apply any missing SpiceDB tables using the caller-owned pool.
cfg := embedded.PostgresConfig{}
if err := cfg.MigrateIfNeeded(ctx, pool); err != nil {
	return err
}

// Create the embedded permissions API over that same pool.
p, err := embedded.NewPostgresPermissions(ctx, pool, cfg)
if err != nil {
	return err
}

// Install the authorization schema used by checks and relationship writes.
if _, err := p.WriteSchema(ctx, schema); err != nil {
	return err
}
```

Then, for each change, begin a transaction, make the application changes, stage relationship
changes with the matching `With…Transaction` method, and finish with `pending.Commit(ctx)`.

The commit returns a revision you can use for permission checks.

```go
// Begin one SERIALIZABLE transaction for application and SpiceDB writes.
tx, err := p.BeginTransaction(ctx)
if err != nil {
	return err
}
defer tx.Rollback(context.Background())

// Write the application row in the shared transaction.
_, err = tx.Exec(
	ctx,
	"INSERT INTO documents (id) VALUES ($1)",
	"doc1",
)
if err != nil {
	return err
}

// Stage the matching SpiceDB relationship on that same transaction.
pending, err := p.WithPostgresTransaction(
	ctx,
	tx,
	func(ctx context.Context, rels *embedded.RelationshipTransaction) error {
		_, err := rels.WriteRelationships(ctx, updates)
		return err
	},
)
if err != nil {
	return err
}

// Commit both sets of writes and retain the revision for later checks.
committed, err := pending.Commit(ctx)
if err != nil {
	return err
}
revision := committed.Revision
```

`committed.Revision` (shown as `revision` above) can then be used for permission checks.

See the [Postgres](example_postgres_test.go), [CRDB](example_crdb_test.go), and
[MySQL](example_mysql_test.go) examples for the full setup and transaction flow.

For MySQL, use InnoDB application tables and avoid DDL or other statements that implicitly
commit inside the shared transaction. The connection needs `parseTime=true`; transaction
instrumentation and the `performance_schema.events_transactions_current` consumer must be
enabled and accessible.
