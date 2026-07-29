# embedded

`embedded` runs SpiceDB's permission engine **in-process, without a gRPC server**. It is
intended for callers that embed SpiceDB as a library and want to issue permission checks
directly against a datastore — paying neither network nor gRPC-serialization cost, and
passing caveat context as native Go values rather than `structpb`.

It is a thin, focused wrapper over SpiceDB's dispatch engine (`computed.ComputeCheck` + a
local dispatcher), exposing a single operation: `Check`.

## When to use it

- You already have (or can construct) a `datastore.Datastore` in your process and want fast,
  allocation-light permission checks against it.
- You want to avoid the overhead of standing up an embedded gRPC server + in-process client
  (bufconn), the full server middleware chain, and the `structpb`/base64 caveat-context
  round-trip.

## When **not** to use it

- You need the full SpiceDB v1 API surface (schema writes, relationship writes, bulk
  operations, watch, lookup, reflection, etc.). This package only does `CheckPermission`.
- You need remote access or multiple processes sharing one logical SpiceDB. Run a real
  SpiceDB server instead.

Checks are always **fully consistent** (evaluated at the datastore head revision).

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
