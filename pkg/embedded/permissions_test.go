package embedded_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	dscfg "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/embedded"
)

const testBootstrap = `
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
  document:doc1#viewer@user:alice

  document:doc1#caveated_viewer@user:bob[is_tuesday]
`

func newTestPermissions(t *testing.T, mode datalayer.SchemaMode) *embedded.Permissions {
	t.Helper()
	ctx := t.Context()

	ds, err := dscfg.NewDatastore(ctx,
		dscfg.DefaultDatastoreConfig().ToOption(),
		dscfg.SetBootstrapFileContents(map[string][]byte{"test.yaml": []byte(testBootstrap)}),
		dscfg.WithCaveatTypeSet(caveattypes.Default.TypeSet),
		dscfg.WithBootstrapSchemaMode(mode),
	)
	require.NoError(t, err)

	// A stored-schema cache is only meaningful (and only exercises the schema-tied compiled
	// caveat cache) when reading from the unified schema.
	var schemaCacheCost int64
	if mode.ReadsFromNew() {
		schemaCacheCost = 1 << 20
	}

	perms, err := embedded.NewPermissions(embedded.Config{
		Datastore:               ds,
		SchemaMode:              mode,
		SchemaCacheMaxCostBytes: schemaCacheCost,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, perms.Close())
		require.NoError(t, ds.Close())
	})
	return perms
}

func TestPermissionsCheck(t *testing.T) {
	modes := []struct {
		name string
		mode datalayer.SchemaMode
	}{
		{"legacy", datalayer.SchemaModeReadLegacyWriteLegacy},
		{"single_store", datalayer.SchemaModeReadNewWriteNew},
	}

	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			ctx := t.Context()
			perms := newTestPermissions(t, m.mode)

			t.Run("member", func(t *testing.T) {
				res, err := perms.Check(ctx, embedded.CheckRequest{
					ResourceType: "document", ResourceID: "doc1", Permission: "view",
					SubjectType: "user", SubjectID: "alice",
				})
				require.NoError(t, err)
				require.True(t, res.HasPermission)
				require.False(t, res.IsConditional)
			})

			t.Run("non_member", func(t *testing.T) {
				res, err := perms.Check(ctx, embedded.CheckRequest{
					ResourceType: "document", ResourceID: "doc1", Permission: "view",
					SubjectType: "user", SubjectID: "carol",
				})
				require.NoError(t, err)
				require.False(t, res.HasPermission)
				require.False(t, res.IsConditional)
			})

			t.Run("caveated_missing_context", func(t *testing.T) {
				res, err := perms.Check(ctx, embedded.CheckRequest{
					ResourceType: "document", ResourceID: "doc1", Permission: "caveated_view",
					SubjectType: "user", SubjectID: "bob",
				})
				require.NoError(t, err)
				require.False(t, res.HasPermission)
				require.True(t, res.IsConditional)
				require.Contains(t, res.MissingContext, "day")
			})

			t.Run("caveated_satisfied", func(t *testing.T) {
				res, err := perms.Check(ctx, embedded.CheckRequest{
					ResourceType: "document", ResourceID: "doc1", Permission: "caveated_view",
					SubjectType: "user", SubjectID: "bob",
					CaveatContext: map[string]any{"day": "tuesday"},
				})
				require.NoError(t, err)
				require.True(t, res.HasPermission)
				require.False(t, res.IsConditional)
			})

			t.Run("caveated_unsatisfied", func(t *testing.T) {
				res, err := perms.Check(ctx, embedded.CheckRequest{
					ResourceType: "document", ResourceID: "doc1", Permission: "caveated_view",
					SubjectType: "user", SubjectID: "bob",
					CaveatContext: map[string]any{"day": "monday"},
				})
				require.NoError(t, err)
				require.False(t, res.HasPermission)
				require.False(t, res.IsConditional)
			})

			// Repeated checks must return consistent results (exercises the schema-tied
			// compiled-caveat cache on the unified-schema path).
			t.Run("repeated_consistent", func(t *testing.T) {
				for i := 0; i < 3; i++ {
					res, err := perms.Check(ctx, embedded.CheckRequest{
						ResourceType: "document", ResourceID: "doc1", Permission: "caveated_view",
						SubjectType: "user", SubjectID: "bob",
						CaveatContext: map[string]any{"day": "tuesday"},
					})
					require.NoError(t, err)
					require.True(t, res.HasPermission)
				}
			})
		})
	}
}

func TestNewPermissionsRequiresDatastore(t *testing.T) {
	_, err := embedded.NewPermissions(embedded.Config{})
	require.Error(t, err)
}
