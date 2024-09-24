package shared

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestApplySchemaChanges(t *testing.T) {
	require := require.New(t)
	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	// Write the initial schema.
	ds, _ := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, `
		definition user {}

		definition document {
			relation viewer: user
			permission view = viewer
		}

		caveat hasFortyTwo(value int) {
          value == 42
        }
	`, nil, require)

	// Update the schema and ensure it works.
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source: input.Source("schema"),
		SchemaString: `
			definition user {}

			definition organization {
				relation member: user
				permission admin = member
			}

			caveat catchTwentyTwo(value int) {
			  value == 22
			}
		`,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(err)

	validated, err := ValidateSchemaChanges(context.Background(), compiled, false)
	require.NoError(err)

	_, err = ds.ReadWriteTx(context.Background(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		applied, err := ApplySchemaChanges(context.Background(), rwt, validated)
		require.NoError(err)

		require.Equal(applied.NewObjectDefNames, []string{"organization"})
		require.Equal(applied.RemovedObjectDefNames, []string{"document"})
		require.Equal(applied.NewCaveatDefNames, []string{"catchTwentyTwo"})
		require.Equal(applied.RemovedCaveatDefNames, []string{"hasFortyTwo"})

		orgDef, err := rwt.LookupNamespacesWithNames(ctx, []string{"organization"})
		require.NoError(err)
		require.Len(orgDef, 1)

		require.NotEmpty(orgDef[0].Definition.Relation[0].CanonicalCacheKey)
		require.NotEmpty(orgDef[0].Definition.Relation[1].CanonicalCacheKey)
		return nil
	})
	require.NoError(err)
}
