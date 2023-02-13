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
	emptyDefaultPrefix := ""
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source: input.Source("schema"),
		SchemaString: `
			definition user {}

			definition organization {}

			caveat catchTwentyTwo(value int) {
			  value == 22
			}
		`,
	}, &emptyDefaultPrefix)
	require.NoError(err)

	validated, err := ValidateSchemaChanges(context.Background(), compiled, false)
	require.NoError(err)

	_, err = ds.ReadWriteTx(context.Background(), func(rwt datastore.ReadWriteTransaction) error {
		applied, err := ApplySchemaChanges(context.Background(), rwt, validated)
		require.NoError(err)

		require.Equal(applied.NewObjectDefNames, []string{"organization"})
		require.Equal(applied.RemovedObjectDefNames, []string{"document"})
		require.Equal(applied.NewCaveatDefNames, []string{"catchTwentyTwo"})
		require.Equal(applied.RemovedCaveatDefNames, []string{"hasFortyTwo"})
		return nil
	})
	require.NoError(err)
}
