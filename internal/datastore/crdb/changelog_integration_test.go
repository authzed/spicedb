//go:build datastore

package crdb

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/crdb/schema"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
)

// TestChangelogTableCreatedWhenEnabled verifies that newCRDBDatastore creates
// the experimental relationship_changelog table when the changelog-watch flag
// is enabled. It constructs the datastore directly via the unexported
// newCRDBDatastore (rather than the exported NewCRDBDatastore, which wraps the
// result in datastore.NewSeparatingContextDatastoreProxy) so the test can
// reach the concrete *crdbDatastore and inspect its readPool.
func TestChangelogTableCreatedWhenEnabled(t *testing.T) {
	engine := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())

	ctx := t.Context()
	ds := engine.NewDatastore(t, func(engine, uri string) datastore.Datastore {
		ds, err := newCRDBDatastore(ctx, uri, ExperimentalChangelogWatch(true))
		require.NoError(t, err)
		return ds
	})
	t.Cleanup(func() {
		_ = ds.Close()
	})

	cds, ok := ds.(*crdbDatastore)
	require.True(t, ok, "expected *crdbDatastore, got %T", ds)

	var exists bool
	require.NoError(t, cds.readPool.QueryRowFunc(ctx, func(_ context.Context, row pgx.Row) error {
		return row.Scan(&exists)
	}, "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)", schema.TableRelationshipChangelog))
	require.True(t, exists, "changelog table should exist when flag is on")
}
