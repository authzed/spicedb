//go:build datastore

package crdb

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/crdb/schema"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	ns "github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/tuple"
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

// extractCRDBDatastore unwraps a (possibly proxy-wrapped) datastore.Datastore
// into the concrete *crdbDatastore, so tests can reach unexported fields such
// as readPool. Mirrors the unwrapping approach used elsewhere in this package
// (see datastore.UnwrapAs and partitioner_test.go).
func extractCRDBDatastore(t *testing.T, ds datastore.Datastore) *crdbDatastore {
	t.Helper()
	cds := datastore.UnwrapAs[*crdbDatastore](ds)
	require.NotNil(t, cds, "expected to unwrap into *crdbDatastore, got %T", ds)
	return cds
}

// TestRelationshipDualWrite verifies that WriteRelationships also inserts a
// self-contained changelog row per mutation, in the same transaction, when
// the changelog-watch flag is enabled.
func TestRelationshipDualWrite(t *testing.T) {
	engine := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())
	createDatastoreTest(engine, func(t *testing.T, ds datastore.Datastore) {
		ctx := t.Context()
		rel := tuple.MustParse("document:doc1#viewer@user:alice")
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
		})
		require.NoError(t, err)

		cds := extractCRDBDatastore(t, ds)
		var count int
		require.NoError(t, cds.readPool.QueryRowFunc(ctx, func(_ context.Context, row pgx.Row) error {
			return row.Scan(&count)
		}, "SELECT count(*) FROM "+schema.TableRelationshipChangelog+" WHERE kind = 'rel' AND object_id = 'doc1'"))
		require.Equal(t, 1, count, "expected exactly one changelog row for the write")
	}, ExperimentalChangelogWatch(true), WithAcquireTimeout(5*time.Second))(t)
}

// TestSchemaDualWrite verifies that LegacyWriteNamespaces also inserts a
// self-contained schema changelog row in the same transaction, when the
// changelog-watch flag is enabled.
func TestSchemaDualWrite(t *testing.T) {
	engine := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())
	createDatastoreTest(engine, func(t *testing.T, ds datastore.Datastore) {
		ctx := t.Context()
		nsDef := ns.Namespace("document", ns.MustRelation("viewer", nil))
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.LegacyWriteNamespaces(ctx, nsDef)
		})
		require.NoError(t, err)

		cds := extractCRDBDatastore(t, ds)
		var count int
		require.NoError(t, cds.readPool.QueryRowFunc(ctx, func(_ context.Context, row pgx.Row) error {
			return row.Scan(&count)
		}, "SELECT count(*) FROM "+schema.TableRelationshipChangelog+" WHERE kind = 'schema' AND schema_kind = 'namespace' AND definition_name = 'document'"))
		require.Equal(t, 1, count)
	}, ExperimentalChangelogWatch(true), WithAcquireTimeout(5*time.Second))(t)
}
