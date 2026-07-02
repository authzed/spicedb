//go:build datastore

package crdb

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
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

// TestChangelogWatchReceivesWrite verifies that, when the changelog-watch flag
// is enabled, Watch serves relationship changes by polling the changelog table
// at a closed timestamp rather than by consuming a CRDB changefeed. It opens a
// watch from HeadRevision, writes a relationship, and asserts the change is
// delivered over the watch channel.
func TestChangelogWatchReceivesWrite(t *testing.T) {
	engine := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())
	createDatastoreTest(engine, func(t *testing.T, ds datastore.Datastore) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		head, err := ds.HeadRevision(ctx)
		require.NoError(t, err)

		changes, errchan := ds.Watch(ctx, head.Revision, datastore.WatchOptions{
			Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
			CheckpointInterval: 100 * time.Millisecond,
		})

		rel := tuple.MustParse("document:doc1#viewer@user:alice")
		_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
		})
		require.NoError(t, err)

		found := false
		deadline := time.After(30 * time.Second)
		for !found {
			select {
			case change, ok := <-changes:
				require.True(t, ok)
				for _, rc := range change.RelationshipChanges {
					if rc.Relationship.Resource.ObjectID == "doc1" {
						found = true
					}
				}
			case err := <-errchan:
				require.NoError(t, err)
			case <-deadline:
				t.Fatal("did not receive the write over changelog watch")
			}
		}
	}, ExperimentalChangelogWatch(true), WithAcquireTimeout(5*time.Second))(t)
}

// TestChangelogPollEmitsRowAtExactlyTarget is a boundary regression test for the
// off-by-one where a changelog row whose change_ts exactly equals a poll's
// target revision was read into the tracker but never emitted (the strict
// "< target" filter dropped it) while the cursor still advanced to target,
// permanently losing that row.
//
// It inserts a changelog row with a KNOWN explicit change_ts value T (bypassing
// dual-write so change_ts is under test control), then invokes the poll range
// helper with cursor < T and target == T, and asserts the row is emitted exactly
// once. Against the pre-fix strict filter this fails (the row is dropped); with
// the inclusive emission it passes.
func TestChangelogPollEmitsRowAtExactlyTarget(t *testing.T) {
	engine := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())
	createDatastoreTest(engine, func(t *testing.T, ds datastore.Datastore) {
		ctx := t.Context()
		cds := extractCRDBDatastore(t, ds)

		conn, err := pgx.Connect(ctx, cds.dburl)
		require.NoError(t, err)
		defer func() { _ = conn.Close(ctx) }()

		// Choose T from the cluster's own logical clock so it is a valid HLC value,
		// then insert a relationship changelog row stamped at exactly T.
		var target decimal.Decimal
		require.NoError(t, conn.QueryRow(ctx, "SELECT cluster_logical_timestamp()").Scan(&target))

		insert := "INSERT INTO " + schema.TableRelationshipChangelog + " (" +
			schema.ColChangeTS + ", " + schema.ColChangeOrdinal + ", " + schema.ColChangeKind + ", " +
			schema.ColNamespace + ", " + schema.ColObjectID + ", " + schema.ColRelation + ", " +
			schema.ColUsersetNamespace + ", " + schema.ColUsersetObjectID + ", " + schema.ColUsersetRelation + ", " +
			schema.ColChangeOperation + ", " + schema.ColChangeTTLExpiration +
			") VALUES ($1, 0, 'rel', 'document', 'boundary', 'viewer', 'user', 'alice', '...', 'touch', $2)"
		_, err = conn.Exec(ctx, insert, target, time.Now().Add(1*time.Hour))
		require.NoError(t, err)

		// cursor strictly less than T; target exactly equal to T.
		cursor := target.Sub(decimal.NewFromInt(1))

		tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(ctx) }()

		var emitted []datastore.RevisionChanges
		sendChange := func(change datastore.RevisionChanges) error {
			emitted = append(emitted, change)
			return nil
		}

		require.NoError(t, cds.pollChangelogRange(ctx, tx, datastore.WatchOptions{
			Content: datastore.WatchRelationships,
		}, cursor, target, cds.watchChangeBufferMaximumSize, sendChange))

		count := 0
		for _, change := range emitted {
			for _, rc := range change.RelationshipChanges {
				if rc.Relationship.Resource.ObjectID == "boundary" {
					count++
				}
			}
		}
		require.Equal(t, 1, count, "row at exactly the poll target must be emitted exactly once")
	}, ExperimentalChangelogWatch(true), WithAcquireTimeout(5*time.Second))(t)
}
