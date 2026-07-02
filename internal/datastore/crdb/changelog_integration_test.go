//go:build datastore

package crdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/crdb/schema"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	ns "github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/tuple"
)

// testBulkSource is a slice-backed datastore.BulkWriteRelationshipSource for
// exercising BulkLoad in tests. It yields a fresh *tuple.Relationship on each
// call (never reusing the same pointer), mirroring the shape of
// testfixtures.BulkRelationshipGenerator.
type testBulkSource struct {
	rels []tuple.Relationship
	next int
}

func (s *testBulkSource) Next(_ context.Context) (*tuple.Relationship, error) {
	if s.next >= len(s.rels) {
		return nil, nil
	}
	rel := s.rels[s.next]
	s.next++
	return &rel, nil
}

var _ datastore.BulkWriteRelationshipSource = &testBulkSource{}

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

// TestChangelogWatchNudgeDeliversQuickly verifies that the changefeed "nudge"
// wakes the poll loop well before the (deliberately long) ticker interval
// fires. It opens a watch with a 30s CheckpointInterval, waits long enough
// for the nudge's underlying changefeed job to finish standing up (CRDB
// changefeed job creation is a several-second, one-time cost, distinct from
// the per-event latency under test), then writes a relationship and asserts
// the change arrives well under the interval -- which is only possible if
// the nudge, and not the 30s ticker, triggered the poll.
func TestChangelogWatchNudgeDeliversQuickly(t *testing.T) {
	engine := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())
	createDatastoreTest(engine, func(t *testing.T, ds datastore.Datastore) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		head, err := ds.HeadRevision(ctx)
		require.NoError(t, err)
		changes, errchan := ds.Watch(ctx, head.Revision, datastore.WatchOptions{
			Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
			CheckpointInterval: 30 * time.Second,
		})

		// Let the nudge's changefeed job finish standing up. Job creation in
		// CockroachDB is a several-second, one-time fixed cost, orthogonal to
		// how quickly a nudge wakes the poll loop once the changefeed is
		// live; draining it here isolates the per-event latency we actually
		// want to measure below.
		time.Sleep(10 * time.Second)

		start := time.Now()
		rel := tuple.MustParse("document:doc1#viewer@user:alice")
		_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
		})
		require.NoError(t, err)
		for {
			select {
			case change, ok := <-changes:
				require.True(t, ok)
				for _, rc := range change.RelationshipChanges {
					if rc.Relationship.Resource.ObjectID == "doc1" {
						require.Less(t, time.Since(start), 10*time.Second, "nudge should deliver well before the 30s interval")
						return
					}
				}
			case err := <-errchan:
				require.NoError(t, err)
			case <-time.After(15 * time.Second):
				t.Fatal("nudge did not deliver before near the interval")
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

// TestChangelogWatchRejectsStaleCursor verifies that opening a changelog
// Watch with an afterRevision far older than the datastore's GC window fails
// fast with the standard stale-revision error, rather than silently polling
// forward from a cursor whose changelog history may have already been
// garbage collected.
func TestChangelogWatchRejectsStaleCursor(t *testing.T) {
	engine := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())
	createDatastoreTest(engine, func(t *testing.T, ds datastore.Datastore) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// A revision far older than the GC window.
		staleDecimal := decimal.NewFromInt(1) // ~1970, well outside gc window
		stale, err := revisions.NewForHLC(staleDecimal)
		require.NoError(t, err)

		_, errchan := ds.Watch(ctx, stale, datastore.WatchOptions{
			Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
			CheckpointInterval: 100 * time.Millisecond,
		})
		select {
		case err := <-errchan:
			require.Error(t, err)
			require.ErrorAs(t, err, &datastore.InvalidRevisionError{})
		case <-time.After(10 * time.Second):
			t.Fatal("expected a stale-revision error")
		}
	}, ExperimentalChangelogWatch(true), WithAcquireTimeout(5*time.Second))(t)
}

// TestChangelogWatchSeesBulkLoad is the motivating scenario for the
// changelog-table Watch feature: CRDB changefeeds stall resolved timestamps
// during bulk loads, so the poll-based Watch must be able to observe
// bulk-loaded relationships via the changelog table instead. It bulk-loads a
// batch of relationships in a single transaction and asserts every one of
// them arrives over the poll-based watch.
func TestChangelogWatchSeesBulkLoad(t *testing.T) {
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

		const total = 500
		rels := make([]tuple.Relationship, 0, total)
		for i := 0; i < total; i++ {
			rels = append(rels, tuple.MustParse(fmt.Sprintf("document:doc%d#viewer@user:alice", i)))
		}
		_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			_, berr := rwt.BulkLoad(ctx, &testBulkSource{rels: rels})
			return berr
		})
		require.NoError(t, err)

		seen := map[string]struct{}{}
		deadline := time.After(60 * time.Second)
		for len(seen) < total {
			select {
			case change, ok := <-changes:
				require.True(t, ok)
				for _, rc := range change.RelationshipChanges {
					seen[rc.Relationship.Resource.ObjectID] = struct{}{}
				}
			case err := <-errchan:
				require.NoError(t, err)
			case <-deadline:
				t.Fatalf("only saw %d/%d bulk-loaded relationships over changelog watch", len(seen), total)
			}
		}
	}, ExperimentalChangelogWatch(true), WithAcquireTimeout(5*time.Second))(t)
}

// TestChangelogWatchSeesBulkLoadAcrossChunks is a regression test for
// chunking the bulk changelog INSERT in appendRelationshipChangelogBatch.
// Postgres/pgx cap bound parameters at 65535 per statement; at 14 columns
// per changelog row, a single multi-row INSERT can only hold a few thousand
// rows before it would overflow that limit. appendRelationshipChangelogBatch
// splits large batches into chunks, but must keep every row's ordinal
// unique and monotonically increasing across chunks (change_ts is constant
// for the whole transaction via cluster_logical_timestamp(), so uniqueness
// of the (change_ts, ordinal) primary key depends entirely on the ordinal).
//
// This test lowers changelogInsertChunkSize to a tiny value so a small
// bulk load spans multiple chunks, then asserts every relationship is
// still observed over the changelog Watch -- proving all chunks landed and
// no ordinals collided across chunk boundaries.
func TestChangelogWatchSeesBulkLoadAcrossChunks(t *testing.T) {
	const testChunkSize = 3
	original := changelogInsertChunkSize
	changelogInsertChunkSize = testChunkSize
	t.Cleanup(func() { changelogInsertChunkSize = original })

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

		// 7 rows over a chunk size of 3 forces 3 chunks (3 + 3 + 1), so the
		// ordinal counter must span multiple INSERT statements correctly.
		const total = 7
		rels := make([]tuple.Relationship, 0, total)
		for i := 0; i < total; i++ {
			rels = append(rels, tuple.MustParse(fmt.Sprintf("document:chunkdoc%d#viewer@user:alice", i)))
		}
		_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			_, berr := rwt.BulkLoad(ctx, &testBulkSource{rels: rels})
			return berr
		})
		require.NoError(t, err)

		seen := map[string]struct{}{}
		deadline := time.After(30 * time.Second)
		for len(seen) < total {
			select {
			case change, ok := <-changes:
				require.True(t, ok)
				for _, rc := range change.RelationshipChanges {
					seen[rc.Relationship.Resource.ObjectID] = struct{}{}
				}
			case err := <-errchan:
				require.NoError(t, err)
			case <-deadline:
				t.Fatalf("only saw %d/%d bulk-loaded relationships over changelog watch across chunks", len(seen), total)
			}
		}
	}, ExperimentalChangelogWatch(true), WithAcquireTimeout(5*time.Second))(t)
}
