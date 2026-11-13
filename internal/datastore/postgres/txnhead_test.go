//go:build datastore && postgres

package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestStableHeadRevision(t *testing.T) {
	t.Parallel()

	b := testdatastore.RunPostgresForTesting(t, postgresConfig.pgVersion, postgresConfig.pgbouncer)

	txnHeadOptions := []Option{
		RevisionQuantization(0),
		GCWindow(1 * time.Millisecond),
		GCInterval(disableBackgroundGC),
		WatchBufferLength(1),
		WithRevisionHeartbeat(false),
		WithStableHeadRevision(true),
	}

	t.Run("RevisionInversion", createDatastoreTest(b, RevisionInversionTest, txnHeadOptions...))
	t.Run("ConcurrentRevisionHead", createDatastoreTest(b, ConcurrentRevisionHeadTest, txnHeadOptions...))
	t.Run("HeadRevisionDoesNotConsumeXID", createDatastoreTest(b, HeadRevisionDoesNotConsumeXIDTest, txnHeadOptions...))
	t.Run("MatchesWrittenRevision", createDatastoreTest(b, TxnHeadMatchesWrittenRevisionTest, txnHeadOptions...))
	t.Run("IgnoresForeignTransactions", createDatastoreTest(b, TxnHeadIgnoresForeignTransactionsTest, txnHeadOptions...))
	t.Run("UncommittedWriteInvisible", createDatastoreTest(b, TxnHeadUncommittedWriteInvisibleTest, txnHeadOptions...))
	t.Run("LaterXIDCommitsFirst", createDatastoreTest(b, TxnHeadLaterXIDCommitsFirstTest(false), txnHeadOptions...))
	t.Run("LaterXIDCommitsFirstInProgressList", createDatastoreTest(b, TxnHeadLaterXIDCommitsFirstTest(true), txnHeadOptions...))
	t.Run("TwoEarlierXIDsCommitLast", createDatastoreTest(b, TxnHeadTwoEarlierXIDsCommitLastTest, txnHeadOptions...))
}

// holdWrite starts a ReadWriteTx that writes one relationship and then blocks until released,
// so that the test can observe the head revision while the write is uncommitted. Callers must
// defer release so that a failed assertion does not leave the writer blocked.
func holdWrite(t *testing.T, ds datastore.Datastore, resourceID string) (release func(), wait func() error) {
	t.Helper()
	written := make(chan struct{})
	releaseCh := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			err := rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
				tuple.Touch(tuple.MustParse("resource:" + resourceID + "#reader@user:1")),
			})
			close(written)
			<-releaseCh
			return err
		})
		done <- err
	}()
	<-written

	var once sync.Once
	release = func() { once.Do(func() { close(releaseCh) }) }
	return release, func() error { return <-done }
}

func TxnHeadMatchesWrittenRevisionTest(t *testing.T, ds datastore.Datastore) {
	ctx := t.Context()
	writeTestNamespaces(t, ds)

	written, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Touch(tuple.MustParse("resource:1#reader@user:1")),
		})
	})
	require.NoError(t, err)

	head, err := ds.HeadRevision(ctx)
	require.NoError(t, err)
	require.True(t, head.Revision.Equal(written), "head %s, written %s", head.Revision, written)
}

func TxnHeadIgnoresForeignTransactionsTest(t *testing.T, ds datastore.Datastore) {
	ctx := t.Context()
	pds := ds.(*pgDatastore)
	writeTestNamespaces(t, ds)

	before, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	foreign, err := pds.readPool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = foreign.Rollback(ctx) }()
	_, err = foreign.Exec(ctx, `CREATE TABLE foreign_writes (id bigserial PRIMARY KEY)`)
	require.NoError(t, err)

	for range 5 {
		_, err := pds.readPool.Exec(ctx, `SELECT pg_current_xact_id()`)
		require.NoError(t, err)
	}

	during, err := ds.HeadRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, before.Revision.String(), during.Revision.String())

	require.NoError(t, foreign.Commit(ctx))
	_, err = pds.readPool.Exec(ctx, `INSERT INTO foreign_writes DEFAULT VALUES`)
	require.NoError(t, err)

	after, err := ds.HeadRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, before.Revision.String(), after.Revision.String())
}

func TxnHeadUncommittedWriteInvisibleTest(t *testing.T, ds datastore.Datastore) {
	ctx := t.Context()
	writeTestNamespaces(t, ds)

	release, wait := holdWrite(t, ds, "pending")
	defer release()

	headBefore, err := ds.HeadRevision(ctx)
	require.NoError(t, err)
	require.Empty(t, readResource(t, ds, headBefore.Revision, "pending"))

	release()
	require.NoError(t, wait())

	headAfter, err := ds.HeadRevision(ctx)
	require.NoError(t, err)
	require.Len(t, readResource(t, ds, headAfter.Revision, "pending"), 1)
	require.Empty(t, readResource(t, ds, headBefore.Revision, "pending"), "revision handed out before the commit must stay stable")
}

// TxnHeadLaterXIDCommitsFirstTest opens transaction A, then opens and commits transaction B,
// then commits A. B's journal row carries the newest xid but its snapshot does not see A;
// the head revision must nevertheless see A once it has committed. With commitForeignBetween,
// another transaction commits between A's start and B's start, which moves B's snapshot xmax
// past A and places A in its in-progress list instead of beyond xmax.
func TxnHeadLaterXIDCommitsFirstTest(commitForeignBetween bool) datastoreTestFunc {
	return func(t *testing.T, ds datastore.Datastore) {
		ctx := t.Context()
		pds := ds.(*pgDatastore)
		writeTestNamespaces(t, ds)

		releaseA, waitA := holdWrite(t, ds, "a")
		defer releaseA()

		if commitForeignBetween {
			_, err := pds.readPool.Exec(ctx, `SELECT pg_current_xact_id()`)
			require.NoError(t, err)
		}

		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
				tuple.Touch(tuple.MustParse("resource:b#reader@user:1")),
			})
		})
		require.NoError(t, err)

		headBefore, err := ds.HeadRevision(ctx)
		require.NoError(t, err)
		require.Len(t, readResource(t, ds, headBefore.Revision, "b"), 1)
		require.Empty(t, readResource(t, ds, headBefore.Revision, "a"))

		releaseA()
		require.NoError(t, waitA())

		headAfter, err := ds.HeadRevision(ctx)
		require.NoError(t, err)
		require.Len(t, readResource(t, ds, headAfter.Revision, "a"), 1)
		require.Len(t, readResource(t, ds, headAfter.Revision, "b"), 1)
		require.Empty(t, readResource(t, ds, headBefore.Revision, "a"), "revision handed out before the commit must stay stable")
	}
}

// TxnHeadTwoEarlierXIDsCommitLastTest holds two writes open, commits a third, then commits the
// two held writes. Both of their xids lie past the snapshot xmax of the newest journal row and
// each must be marked complete on its own.
func TxnHeadTwoEarlierXIDsCommitLastTest(t *testing.T, ds datastore.Datastore) {
	ctx := t.Context()
	writeTestNamespaces(t, ds)

	releaseA1, waitA1 := holdWrite(t, ds, "a1")
	defer releaseA1()
	releaseA2, waitA2 := holdWrite(t, ds, "a2")
	defer releaseA2()

	_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Touch(tuple.MustParse("resource:b#reader@user:1")),
		})
	})
	require.NoError(t, err)

	releaseA1()
	require.NoError(t, waitA1())
	releaseA2()
	require.NoError(t, waitA2())

	head, err := ds.HeadRevision(ctx)
	require.NoError(t, err)
	for _, resourceID := range []string{"a1", "a2", "b"} {
		require.Len(t, readResource(t, ds, head.Revision, resourceID), 1, resourceID)
	}
}

func writeTestNamespaces(t *testing.T, ds datastore.Datastore) {
	t.Helper()
	_, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(ctx, namespace.Namespace(
			"resource",
			namespace.MustRelation("reader", nil),
		), namespace.Namespace("user"))
	})
	require.NoError(t, err)
}

func readResource(t *testing.T, ds datastore.Datastore, rev datastore.Revision, resourceID string) []tuple.Relationship {
	t.Helper()
	it, err := ds.SnapshotReader(rev).QueryRelationships(t.Context(), datastore.RelationshipsFilter{
		OptionalResourceType: "resource",
		OptionalResourceIds:  []string{resourceID},
	})
	require.NoError(t, err)
	found, err := datastore.IteratorToSlice(it)
	require.NoError(t, err)
	return found
}
