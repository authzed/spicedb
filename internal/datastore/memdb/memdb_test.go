package memdb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	test "github.com/authzed/spicedb/pkg/datastore/test"
	ns "github.com/authzed/spicedb/pkg/namespace"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

var memdbFactory = test.NewTesterFactory(ErrSerialization)

type memDBTest struct{}

func (memDBTest) New(_ testing.TB, revisionQuantization, _, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
	return NewMemdbDatastore(watchBufferLength, revisionQuantization, gcWindow)
}

func TestMemdbDatastore(t *testing.T) {
	// ConcurrentWrite tests require row-level locking; memdb uses a global write lock
	// and would deadlock if two write transactions were opened concurrently.
	test.AllWithExceptions(t, memdbFactory.NewTester(memDBTest{}), test.WithCategories(test.ConcurrentWriteCategory))
}

func TestConcurrentWritePanic(t *testing.T) {
	require := require.New(t)

	ds, err := NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	require.NoError(err)

	ctx := t.Context()
	recoverErr := errors.New("panic")

	// Make the namespace very large to increase the likelihood of overlapping
	relationList := make([]*corev1.Relation, 0, 1000)
	for i := range 1000 {
		relationList = append(relationList, ns.MustRelation(fmt.Sprintf("reader%d", i), nil))
	}

	numPanics := uint64(0)
	require.Eventually(func() bool {
		_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			g := errgroup.Group{}
			g.Go(func() (err error) {
				defer func() {
					if rec := recover(); rec != nil {
						atomic.AddUint64(&numPanics, 1)
						err = recoverErr
					}
				}()

				return rwt.LegacyWriteNamespaces(ctx, ns.Namespace(
					"resource",
					relationList...,
				))
			})

			g.Go(func() (err error) {
				defer func() {
					if rec := recover(); rec != nil {
						atomic.AddUint64(&numPanics, 1)
						err = recoverErr
					}
				}()

				return rwt.LegacyWriteNamespaces(ctx, ns.Namespace("user", relationList...))
			})

			return g.Wait()
		})
		return numPanics > 0
	}, 3*time.Second, 10*time.Millisecond)
	require.ErrorIs(err, recoverErr)
}

func TestConcurrentWriteRelsSucceed(t *testing.T) {
	require := require.New(t)

	ds, err := NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	require.NoError(err)

	ctx := t.Context()

	// With sync.Cond serialization, concurrent writes block until the active write
	// finishes rather than failing immediately, so all goroutines succeed.
	g := errgroup.Group{}

	for i := range 50 {
		g.Go(func() error {
			_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				updates := []tuple.RelationshipUpdate{}
				for j := range 500 {
					updates = append(updates, tuple.Touch(tuple.MustParse(fmt.Sprintf("document:doc-%d-%d#viewer@user:tom", i, j))))
				}

				return rwt.WriteRelationships(ctx, updates)
			}, options.WithDisableRetries(true))
			return err
		})
	}

	require.NoError(g.Wait())
}

// TestConcurrentWriteRevisionInversionLosesRead deterministically reproduces
// https://github.com/authzed/spicedb/issues/3212: newRevisionID is stamped
// before the write-serialization lock is acquired, so a transaction (B) that
// starts after another (A) can still commit before it, even though B's
// revision number is numerically greater than A's. Recording snapshots in
// commit order rather than revision order breaks the sortedness that
// SnapshotReader's binary search relies on, and a fully-consistent read at
// A's own acknowledged revision ends up resolving to B's earlier snapshot,
// which does not contain A's write.
func TestConcurrentWriteRevisionInversionLosesRead(t *testing.T) {
	require := require.New(t)

	ds, err := NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	require.NoError(err)
	mds := ds.(*memdbDatastore)

	ctx := t.Context()

	aEntered := make(chan struct{})
	releaseA := make(chan struct{})
	aDone := make(chan struct{})

	var revA datastore.Revision
	var errA error
	go func() {
		defer close(aDone)
		revA, errA = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			// newRevisionID has already been called for this transaction by the
			// time f runs, so closing this channel signals that A's revision
			// number has been stamped, even though A hasn't touched the
			// write-transaction lock yet.
			close(aEntered)
			<-releaseA
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
				tuple.Touch(tuple.MustParse("document:doc-a#viewer@user:tom")),
			})
		}, options.WithDisableRetries(true))
	}()

	<-aEntered

	revB, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Touch(tuple.MustParse("document:doc-b#viewer@user:tom")),
		})
	}, options.WithDisableRetries(true))
	require.NoError(err)

	close(releaseA)
	<-aDone
	require.NoError(errA)

	// Sanity check on the harness itself: B was assigned its revision strictly
	// after A signaled entry, so it must be the numerically greater one. This
	// holds regardless of whether the storage-ordering bug is present.
	require.True(revB.GreaterThan(revA), "expected B's revision (%v) to be greater than A's (%v)", revB, revA)

	// Diagnostic only (not an invariant the fix must preserve either way):
	// log where each ended up in storage order, for visibility into whether
	// commit order matched revision order on this run.
	indexOf := func(r datastore.Revision) int {
		mds.RLock()
		defer mds.RUnlock()
		for i, snap := range mds.revisions {
			if snap.revision.Equal(r) {
				return i
			}
		}
		return -1
	}
	t.Logf("storage order: A(rev=%v) at index %d, B(rev=%v) at index %d", revA, indexOf(revA), revB, indexOf(revB))

	// The actual bug/fix boundary: a fully-consistent read at A's own
	// acknowledged revision must see A's write. Before the fix, this could
	// resolve to B's earlier snapshot instead, since B committed first.
	reader := ds.SnapshotReader(revA)
	it, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{OptionalResourceType: "document"})
	require.NoError(err)
	rels, err := datastore.IteratorToSlice(it)
	require.NoError(err)

	var sawA bool
	for _, rel := range rels {
		if rel.Resource.ObjectID == "doc-a" {
			sawA = true
		}
	}
	require.True(sawA, "BUG REPRODUCED: relationship written by transaction A is invisible when reading at A's own returned revision %v", revA)
}

func TestAnythingAfterCloseDoesNotPanic(t *testing.T) {
	require := require.New(t)

	ds, err := NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	require.NoError(err)

	lowestRevision, err := ds.HeadRevision(t.Context())
	require.NoError(err)

	err = ds.Close()
	require.NoError(err)

	_, errChan := ds.Watch(t.Context(), lowestRevision.Revision, datastore.WatchJustRelationships())

	select {
	case err := <-errChan:
		require.ErrorIs(err, ErrMemDBIsClosed)
	case <-time.After(time.Second):
		require.Fail("expected an error but waited too long")
	}

	_, err = ds.Statistics(t.Context())
	require.ErrorIs(err, ErrMemDBIsClosed)

	err = ds.CheckRevision(t.Context(), lowestRevision.Revision)
	require.ErrorIs(err, ErrMemDBIsClosed)

	_, err = ds.OptimizedRevision(t.Context())
	require.ErrorIs(err, ErrMemDBIsClosed)

	reader := ds.SnapshotReader(datastore.NoRevision)
	_, err = reader.CountRelationships(t.Context(), "blah")
	require.ErrorIs(err, ErrMemDBIsClosed)
}
