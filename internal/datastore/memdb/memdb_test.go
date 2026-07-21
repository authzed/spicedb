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

// TestConcurrentWrite covers revision visibility when two write transactions overlap.
//   - each transaction's write should be visible when reading at its own returned revision (read-your-writes)
//   - a write should NOT be visible at revisions below its own (consistent snapshot)
//   - a write SHOULD be visible at every revision at or above its own
//   - the head revision sees every committed write
// TODO: should this be a datastore conformance test (not specific to memdb)?
func TestConcurrentWrite(t *testing.T) {
	ds, err := NewMemdbDatastore(0, 1*time.Hour, 1*time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, ds.Close())
	})

	// docsAt returns the set of document object IDs visible at the given revision.
	docsAt := func(rev datastore.Revision) map[string]bool {
		reader := ds.SnapshotReader(rev)
		it, err := reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{OptionalResourceType: "document"})
		require.NoError(t, err)
		rels, err := datastore.IteratorToSlice(it)
		require.NoError(t, err)

		seen := map[string]bool{}
		for _, rel := range rels {
			seen[rel.Resource.ObjectID] = true
		}
		return seen
	}

	// Execute many iterations so that one run is enough to expose ordering problems
	const iterations = 10_000
	for i := 0; i < iterations; i++ {
		// Unique names per iteration so that visibility assertions cannot be
		// satisfied by a previous iteration's writes.
		docA := fmt.Sprintf("doc-a-%d", i)
		docB := fmt.Sprintf("doc-b-%d", i)
		relA := tuple.MustParse(fmt.Sprintf("document:%s#viewer@user:tom", docA))
		relB := tuple.MustParse(fmt.Sprintf("document:%s#viewer@user:tom", docB))

		var (
			revA                datastore.Revision
			errA                error
			waitUntilAStarts    = make(chan struct{}, 1)
			waitUntilBCompletes = make(chan struct{}, 1)
			waitUntilADone      = make(chan struct{})
		)
		go func() {
			defer close(waitUntilADone)
			revA, errA = ds.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				waitUntilAStarts <- struct{}{}
				<-waitUntilBCompletes // held until B has fully committed below
				return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
					tuple.Touch(relA),
				})
			}, options.WithDisableRetries(true))
		}()

		<-waitUntilAStarts // A is now blocked

		// Transaction B runs to completion while A is blocked
		revB, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
				tuple.Touch(relB),
			})
		}, options.WithDisableRetries(true))
		require.NoError(t, err)

		waitUntilBCompletes <- struct{}{} // unblocks A
		<-waitUntilADone
		require.NoError(t, errA)

		require.False(t, revA.Equal(revB), "iteration %d: concurrent transactions must be assigned distinct revisions, both got %v", i, revA)

		// Each transaction's write must be visible at its own returned revision.
		require.True(t, docsAt(revA)[docA], "iteration %d: A's write is invisible when reading at A's own returned revision %v", i, revA)
		require.True(t, docsAt(revB)[docB], "iteration %d: B's write is invisible when reading at B's own returned revision %v", i, revB)

		type write struct {
			rev datastore.Revision
			doc string
		}
		earlier, later := write{revA, docA}, write{revB, docB}
		if later.rev.LessThan(earlier.rev) {
			earlier, later = later, earlier
		}

		// The write committed at the later revision must not be visible at the earlier revision
		atEarlier := docsAt(earlier.rev)
		require.True(t, atEarlier[earlier.doc], "iteration %d: %s is invisible at its own revision %v", i, earlier.doc, earlier.rev)
		require.False(t, atEarlier[later.doc], "iteration %d: %s was committed at later revision %v but is visible at earlier revision %v", i, later.doc, later.rev, earlier.rev)

		// Both writes must be visible at the later revision
		atLater := docsAt(later.rev)
		require.True(t, atLater[earlier.doc], "iteration %d: %s is visible at revision %v but disappears at later revision %v", i, earlier.doc, earlier.rev, later.rev)
		require.True(t, atLater[later.doc], "iteration %d: %s is invisible at its own revision %v", i, later.doc, later.rev)

		// The head revision must see every committed write
		head, err := ds.HeadRevision(t.Context())
		require.NoError(t, err)
		atHead := docsAt(head.Revision)
		require.True(t, atHead[docA] && atHead[docB], "iteration %d: head revision %v is missing committed writes: %v", i, head.Revision, atHead)

		// Delete this iteration's relationships so the relationship table does
		// not grow across iterations: memdb scans the whole namespace per
		// query, so an ever-growing table would make this test quadratic.
		// Earlier revisions retain their snapshots, so this does not affect
		// the assertions above.
		_, err = ds.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
				tuple.Delete(relA),
				tuple.Delete(relB),
			})
		}, options.WithDisableRetries(true))
		require.NoError(t, err)
	}
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
