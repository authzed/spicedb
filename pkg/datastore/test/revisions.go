package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// RevisionQuantizationTest tests whether or not the requirements for revisions hold
// for a particular datastore.
func RevisionQuantizationTest(t *testing.T, tester DatastoreTester) {
	testCases := []struct {
		quantizationRange        time.Duration
		expectFindLowerRevisions bool
	}{
		{0 * time.Second, false},
		{100 * time.Millisecond, true},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("quantization%s", tc.quantizationRange), func(t *testing.T) {
			require := require.New(t)

			ds, err := tester.New(tc.quantizationRange, veryLargeGCInterval, veryLargeGCWindow, 1)
			require.NoError(err)

			ctx := context.Background()
			veryFirstRevision, err := ds.OptimizedRevision(ctx)
			require.NoError(err)

			postSetupRevision := setupDatastore(ds, require)
			require.True(postSetupRevision.GreaterThan(veryFirstRevision))

			// Create some revisions
			var writtenAt datastore.Revision
			tpl := makeTestRel("first", "owner")
			for i := 0; i < 10; i++ {
				writtenAt, err = common.WriteTuples(ctx, ds, tuple.UpdateOperationTouch, tpl)
				require.NoError(err)
			}
			require.True(writtenAt.GreaterThan(postSetupRevision))

			// Get the new now revision
			nowRevision, err := ds.HeadRevision(ctx)
			require.NoError(err)

			// Let the quantization window expire
			time.Sleep(tc.quantizationRange)

			// Now we should ONLY get revisions later than the now revision
			for start := time.Now(); time.Since(start) < 10*time.Millisecond; {
				testRevision, err := ds.OptimizedRevision(ctx)
				require.NoError(err)
				require.True(nowRevision.LessThan(testRevision) || nowRevision.Equal(testRevision))
			}
		})
	}
}

// RevisionSerializationTest tests whether the revisions generated by this datastore can
// be serialized and sent through the dispatch layer.
func RevisionSerializationTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	revToTest, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, testNamespace)
	})
	require.NoError(err)

	meta := dispatch.ResolverMeta{
		AtRevision:     revToTest.String(),
		DepthRemaining: 50,
		TraversalBloom: dispatch.MustNewTraversalBloomFilter(50),
	}
	require.NoError(meta.Validate())
}

// RevisionGCTest makes sure revision GC takes place, revisions out-side of the GC window
// are invalid, and revisions inside the GC window are valid.
func RevisionGCTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, 10*time.Millisecond, 300*time.Millisecond, 1)
	require.NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	testCaveat := createCoreCaveat(t)
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		if err := rwt.WriteNamespaces(ctx, ns.Namespace("foo/createdtxgc")); err != nil {
			return err
		}
		return rwt.WriteCaveats(ctx, []*core.CaveatDefinition{
			testCaveat,
		})
	})
	require.NoError(err)

	previousRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, testNamespace)
	})
	require.NoError(err)

	require.NoError(ds.CheckRevision(ctx, previousRev), "expected latest write revision to be within GC window")

	head, err := ds.HeadRevision(ctx)
	require.NoError(err)
	require.NoError(ds.CheckRevision(ctx, head), "expected head revision to be valid in GC Window")

	// Make sure GC kicks in after the window.
	time.Sleep(300 * time.Millisecond)

	gcable, ok := ds.(common.GarbageCollector)
	if ok {
		gcable.ResetGCCompleted()
		require.Eventually(func() bool { return gcable.HasGCRun() }, 5*time.Second, 50*time.Millisecond, "GC was never run as expected")
	}

	// FIXME currently the various datastores behave differently when a revision was requested and GC Window elapses.
	// this is due to the fact MySQL and PostgreSQL implement revisions as a snapshot, while CRDB, Spanner and MemDB
	// implement it as a timestamp.
	//
	// previous head revision is not valid if outside GC Window
	// require.Error(ds.CheckRevision(ctx, head), "expected head revision to be valid if out of GC window")
	//
	// latest state of the system is invalid if head revision is out of GC window
	//_, _, err = ds.SnapshotReader(head).ReadNamespaceByName(ctx, "foo/bar")
	// require.Error(err, "expected previously written schema to exist at out-of-GC window head")

	// check freshly fetched head revision is valid after GC window elapsed
	head, err = ds.HeadRevision(ctx)
	require.NoError(err)

	// check that we can read a caveat whose revision has been garbage collectged
	_, _, err = ds.SnapshotReader(head).ReadCaveatByName(ctx, testCaveat.Name)
	require.NoError(err, "expected previously written caveat should exist at head")

	// check that we can read the namespace which had its revision garbage collected
	_, _, err = ds.SnapshotReader(head).ReadNamespaceByName(ctx, "foo/createdtxgc")
	require.NoError(err, "expected previously written namespace should exist at head")

	// state of the system is also consistent at a recent call to head
	_, _, err = ds.SnapshotReader(head).ReadNamespaceByName(ctx, "foo/bar")
	require.NoError(err, "expected previously written schema to exist at head")

	// and that recent call to head revision is also valid, even after a GC window cycle without writes elapsed
	require.NoError(ds.CheckRevision(ctx, head), "expected freshly obtained head revision to be valid")

	// write happens, we get a new head revision
	newerRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, testNamespace)
	})
	require.NoError(err)
	require.NoError(ds.CheckRevision(ctx, newerRev), "expected newer head revision to be within GC Window")
	require.Error(ds.CheckRevision(ctx, previousRev), "expected revision head-1 to be outside GC Window")
}

func CheckRevisionsTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, 1000*time.Second, 300*time.Minute, 1)
	require.NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Write a new revision.
	writtenRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, ns.Namespace("foo/somethingnew1"))
	})
	require.NoError(err)
	require.NoError(ds.CheckRevision(ctx, writtenRev), "expected written revision to be valid in GC Window")

	head, err := ds.HeadRevision(ctx)
	require.NoError(err)

	// Check the head revision is valid
	require.NoError(ds.CheckRevision(ctx, head), "expected head revision to be valid in GC Window")

	// Write a new revision.
	writtenRev, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, ns.Namespace("foo/somethingnew2"))
	})
	require.NoError(err)
	require.NoError(ds.CheckRevision(ctx, writtenRev), "expected written revision to be valid in GC Window")

	// Check the previous head revision is still valid
	require.NoError(ds.CheckRevision(ctx, head), "expected previous revision to be valid in GC Window")

	// Get the updated head revision.
	head, err = ds.HeadRevision(ctx)
	require.NoError(err)

	// Check the new head revision is valid.
	require.NoError(ds.CheckRevision(ctx, head), "expected head revision to be valid in GC Window")
}

func SequentialRevisionsTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, 10*time.Second, 300*time.Minute, 1)
	require.NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var previous datastore.Revision
	for i := 0; i < 50; i++ {
		head, err := ds.HeadRevision(ctx)
		require.NoError(err)
		require.NoError(ds.CheckRevision(ctx, head), "expected head revision to be valid in GC Window")

		if previous != nil {
			require.True(head.GreaterThan(previous) || head.Equal(previous))
		}

		previous = head
	}
}

func ConcurrentRevisionsTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, 10*time.Second, 300*time.Minute, 1)
	require.NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(10)

	startingRev, err := ds.HeadRevision(ctx)
	require.NoError(err)

	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()

			for i := 0; i < 5; i++ {
				head, err := ds.HeadRevision(ctx)
				require.NoError(err)
				require.NoError(ds.CheckRevision(ctx, head), "expected head revision to be valid in GC Window")
				require.True(head.GreaterThan(startingRev) || head.Equal(startingRev))
			}
		}()
	}

	wg.Wait()
}
