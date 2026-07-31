package test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// RevisionQuantizationTest tests that revision quantization works correctly
func RevisionQuantizationTest(t *testing.T, tester DatastoreTester) {
	quantizationRanges := []time.Duration{
		0 * time.Second,
		100 * time.Millisecond,
	}

	for _, quantizationRange := range quantizationRanges {
		t.Run(fmt.Sprintf("quantization%s", quantizationRange), func(t *testing.T) {
			require := require.New(t)

			ds, err := tester.New(t, DefaultRevisionParameters().WithQuantization(quantizationRange), 1)
			require.NoError(err)

			ctx := t.Context()
			veryFirstRevisionResult, err := ds.OptimizedRevision(ctx)
			require.NoError(err)
			veryFirstRevision := veryFirstRevisionResult.Revision

			postSetupRevision := setupDatastore(t, ds)
			require.True(postSetupRevision.GreaterThan(veryFirstRevision), "post-setup revision should be greater than the first revision")

			// Create some revisions (a brand new relationship each time to force a new revision)
			var writtenAt datastore.Revision

			for i := range 10 {
				tpl := makeTestRel(fmt.Sprint(i), "owner")
				writtenAt, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, tpl)
				require.NoError(err)
			}
			require.True(writtenAt.GreaterThan(postSetupRevision))

			// Get the new now revision
			nowRevisionResult, err := ds.HeadRevision(ctx)
			require.NoError(err)
			nowRevision := nowRevisionResult.Revision

			// Let the quantization window expire
			time.Sleep(quantizationRange)

			// Now we should ONLY get revisions later than the now revision
			for start := time.Now(); time.Since(start) < 10*time.Millisecond; {
				testRevisionResult, err := ds.OptimizedRevision(ctx)
				require.NoError(err)
				testRevision := testRevisionResult.Revision
				require.True(nowRevision.LessThan(testRevision) || nowRevision.Equal(testRevision))
			}
		})
	}
}

// SnapshotReadStabilityTest asserts that the revision returned by
// OptimizedRevision behaves like a snapshot: reads at it are repeatable, and
// they never observe a write committed after it.
//
// An optimized revision is allowed to be *stale* — that is its entire purpose —
// but it is not allowed to be *unstable*. Callers resolve a revision once and
// then read at it many times: the dispatcher fans subrequests out at a single
// revision, the schema and relationship caches key on one, and a streaming API
// call serves its whole response from one. If two reads at the same revision
// disagree, every consistency guarantee layered on top of that revision is
// void, and the failure surfaces far from the datastore as a phantom read.
//
// The test deliberately arranges for the quantization boundary to precede the
// datastore's own creation: it sleeps until just after a boundary before
// calling New, leaving a full quantization window in which every write it makes
// lands after that boundary. A datastore that implements historical reads with
// real MVCC visibility returns a stable (possibly empty) view. A datastore that
// approximates them by snapping to the nearest snapshot it happens to know
// about can instead hand back a live, moving view.
//
// This test is not gated on a category: staleness is configurable, but snapshot
// stability is part of the Datastore contract, so every engine must satisfy it.
func SnapshotReadStabilityTest(t *testing.T, tester DatastoreTester) {
	// Large enough that the whole test body fits inside one window on any
	// engine, small enough that the resulting read timestamp stays well within
	// the historical-read retention limits of hosted backends (Spanner allows
	// one hour).
	const quantization = 1 * time.Second

	// Align to just after a boundary *before* the datastore exists, so the
	// boundary OptimizedRevision rounds back to is older than anything this
	// test writes.
	now := time.Now()
	time.Sleep(now.Truncate(quantization).Add(quantization).Sub(now))

	ds, err := tester.New(t, DefaultRevisionParameters().WithQuantization(quantization), 1)
	require.NoError(t, err)

	ctx := t.Context()
	setupDatastore(t, ds)

	const docA = "stability-doc-a"
	const docB = "stability-doc-b"

	// docsAt reports which of the two documents are visible at the given revision.
	docsAt := func(rev datastore.Revision) map[string]bool {
		reader := ds.SnapshotReader(rev)
		it, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
			OptionalResourceType: testResourceNamespace,
			OptionalResourceIds:  []string{docA, docB},
		}, options.WithQueryShape(queryshape.Varying))
		require.NoError(t, err)
		rels, err := datastore.IteratorToSlice(it)
		require.NoError(t, err)

		seen := map[string]bool{}
		for _, rel := range rels {
			seen[rel.Resource.ObjectID] = true
		}
		return seen
	}

	revA, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, makeTestRel(docA, "tom"))
	require.NoError(t, err)

	// Resolve the revision once, the way a request would.
	opt, err := ds.OptimizedRevision(ctx)
	require.NoError(t, err)

	// Whether docA is visible here is not asserted: the optimized revision may
	// legitimately predate it. Only stability is contractual.
	before := docsAt(opt.Revision)

	revB, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, makeTestRel(docB, "tom"))
	require.NoError(t, err)

	after := docsAt(opt.Revision)

	require.Equal(t, before, after,
		"two reads at the same revision %v returned different data (%v, then %v after an unrelated write at %v)",
		opt.Revision, before, after, revB)
	require.False(t, after[docB],
		"%s was committed at revision %v, which is after the read revision %v, but is visible there",
		docB, revB, opt.Revision)
	require.False(t, opt.Revision.GreaterThan(revA) && !before[docA],
		"revision %v is at or after %v where %s was written, but %s is not visible there",
		opt.Revision, revA, docA, docA)
}

// RevisionSerializationTest tests whether the revisions generated by this datastore can
// be serialized and sent through the dispatch layer.
func RevisionSerializationTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, DefaultRevisionParameters(), 1)
	require.NoError(err)

	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()
	revToTest, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(ctx, testNamespace)
	})
	require.NoError(err)

	meta := &dispatch.ResolverMeta{
		AtRevision:     revToTest.String(),
		DepthRemaining: 50,
		TraversalBloom: dispatch.MustNewTraversalBloomFilter(50),
		SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
	}
	require.NoError(protovalidate.Validate(meta))
}

// GCProcessRunTest tests whether the custom GC process runs for the datastore.
// For datastores that do not have custom GC processes, will no-op.
// TODO: rewrite using synctest
func GCProcessRunTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	ds, err := tester.New(t, DefaultRevisionParameters().
		WithGCRunInterval(GCRunInterval(500*time.Millisecond)).
		WithGCRetentionWindow(GCRetentionWindow(300*time.Millisecond)), 1)
	require.NoError(err)

	// NOTE: this test runs for all datastores, but only some datastores have GC logic.
	gcable, ok := ds.(datastore.GarbageCollectableDatastore)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	testCaveat := createCoreCaveat(t)
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		if err := rwt.LegacyWriteNamespaces(ctx, ns.Namespace("foo/createdtxgc")); err != nil {
			return err
		}
		return rwt.LegacyWriteCaveats(ctx, []*core.CaveatDefinition{
			testCaveat,
		})
	})
	require.NoError(err)

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(ctx, testNamespace)
	})
	require.NoError(err)

	// Reset that GC was run.
	gcable.ResetGCCompleted()

	// Wait the GC interval + a bit more time.
	time.Sleep(500*time.Millisecond + 100*time.Millisecond)

	// Ensure GC was run.
	require.True(gcable.HasGCRun(), "GC was never run as expected")
}

// RevisionGCTest makes sure revision GC takes place, revisions out-side of the GC window
// are invalid, and revisions inside the GC window are valid.
// TODO: rewrite using synctest if possible
func RevisionGCTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	gcWindow := 300 * time.Millisecond

	// NOTE: we leave the background GC process disabled here and instead manually run it below.
	ds, err := tester.New(t, DefaultRevisionParameters().
		WithGCRetentionWindow(GCRetentionWindow(gcWindow)), 1)
	require.NoError(err)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	testCaveat := createCoreCaveat(t)
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		if err := rwt.LegacyWriteNamespaces(ctx, ns.Namespace("foo/createdtxgc")); err != nil {
			return err
		}
		return rwt.LegacyWriteCaveats(ctx, []*core.CaveatDefinition{
			testCaveat,
		})
	})
	require.NoError(err)

	previousRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(ctx, testNamespace)
	})
	require.NoError(err)

	require.NoError(ds.CheckRevision(ctx, previousRev), "expected latest write revision to be within GC window")

	headResult, err := ds.HeadRevision(ctx)
	require.NoError(err)
	head := headResult.Revision
	require.NoError(ds.CheckRevision(ctx, head), "expected head revision to be valid in GC Window")

	// Sleep to ensure we're past the GC window.
	time.Sleep(gcWindow)

	gcable, ok := ds.(datastore.GarbageCollectableDatastore)
	// NOTE: CRDB and Spanner both do garbage collection with row-level TTLs
	if ok {
		// Run garbage collection.
		gcable.ResetGCCompleted()
		err := datastore.RunGarbageCollection(ctx, gcable, gcWindow)
		require.NoError(err)
		require.True(gcable.HasGCRun(), "GC was never run as expected")
	}

	// FIXME currently the various datastores behave differently when a revision was requested and GC Window elapses.
	// this is due to the fact MySQL and PostgreSQL implement revisions as a snapshot, while CRDB, Spanner and MemDB
	// implement it as a timestamp.
	//
	// previous head revision is not valid if outside GC Window
	// require.Error(ds.CheckRevision(ctx, head), "expected head revision to be valid if out of GC window")
	//
	// latest state of the system is invalid if head revision is out of GC window
	// _, _, err = ds.SnapshotReader(head).LegacyReadNamespaceByName(ctx, "foo/bar")
	// require.Error(err, "expected previously written schema to exist at out-of-GC window head")

	// check freshly fetched head revision is valid after GC window elapsed
	headResult, err = ds.HeadRevision(ctx)
	require.NoError(err)
	head = headResult.Revision

	// assert that recent call to head revision is also valid, even after a GC window cycle without writes elapsed
	require.NoError(ds.CheckRevision(ctx, head), "expected freshly obtained head revision to be valid")

	// TODO: these reads are taking a significant amount of time on CRDB, on the order
	// of 100ms for a row read. We need to ascertain whether this is a test artifact
	// or a performance regression.
	// check that we can read a caveat whose revision has been garbage collectged
	_, _, err = ds.SnapshotReader(head).LegacyReadCaveatByName(ctx, testCaveat.Name)
	require.NoError(err, "expected previously written caveat should exist at head")

	// check that we can read the namespace which had its revision garbage collected
	_, _, err = ds.SnapshotReader(head).LegacyReadNamespaceByName(ctx, "foo/createdtxgc")
	require.NoError(err, "expected previously written namespace should exist at head")

	// state of the system is also consistent at a recent call to head
	_, _, err = ds.SnapshotReader(head).LegacyReadNamespaceByName(ctx, "foo/bar")
	require.NoError(err, "expected previously written schema to exist at head")

	// write happens, we get a new head revision
	newerRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(ctx, testNamespace)
	})
	require.NoError(err)
	require.NoError(ds.CheckRevision(ctx, newerRev), "expected newer head revision to be within GC Window")
	require.Error(ds.CheckRevision(ctx, previousRev), "expected revision head-1 to be outside GC Window")
}

// QuantizedRevisionStaysReadableTest asserts that
// OptimizedRevision never hands out a revision that CheckRevision would reject,
// i.e. quantization must stay smaller than the GC retention window.
// TODO: rewrite using synctest
func QuantizedRevisionStaysReadableTest(t *testing.T, tester DatastoreTester) {
	const (
		quantization = 1 * time.Second
		gcWindow     = 2 * time.Second

		// How far the sleeps below stay clear of a bucket boundary, instead of
		// landing right on it. This test picks its timings from its own clock,
		// but Postgres, MySQL and CRDB derive buckets from the database's clock:
		// a write this test believes is just inside a new bucket can land just
		// before it in the datastore, which would put it in the previous bucket
		// and test nothing. This margin absorbs any skew smaller than itself.
		clockSkewBuffer = 100 * time.Millisecond
	)

	require := require.New(t)

	ds, err := tester.New(t, DefaultRevisionParameters().
		WithQuantization(quantization).
		WithGCRetentionWindow(GCRetentionWindow(gcWindow)), 1)
	require.NoError(err)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	setupDatastore(t, ds)

	// Land just inside a fresh bucket and write there, so that what gets
	// advertised for the rest of the bucket dates from the top of it. Postgres
	// and MySQL advertise the first transaction in the bucket, which is this
	// write; memdb, CRDB and Spanner advertise the bucket start itself.
	time.Sleep(time.Until(nextQuantizationBoundary(time.Now(), quantization)) + clockSkewBuffer)

	rel := makeTestRel("photo", "owner")
	writtenAt, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, rel)
	require.NoError(err)
	require.NoError(ds.CheckRevision(ctx, writtenAt))

	// Sleep to the far end of the bucket, where the advertised revision is at
	// its oldest and closest to aging out.
	time.Sleep(time.Until(nextQuantizationBoundary(time.Now(), quantization)) - clockSkewBuffer)

	// Sample a few times: every request in this bucket gets the same revision,
	// so an aged-out one fails for all of them, not just one.
	for range 5 {
		optimized, err := ds.OptimizedRevision(ctx)
		require.NoError(err)
		require.NoError(ds.CheckRevision(ctx, optimized.Revision),
			"revision advertised at the end of the quantization window must still be within the GC window")
	}

	// Now age the write out of the retention window. The second write gives the
	// datastores that read the oldest valid revision off the transaction log a
	// newer transaction to compare against.
	time.Sleep(gcWindow)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, rel)
	require.NoError(err)

	// This stale error is what every request would get, for the tail of every
	// bucket, if quantization outgrew the retention window.
	revisionErr := datastore.InvalidRevisionError{}
	require.ErrorAs(ds.CheckRevision(ctx, writtenAt), &revisionErr)
	require.Equal(datastore.RevisionStale, revisionErr.Reason())
}

// nextQuantizationBoundary returns the start of the bucket after the one
// containing now.
func nextQuantizationBoundary(now time.Time, quantization time.Duration) time.Time {
	return now.Truncate(quantization).Add(quantization)
}

func CheckRevisionsTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	gcRunInterval := 10 * time.Second
	ds, err := tester.New(t, DefaultRevisionParameters().
		WithGCRunInterval(GCRunInterval(gcRunInterval)), 1)
	require.NoError(err)

	ctx, cancel := context.WithTimeout(t.Context(), gcRunInterval)
	defer cancel()

	// Write a new revision.
	writtenRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(ctx, ns.Namespace("foo/somethingnew1"))
	})
	require.NoError(err)
	require.NoError(ds.CheckRevision(ctx, writtenRev), "expected written revision to be valid in GC Window")

	headResult, err := ds.HeadRevision(ctx)
	require.NoError(err)
	head := headResult.Revision

	// Check the head revision is valid
	require.NoError(ds.CheckRevision(ctx, head), "expected head revision to be valid in GC Window")

	// Write a new revision.
	writtenRev, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(ctx, ns.Namespace("foo/somethingnew2"))
	})
	require.NoError(err)
	require.NoError(ds.CheckRevision(ctx, writtenRev), "expected written revision to be valid in GC Window")

	// Check the previous head revision is still valid
	require.NoError(ds.CheckRevision(ctx, head), "expected previous revision to be valid in GC Window")

	// Get the updated head revision.
	headResult, err = ds.HeadRevision(ctx)
	require.NoError(err)
	head = headResult.Revision

	// Check the new head revision is valid.
	require.NoError(ds.CheckRevision(ctx, head), "expected head revision to be valid in GC Window")
}

// SequentialRevisionsTest asserts that calls to HeadRevision move the revision forward
func SequentialRevisionsTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	gcRunInterval := 10 * time.Second
	ds, err := tester.New(t, DefaultRevisionParameters().
		WithGCRunInterval(GCRunInterval(gcRunInterval)), 1)
	require.NoError(err)

	ctx, cancel := context.WithTimeout(t.Context(), gcRunInterval)
	defer cancel()

	var previous datastore.Revision
	for range 50 {
		headResult, err := ds.HeadRevision(ctx)
		require.NoError(err)
		head := headResult.Revision
		require.NoError(ds.CheckRevision(ctx, head), "expected head revision to be valid in GC Window")

		if previous != nil {
			require.True(head.GreaterThan(previous) || head.Equal(previous))
		}

		previous = head
	}
}

// ConcurrentRevisionsTest asserts that concurrent calls to HeadRevision move the revision forward
func ConcurrentRevisionsTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	gcRunInterval := 10 * time.Second
	ds, err := tester.New(t, DefaultRevisionParameters().
		WithGCRunInterval(GCRunInterval(gcRunInterval)), 1)
	require.NoError(err)

	ctx, cancel := context.WithTimeout(t.Context(), gcRunInterval)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(10)

	startingRevResult, err := ds.HeadRevision(ctx)
	require.NoError(err)
	startingRev := startingRevResult.Revision

	errCh := make(chan error, 10*5)

	for range 10 {
		go func() {
			defer wg.Done()

			for range 5 {
				headResult, err := ds.HeadRevision(ctx)
				if err != nil {
					errCh <- fmt.Errorf("HeadRevision error: %w", err)
					continue
				}
				head := headResult.Revision
				if err := ds.CheckRevision(ctx, head); err != nil {
					errCh <- fmt.Errorf("CheckRevision error: %w", err)
					continue
				}
				if !head.GreaterThan(startingRev) && !head.Equal(startingRev) {
					errCh <- errors.New("head revision is not greater than or equal to startingRev")
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(err)
	}
}
