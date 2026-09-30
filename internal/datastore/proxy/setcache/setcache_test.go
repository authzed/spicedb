package setcache_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/proxy/setcache"
	"github.com/authzed/spicedb/pkg/cache"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

// countingDatastore counts delegate queries and materializing reads, which have no subject filter.
type countingDatastore struct {
	datastore.Datastore
	queries    atomic.Int64
	unfiltered atomic.Int64
	// inflight counts the unfiltered reads that wait on gate.
	inflight       atomic.Int64
	failUnfiltered atomic.Bool
	failAll        atomic.Bool
	// If gate is not nil, unfiltered reads wait until it closes.
	gate chan struct{}

	mu          sync.Mutex
	resourceIDs [][]string
}

func (c *countingDatastore) reset() {
	c.queries.Store(0)
	c.unfiltered.Store(0)
	c.mu.Lock()
	c.resourceIDs = nil
	c.mu.Unlock()
}

func (c *countingDatastore) queriedIDs() [][]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.resourceIDs)
}

func (c *countingDatastore) SnapshotReader(rev datastore.Revision) datastore.Reader {
	return &countingReader{c.Datastore.SnapshotReader(rev), c}
}

type countingReader struct {
	datastore.Reader
	c *countingDatastore
}

func (r *countingReader) QueryRelationships(ctx context.Context,
	filter datastore.RelationshipsFilter, opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	r.c.queries.Add(1)
	r.c.mu.Lock()
	r.c.resourceIDs = append(r.c.resourceIDs, slices.Clone(filter.OptionalResourceIds))
	r.c.mu.Unlock()
	if r.c.failAll.Load() {
		return nil, errors.New("synthetic query failure")
	}
	if len(filter.OptionalSubjectsSelectors) == 0 {
		r.c.unfiltered.Add(1)
		if r.c.failUnfiltered.Load() {
			return nil, errors.New("synthetic materialize failure")
		}
		if r.c.gate != nil {
			r.c.inflight.Add(1)
			<-r.c.gate
			r.c.inflight.Add(-1)
		}
	}
	return r.Reader.QueryRelationships(ctx, filter, opts...)
}

func writeRels(t testing.TB, ds datastore.Datastore, rels ...string) datastore.Revision {
	t.Helper()
	rev, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		updates := make([]tuple.RelationshipUpdate, 0, len(rels))
		for _, rel := range rels {
			updates = append(updates, tuple.Create(tuple.MustParse(rel)))
		}
		return rwt.WriteRelationships(ctx, updates)
	})
	require.NoError(t, err)
	return rev
}

func writeTestRels(t *testing.T, ds datastore.Datastore) datastore.Revision {
	return writeRels(t, ds,
		"document:foo#viewer@user:evan",
		"document:foo#viewer@user:tanner",
		"document:bar#viewer@user:evan",
	)
}

func writeOneMoreRel(t *testing.T, ds datastore.Datastore) datastore.Revision {
	return writeRels(t, ds, "document:foo#viewer@user:third")
}

func newProxyForTest(t *testing.T, threshold, maxSize uint64) (*countingDatastore, datastore.Datastore, datastore.Revision) {
	return newProxyWithRels(t, threshold, maxSize, func(raw datastore.Datastore) datastore.Revision {
		return writeTestRels(t, raw)
	})
}

// newProxyWithRels returns a set cache proxy over a counting memdb delegate that write populates.
func newProxyWithRels(t testing.TB, threshold, maxSize uint64, write func(datastore.Datastore) datastore.Revision) (*countingDatastore, datastore.Datastore, datastore.Revision) {
	return newProxyWithOptions(t, setcache.Options{MaterializeThreshold: threshold, MaximumSetSize: maxSize}, write)
}

// newProxyWithOptions is newProxyWithRels with proxy options from the caller.
func newProxyWithOptions(t testing.TB, opts setcache.Options, write func(datastore.Datastore) datastore.Revision) (*countingDatastore, datastore.Datastore, datastore.Revision) {
	raw, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	rev := write(raw)
	counting := &countingDatastore{Datastore: raw}
	c, err := cache.NewStandardCache[setcache.SetKey, *setcache.CachedSet](&cache.Config{MaxCost: 1 << 24})
	require.NoError(t, err)
	t.Cleanup(c.Close)
	est, err := setcache.NewAccessCounter(1<<20, time.Minute)
	require.NoError(t, err)
	t.Cleanup(est.Close)
	return counting, setcache.NewProxy(counting, c, est, opts), rev
}

func queryViewerErr(ctx context.Context, r datastore.Reader, objID, subjectID string) ([]tuple.Relationship, error) {
	it, err := r.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: "document", OptionalResourceIds: []string{objID},
		OptionalResourceRelation: "viewer",
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{{
			OptionalSubjectType: "user", OptionalSubjectIds: []string{subjectID},
			RelationFilter: datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
		}},
	})
	if err != nil {
		return nil, err
	}
	return datastore.IteratorToSlice(it)
}

func queryViewer(t testing.TB, r datastore.Reader, objID, subjectID string) []tuple.Relationship {
	t.Helper()
	rels, err := queryViewerErr(t.Context(), r, objID, subjectID)
	require.NoError(t, err)
	return rels
}

func TestThresholdGatesMaterialization(t *testing.T) {
	counting, ds, rev := newProxyForTest(t, 2, 1000)
	r := ds.SnapshotReader(rev)

	// Access 1 goes to the delegate.
	require.Len(t, queryViewer(t, r, "foo", "evan"), 1)
	require.EqualValues(t, 1, counting.queries.Load())
	require.EqualValues(t, 0, counting.unfiltered.Load())

	// Access 2 materializes the set and serves from it.
	require.Len(t, queryViewer(t, r, "foo", "evan"), 1)
	require.EqualValues(t, 2, counting.queries.Load())
	require.EqualValues(t, 1, counting.unfiltered.Load())

	// The set serves these queries without the datastore.
	// A subject that the cache never saw gets a negative answer from memory.
	require.Len(t, queryViewer(t, r, "foo", "tanner"), 1)
	require.Empty(t, queryViewer(t, r, "foo", "jake"))
	require.EqualValues(t, 2, counting.queries.Load())
}

func TestTooBigSetIsMemoized(t *testing.T) {
	// The cap is 1 and foo#viewer has 2 relationships.
	counting, ds, rev := newProxyForTest(t, 1, 1)
	r := ds.SnapshotReader(rev)
	// The probe reads cap+1 rows and stores a memo.
	require.Len(t, queryViewer(t, r, "foo", "evan"), 1)
	require.EqualValues(t, 1, counting.unfiltered.Load())
	// The memo sends a filtered query to the delegate without a re-probe.
	require.Len(t, queryViewer(t, r, "foo", "evan"), 1)
	require.EqualValues(t, 1, counting.unfiltered.Load())
}

func TestRevisionIsolation(t *testing.T) {
	counting, ds, rev1 := newProxyForTest(t, 1, 1000)
	// This materializes the set at rev1.
	require.Empty(t, queryViewer(t, ds.SnapshotReader(rev1), "foo", "third"))
	// This writes document:foo#viewer@user:third.
	rev2 := writeOneMoreRel(t, counting.Datastore)
	got := queryViewer(t, ds.SnapshotReader(rev2), "foo", "third")
	require.Len(t, got, 1, "a set must never be served across revisions")
}

func TestMaterializeErrorFallsBackToPassThrough(t *testing.T) {
	counting, ds, rev := newProxyForTest(t, 1, 1000)
	counting.failUnfiltered.Store(true)
	require.Len(t, queryViewer(t, ds.SnapshotReader(rev), "foo", "evan"), 1,
		"a failed materializing read must never fail the original query")
	require.EqualValues(t, 1, counting.unfiltered.Load())
}

func TestMaterializationIsSingleflighted(t *testing.T) {
	counting, ds, rev := newProxyForTest(t, 1, 1000)
	counting.gate = make(chan struct{})
	var wg sync.WaitGroup
	results := make([]int, 8)
	errs := make([]error, 8)
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rels, err := queryViewerErr(t.Context(), ds.SnapshotReader(rev), "foo", "evan")
			results[i], errs[i] = len(rels), err
		}()
	}
	// Let the goroutines reach the singleflight.
	// A goroutine that arrives after the gate opens uses the stored set, so the assertion stays true.
	time.Sleep(50 * time.Millisecond)
	close(counting.gate)
	wg.Wait()
	require.EqualValues(t, 1, counting.unfiltered.Load())
	for i := range results {
		require.NoError(t, errs[i])
		require.Equal(t, 1, results[i])
	}
}

// evanOrTanner selects both test subjects.
// Thus multi-ID queries have a subject selector, and each unfiltered delegate read is a materializing read.
var evanOrTanner = []datastore.SubjectsSelector{{
	OptionalSubjectType: "user", OptionalSubjectIds: []string{"evan", "tanner"},
	RelationFilter: datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
}}

func queryViewers(ctx context.Context, r datastore.Reader, objIDs ...string) (datastore.RelationshipIterator, error) {
	return r.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: "document", OptionalResourceIds: objIDs,
		OptionalResourceRelation: "viewer", OptionalSubjectsSelectors: evanOrTanner,
	})
}

// warmFooOnly materializes foo#viewer with threshold 2, leaving bar cold.
func warmFooOnly(t *testing.T) (*countingDatastore, datastore.Reader) {
	counting, ds, rev := newProxyForTest(t, 2, 1000)
	r := ds.SnapshotReader(rev)
	queryViewer(t, r, "foo", "evan")
	queryViewer(t, r, "foo", "evan")
	require.EqualValues(t, 1, counting.unfiltered.Load())
	counting.reset()
	return counting, r
}

func TestMultiIDQuerySplitsServedAndRemainder(t *testing.T) {
	counting, r := warmFooOnly(t)

	it, err := queryViewers(t.Context(), r, "foo", "bar")
	require.NoError(t, err)
	rels, err := datastore.IteratorToSlice(it)
	require.NoError(t, err)
	// The foo set gives 2 relationships and the delegate gives 1 for bar.
	require.Len(t, rels, 3)

	// bar is below the threshold, so one filtered delegate query reads only bar.
	require.EqualValues(t, 1, counting.queries.Load())
	require.EqualValues(t, 0, counting.unfiltered.Load())
	require.Equal(t, [][]string{{"bar"}}, counting.queriedIDs())
}

func TestRemainderErrorSurfacesDuringIteration(t *testing.T) {
	counting, r := warmFooOnly(t)
	counting.failAll.Store(true)

	it, err := queryViewers(t.Context(), r, "foo", "bar")
	require.NoError(t, err, "the remainder query is issued lazily")

	var served int
	var iterErr error
	for _, err := range it {
		if err != nil {
			iterErr = err
			break
		}
		served++
	}
	require.Equal(t, 2, served, "cached foo rels are yielded before the remainder")
	require.ErrorContains(t, iterErr, "synthetic query failure")
	require.Equal(t, [][]string{{"bar"}}, counting.queriedIDs())
}

func TestEarlyStopSkipsRemainderQuery(t *testing.T) {
	counting, r := warmFooOnly(t)

	it, err := queryViewers(t.Context(), r, "foo", "bar")
	require.NoError(t, err)
	for _, err := range it {
		require.NoError(t, err)
		break
	}
	require.EqualValues(t, 0, counting.queries.Load(),
		"stopping inside the served rels must not issue the remainder query")
}

func TestHotIDsMaterializeConcurrently(t *testing.T) {
	counting, ds, _ := newProxyForTest(t, 1, 1000)
	rev := writeRels(t, counting.Datastore, "document:baz#viewer@user:tanner")
	r := ds.SnapshotReader(rev)

	// All three materializing reads wait on the gate.
	// They can all be in flight only if they run concurrently.
	gate := make(chan struct{})
	counting.gate = gate
	var closeOnce sync.Once
	openGate := func() { closeOnce.Do(func() { close(gate) }) }
	t.Cleanup(openGate)

	type result struct {
		rels []tuple.Relationship
		err  error
	}
	done := make(chan result, 1)
	go func() {
		it, err := queryViewers(t.Context(), r, "foo", "bar", "baz")
		if err != nil {
			done <- result{err: err}
			return
		}
		rels, err := datastore.IteratorToSlice(it)
		done <- result{rels, err}
	}()

	require.Eventually(t, func() bool { return counting.inflight.Load() == 3 },
		5*time.Second, time.Millisecond, "hot IDs must materialize concurrently")
	openGate()
	res := <-done
	require.NoError(t, res.err)
	require.Len(t, res.rels, 4)
	require.EqualValues(t, 3, counting.unfiltered.Load())
	require.EqualValues(t, 3, counting.queries.Load())

	// The cache holds all three sets, so a repeat query never reaches the delegate.
	counting.reset()
	it, err := queryViewers(t.Context(), r, "foo", "bar", "baz")
	require.NoError(t, err)
	rels, err := datastore.IteratorToSlice(it)
	require.NoError(t, err)
	require.Len(t, rels, 4)
	require.EqualValues(t, 0, counting.queries.Load())
}

func TestTooBigMemoSpansRevisions(t *testing.T) {
	// The cap is 1 and foo#viewer has 2 relationships.
	counting, ds, rev1 := newProxyForTest(t, 1, 1)
	require.Len(t, queryViewer(t, ds.SnapshotReader(rev1), "foo", "evan"), 1)
	// The probe at rev1 stores a memo.
	require.EqualValues(t, 1, counting.unfiltered.Load())

	rev2 := writeOneMoreRel(t, counting.Datastore)
	require.Len(t, queryViewer(t, ds.SnapshotReader(rev2), "foo", "third"), 1)
	require.EqualValues(t, 1, counting.unfiltered.Load(),
		"a too-big memo from rev1 must prevent a re-probe at rev2")
}

func TestTooBigMemoIsReprobedAfterInterval(t *testing.T) {
	var nowNanos atomic.Int64
	start := time.Now()
	nowNanos.Store(start.UnixNano())
	now := func() time.Time { return time.Unix(0, nowNanos.Load()) }
	advance := func(d time.Duration) { nowNanos.Add(int64(d)) }

	// The cap is 1 and foo#viewer has 2 relationships.
	counting, ds, rev := newProxyWithOptions(t, setcache.Options{
		MaterializeThreshold: 1, MaximumSetSize: 1, NowFunc: now,
	}, func(raw datastore.Datastore) datastore.Revision { return writeTestRels(t, raw) })
	r := ds.SnapshotReader(rev)

	// The probe stores a memo at start.
	require.Len(t, queryViewer(t, r, "foo", "evan"), 1)
	require.EqualValues(t, 1, counting.unfiltered.Load())

	// Continuous access keeps the memo hot, and the memo stays fresh.
	for range 5 {
		advance(10 * time.Second)
		require.Len(t, queryViewer(t, r, "foo", "evan"), 1)
	}
	require.EqualValues(t, 1, counting.unfiltered.Load(), "a fresh memo must prevent a re-probe")

	// The memo is 60s old after this advance.
	advance(10 * time.Second)
	require.Len(t, queryViewer(t, r, "foo", "evan"), 1)
	require.EqualValues(t, 2, counting.unfiltered.Load(),
		"a memo older than the re-probe interval must be re-probed even while hot")

	// The re-probe stores a new memo with a new creation time.
	advance(30 * time.Second)
	require.Len(t, queryViewer(t, r, "foo", "evan"), 1)
	require.EqualValues(t, 2, counting.unfiltered.Load())
}

func TestFreshRevisionsNeverMaterialize(t *testing.T) {
	const threshold = 3
	counting, ds, _ := newProxyForTest(t, threshold, 1000)

	// A hot object that the test reads once at each of many new revisions never reaches the threshold.
	for i := range 4 * threshold {
		rev := writeRels(t, counting.Datastore, fmt.Sprintf("document:other#viewer@user:u%d", i))
		require.Len(t, queryViewer(t, ds.SnapshotReader(rev), "foo", "evan"), 1)
	}
	require.EqualValues(t, 4*threshold, counting.queries.Load())
	require.EqualValues(t, 0, counting.unfiltered.Load(),
		"reads at distinct fresh revisions must never issue a materializing read")

	// Repeated reads at one revision materialize at the threshold.
	rev := writeRels(t, counting.Datastore, "document:other#viewer@user:last")
	r := ds.SnapshotReader(rev)
	for range threshold {
		queryViewer(t, r, "foo", "evan")
	}
	require.EqualValues(t, 1, counting.unfiltered.Load())
}

func TestDuplicateResourceIDsServedOnce(t *testing.T) {
	_, ds, rev := newProxyForTest(t, 1, 1000)
	r := ds.SnapshotReader(rev)
	queryViewer(t, r, "foo", "evan")

	it, err := r.QueryRelationships(t.Context(), datastore.RelationshipsFilter{
		OptionalResourceType: "document", OptionalResourceIds: []string{"foo", "foo"},
		OptionalResourceRelation: "viewer",
	})
	require.NoError(t, err)
	rels, err := datastore.IteratorToSlice(it)
	require.NoError(t, err)
	require.Len(t, rels, 2)
}

func TestNonServableShapesPassThrough(t *testing.T) {
	counting, ds, rev := newProxyForTest(t, 1, 1000)
	r := ds.SnapshotReader(rev)

	// Every query has a subject selector, so each unfiltered delegate read is a materializing read.
	evan := []datastore.SubjectsSelector{{
		OptionalSubjectType: "user", OptionalSubjectIds: []string{"evan"},
		RelationFilter: datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
	}}
	limit := uint64(1)
	cursor := options.ToCursor(tuple.MustParse("document:foo#viewer@user:evan"))
	for _, tc := range []struct {
		opts []options.QueryOptionsOption
		// memdb rejects cursors on unsorted results. That error is acceptable.
		// The test checks only that the query reached the delegate without change.
		cursorErrOK bool
	}{
		{opts: []options.QueryOptionsOption{options.WithLimit(&limit)}},
		{opts: []options.QueryOptionsOption{options.WithSort(options.ByResource)}},
		{opts: []options.QueryOptionsOption{options.WithSort(options.ChooseEfficient)}},
		{opts: []options.QueryOptionsOption{options.WithAfter(cursor)}, cursorErrOK: true},
		{opts: []options.QueryOptionsOption{options.WithBeforeOrEqual(cursor)}, cursorErrOK: true},
		{opts: []options.QueryOptionsOption{options.WithSQLCheckAssertionForTest(func(string) {})}},
		{opts: []options.QueryOptionsOption{options.WithUseTupleComparison(true)}},
	} {
		it, err := r.QueryRelationships(t.Context(), datastore.RelationshipsFilter{
			OptionalResourceType: "document", OptionalResourceIds: []string{"foo"},
			OptionalResourceRelation: "viewer", OptionalSubjectsSelectors: evan,
		}, tc.opts...)
		if tc.cursorErrOK && err != nil {
			continue
		}
		require.NoError(t, err)
		_, err = datastore.IteratorToSlice(it)
		require.NoError(t, err)
	}
	for _, filter := range []datastore.RelationshipsFilter{
		{OptionalResourceType: "document", OptionalResourceRelation: "viewer"},
		{OptionalResourceType: "document", OptionalResourceIds: []string{"foo"}},
		{OptionalResourceIds: []string{"foo"}, OptionalResourceRelation: "viewer"},
		{OptionalResourceType: "document", OptionalResourceIDPrefix: "f", OptionalResourceRelation: "viewer"},
	} {
		filter.OptionalSubjectsSelectors = evan
		it, err := r.QueryRelationships(t.Context(), filter)
		require.NoError(t, err)
		_, err = datastore.IteratorToSlice(it)
		require.NoError(t, err)
	}
	require.EqualValues(t, 11, counting.queries.Load())
	require.EqualValues(t, 0, counting.unfiltered.Load(),
		"limited/sorted/paginated/unscoped queries must never trigger materialization")
}
