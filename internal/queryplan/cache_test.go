package queryplan

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/queryopttest"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/query/queryprofile"
)

type schemaReader struct {
	datalayer.SchemaReader
	stored *datastore.ReadOnlyStoredSchema
}

func (r schemaReader) StoredSchema() *datastore.ReadOnlyStoredSchema { return r.stored }

func storedReader() schemaReader {
	return schemaReader{stored: datastore.NewReadOnlyStoredSchema(&core.StoredSchema{
		VersionOneof: &core.StoredSchema_V1{V1: &core.StoredSchema_V1StoredSchema{}},
	})}
}

func checkKey(t *testing.T, width uint16) Key {
	t.Helper()
	profile, err := queryprofile.Resolve(queryprofile.NativeParityV1, queryprofile.Options{CheckConcurrency: width, DispatchChunkSize: 100})
	require.NoError(t, err)
	return Key{
		Profile:      profile,
		Operation:    query.OperationCheck,
		ResourceType: "document",
		Permission:   "view",
		Subject:      query.ObjectType{Type: "user", Subrelation: "..."},
	}
}

func checkBuilder(t *testing.T, key Key, calls *atomic.Int32) func(context.Context) (query.CanonicalOutline, error) {
	t.Helper()
	return func(context.Context) (query.CanonicalOutline, error) {
		calls.Add(1)
		outline := queryopttest.Canonical(t, queryopttest.Union(
			queryopttest.Fixed(t, "document:d#reader@user:u"),
			queryopttest.Fixed(t, "document:d#writer@user:u"),
		))
		params := queryopt.RequestParams{Operation: query.OperationCheck}
		return queryopt.ApplyOptimizations(outline, key.Profile.StructuralOptimizers(params), params)
	}
}

func TestLoadReusesCompiledPlanWithIndependentExecution(t *testing.T) {
	reader := storedReader()
	key := checkKey(t, 2)
	var calls atomic.Int32
	build := checkBuilder(t, key, &calls)
	first, firstLedger, err := Load(t.Context(), reader, key, build)
	require.NoError(t, err)
	second, secondLedger, err := Load(t.Context(), reader, key, build)
	require.NoError(t, err)
	require.Same(t, first, second)
	require.NotSame(t, firstLedger, secondLedger)
	require.EqualValues(t, 1, calls.Load())

	// Both requests share only the completed tree, including embedded hint IDs.
	// Runtime accounting and mutable returned paths remain independently owned.
	for _, run := range []struct {
		it     query.Iterator
		ledger *planhint.Ledger
	}{{first, firstLedger}, {second, secondLedger}} {
		ctx := query.NewLocalContext(t.Context(), query.WithHintLedger(run.ledger), query.WithCheckConcurrency(2))
		path, err := ctx.Check(run.it, query.NewObject("document", "d"), query.NewObjectAndRelation("u", "user", "..."))
		require.NoError(t, err)
		require.NotNil(t, path)
		require.Equal(t, "d", path.Resource.ObjectID)
		path.Resource.ObjectID = "mutated"
	}

	require.Equal(t, firstLedger.Decisions(), secondLedger.Decisions())
}

func TestLoadSeparatesSchemaPolicyAndPlanIdentity(t *testing.T) {
	reader := storedReader()
	key := checkKey(t, 2)
	var calls atomic.Int32
	build := checkBuilder(t, key, &calls)
	original, _, err := Load(t.Context(), reader, key, build)
	require.NoError(t, err)

	variants := []Key{checkKey(t, 3), key, key, key, key}
	variants[1].Permission = "edit"
	variants[2].Subject.Subrelation = "member"
	variants[3].Subject.Type = "group"
	variants[4].SerializedPlan = "different exact wire plan"

	for _, variant := range variants {
		distinct, _, err := Load(t.Context(), reader, variant, build)
		require.NoError(t, err)
		require.NotSame(t, original, distinct)
	}

	replacement, _, err := Load(t.Context(), storedReader(), key, build)
	require.NoError(t, err)
	require.NotSame(t, original, replacement)
	require.EqualValues(t, 7, calls.Load())
}

func TestLoadDoesNotRetainFailuresOrLegacyPlans(t *testing.T) {
	reader := storedReader()
	key := checkKey(t, 2)
	failure := errors.New("build failed")
	_, _, err := Load(
		t.Context(),
		reader,
		key,
		func(context.Context) (query.CanonicalOutline, error) { return query.CanonicalOutline{}, failure },
	)
	require.ErrorIs(t, err, failure)
	var calls atomic.Int32
	build := checkBuilder(t, key, &calls)
	_, _, err = Load(t.Context(), reader, key, build)
	require.NoError(t, err)

	for range 2 {
		_, _, err = Load(t.Context(), nil, key, build)
		require.NoError(t, err)
	}

	require.EqualValues(t, 3, calls.Load())
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = Load(canceled, reader, key, build)
	require.ErrorIs(t, err, context.Canceled)
	require.EqualValues(t, 3, calls.Load())
}

func TestConcurrentFirstPlanLoadsShareCompilation(t *testing.T) {
	reader := storedReader()
	key := checkKey(t, 2)
	var calls atomic.Int32
	build := checkBuilder(t, key, &calls)
	var workers sync.WaitGroup
	plans := make([]query.Iterator, 24)
	ledgers := make([]*planhint.Ledger, len(plans))

	for i := range plans {
		workers.Go(func() {
			var err error
			plans[i], ledgers[i], err = Load(t.Context(), reader, key, build)
			require.NoError(t, err)
		})
	}

	workers.Wait()
	require.EqualValues(t, 1, calls.Load())

	for i := 1; i < len(plans); i++ {
		require.Same(t, plans[0], plans[i])
		require.NotSame(t, ledgers[0], ledgers[i])
	}
}

func TestPlanCacheEvictsLeastRecentlyUsedAndSkipsOversizedPlans(t *testing.T) {
	key := checkKey(t, 2)
	var calls atomic.Int32
	build := checkBuilder(t, key, &calls)
	sample, err := compile(t.Context(), key, build, true)
	require.NoError(t, err)
	cache := newCache(2 * sample.cost)
	first, second, third := key, key, key
	first.Permission, second.Permission, third.Permission = "aaaa", "bbbb", "cccc"

	// Touch the first plan again before admitting the third. The second becomes
	// the eviction candidate even though it was constructed more recently.
	_, err = cache.load(t.Context(), first, build)
	require.NoError(t, err)
	_, err = cache.load(t.Context(), second, build)
	require.NoError(t, err)
	_, err = cache.load(t.Context(), first, build)
	require.NoError(t, err)
	_, err = cache.load(t.Context(), third, build)
	require.NoError(t, err)
	require.NotNil(t, cache.get(first))
	require.Nil(t, cache.get(second))
	require.NotNil(t, cache.get(third))
	require.LessOrEqual(t, cache.used, cache.budget)
	before := calls.Load()
	_, err = cache.load(t.Context(), second, build)
	require.NoError(t, err)
	require.Equal(t, before+1, calls.Load())
	require.LessOrEqual(t, cache.used, cache.budget)

	oversized := newCache(sample.cost - 1)
	before = calls.Load()

	for range 2 {
		_, err = oversized.load(t.Context(), key, build)
		require.NoError(t, err)
	}

	require.Equal(t, before+2, calls.Load())
	require.Zero(t, oversized.used)
}

func TestSharedPlanBuildSurvivesCanceledFirstCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		key := checkKey(t, 2)
		cache := newCache(256 << 10)
		started, release := make(chan struct{}), make(chan struct{})
		var calls atomic.Int32
		base := checkBuilder(t, key, &calls)
		build := func(ctx context.Context) (query.CanonicalOutline, error) {
			close(started)

			select {
			case <-release:
				return base(ctx)
			case <-ctx.Done():
				return query.CanonicalOutline{}, ctx.Err()
			}
		}
		firstContext, cancel := context.WithCancel(t.Context())
		first, second := make(chan error, 1), make(chan error, 1)

		go func() {
			_, err := cache.load(firstContext, key, build)
			first <- err
		}()
		<-started

		go func() {
			_, err := cache.load(t.Context(), key, build)
			second <- err
		}()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-first, context.Canceled)
		close(release)
		require.NoError(t, <-second)
		require.EqualValues(t, 1, calls.Load())
		require.NotNil(t, cache.get(key))
	})
}

func TestPlanAdmissionCountsCompleteEmbeddedPayload(t *testing.T) {
	key := checkKey(t, 2)
	build := func(subject string) func(context.Context) (query.CanonicalOutline, error) {
		return func(context.Context) (query.CanonicalOutline, error) {
			outline := queryopttest.Fixed(t, "document:d#reader@user:u")
			outline.Args.FixedPaths[0].Subject.ObjectID = subject
			return queryopttest.Canonical(t, outline), nil
		}
	}
	small, err := compile(t.Context(), key, build("u"), true)
	require.NoError(t, err)
	large, err := compile(t.Context(), key, build(strings.Repeat("u", 4096)), true)
	require.NoError(t, err)

	// Fixed outlines identify the path count, so outline identity length alone
	// cannot account for the decoded strings retained by the compiled iterator.
	require.Greater(t, large.cost-small.cost, int64(4095*8))
	cache := newCache(small.cost)
	cache.add(key, large)
	require.Nil(t, cache.get(key))
	require.Zero(t, cache.used)
}
