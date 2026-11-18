package datastore_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type testCache struct{ id int }

func newStoredSchema() *datastore.ReadOnlyStoredSchema {
	return datastore.NewReadOnlyStoredSchema(&core.StoredSchema{})
}

func TestLoadOrStoreDerivedLazyAndShared(t *testing.T) {
	key := datastore.NewDerivedCacheKey[*testCache]()
	built := 0
	build := func() *testCache {
		built++
		return &testCache{id: built}
	}

	s := newStoredSchema()

	// Built lazily on first access, and the same instance is returned thereafter.
	c1, err := datastore.LoadOrStoreDerived(s, key, build)
	require.NoError(t, err)
	c2, err := datastore.LoadOrStoreDerived(s, key, build)
	require.NoError(t, err)
	require.Same(t, c1, c2)
	require.Equal(t, 1, built, "build should be invoked exactly once per schema instance")

	// A different stored-schema instance gets its own cache (per-schema-version isolation).
	other := newStoredSchema()
	c3, err := datastore.LoadOrStoreDerived(other, key, build)
	require.NoError(t, err)
	require.NotSame(t, c1, c3)
	require.Equal(t, 2, built)
}

func TestLoadOrStoreDerivedDistinctKeysDoNotCollide(t *testing.T) {
	// Keys are identified by handle, not by type, so two caches of the same type coexist.
	keyA := datastore.NewDerivedCacheKey[*testCache]()
	keyB := datastore.NewDerivedCacheKey[*testCache]()
	s := newStoredSchema()

	a, err := datastore.LoadOrStoreDerived(s, keyA, func() *testCache { return &testCache{id: 1} })
	require.NoError(t, err)
	b, err := datastore.LoadOrStoreDerived(s, keyB, func() *testCache { return &testCache{id: 2} })
	require.NoError(t, err)
	require.NotSame(t, a, b)
	require.Equal(t, 1, a.id)
	require.Equal(t, 2, b.id)
}

func TestEstimatedSizeIncludesSchemaAndCaveatOverhead(t *testing.T) {
	// An empty stored schema needs no converted definitions or compiled caveats.
	s := datastore.NewReadOnlyStoredSchemaWithSize(&core.StoredSchema{}, 1000)
	require.Equal(t, int64(1000)+s.CompiledQueryPlanCacheBudget(), s.EstimatedSize())

	// Each caveat reserves both its converted view and compiled-CEL overhead.
	withCaveats := datastore.NewReadOnlyStoredSchemaWithSize(&core.StoredSchema{
		VersionOneof: &core.StoredSchema_V1{
			V1: &core.StoredSchema_V1StoredSchema{
				CaveatDefinitions: map[string]*core.CaveatDefinition{
					"a": {Name: "a", SerializedExpression: []byte("abc")},
					"b": {Name: "b", SerializedExpression: []byte("de")},
				},
			},
		},
	}, 1000)

	const compiledCaveatOverheadBytes = 8 * 1024
	want := int64(1000) + withCaveats.CompiledQueryPlanCacheBudget() + int64(
		len("abc")+len("de"),
	) + 2*compiledCaveatOverheadBytes + 4*(8+7) + 2*512
	require.Equal(t, want, withCaveats.EstimatedSize())
}

func TestEstimatedSizeReservesConvertedSchemaView(t *testing.T) {
	stored := datastore.NewReadOnlyStoredSchemaWithSize(&core.StoredSchema{
		VersionOneof: &core.StoredSchema_V1{V1: &core.StoredSchema_V1StoredSchema{
			NamespaceDefinitions: map[string]*core.NamespaceDefinition{"user": {Name: "user"}},
		}},
	}, 1000)

	// Budget the lazy view before insertion: growing the cost after a cache entry
	// is already admitted would let concurrent planning exceed the schema budget.
	require.Greater(t, stored.EstimatedSize(), int64(1000))
}

func TestCompiledPlanBudgetIsReservedBeforeConstruction(t *testing.T) {
	var missing *datastore.ReadOnlyStoredSchema
	require.Zero(t, missing.CompiledQueryPlanCacheBudget())

	for _, size := range []int{1, 32 << 10, 1 << 20} {
		stored := datastore.NewReadOnlyStoredSchemaWithSize(&core.StoredSchema{}, size)
		budget := stored.CompiledQueryPlanCacheBudget()
		require.GreaterOrEqual(t, budget, int64(512<<10))
		require.LessOrEqual(t, budget, int64(4<<20))
		require.GreaterOrEqual(t, stored.EstimatedSize(), int64(size)+budget)
	}
}
