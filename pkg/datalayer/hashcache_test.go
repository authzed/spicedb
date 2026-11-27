package datalayer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func makeTestSchema(text, hash string) *datastore.ReadOnlyStoredSchema {
	return datastore.NewReadOnlyStoredSchema(&core.StoredSchema{
		Version: 1,
		VersionOneof: &core.StoredSchema_V1{
			V1: &core.StoredSchema_V1StoredSchema{
				SchemaText: text,
				SchemaHash: hash,
			},
		},
	})
}

func TestSchemaHashCache_BasicGetSet(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache())

	// Cache miss
	retrieved, err := shc.get(SchemaHash("hash1"))
	require.NoError(t, err)
	require.Nil(t, retrieved)

	// Set and get
	schema := makeTestSchema("definition user {}", "hash1")
	err = shc.Set(SchemaHash("hash1"), schema)
	require.NoError(t, err)

	retrieved, err = shc.get(SchemaHash("hash1"))
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, schema.Get().GetV1().SchemaText, retrieved.Get().GetV1().SchemaText)
}

func TestSchemaHashCache_SetCostsBySize(t *testing.T) {
	tc := newTestSchemaCache()
	shc := newSchemaHashCache(tc)

	// A larger schema must be costed higher than a smaller one, and the cost
	// must match the schema's estimated (serialized) size rather than a flat 1.
	small := makeTestSchema("definition user {}", "small")
	large := makeTestSchema("definition user {}"+string(make([]byte, 4096)), "large")

	require.NoError(t, shc.Set(SchemaHash("small"), small))
	require.NoError(t, shc.Set(SchemaHash("large"), large))

	require.Equal(t, small.EstimatedSize(), tc.cost(SchemaCacheKey("small")))
	require.Equal(t, large.EstimatedSize(), tc.cost(SchemaCacheKey("large")))
	require.Greater(t, tc.cost(SchemaCacheKey("large")), tc.cost(SchemaCacheKey("small")))
	require.Greater(t, tc.cost(SchemaCacheKey("small")), int64(1))
}

func TestSchemaHashCache_EmptyHash(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache())

	require.Panics(t, func() {
		_ = shc.Set(SchemaHash(""), makeTestSchema("definition user {}", ""))
	}, "empty hash should panic")

	require.Panics(t, func() {
		_, _ = shc.get(SchemaHash(""))
	}, "empty hash should panic")
}

func TestSchemaHashCache_GetOrLoad(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache())

	loadCalls := 0
	loader := func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
		loadCalls++
		return makeTestSchema("loaded definition", "hash1"), nil
	}

	// First call should load
	schema, err := shc.GetOrLoad(t.Context(), datastore.NoRevision, SchemaHash("hash1"), loader)
	require.NoError(t, err)
	require.NotNil(t, schema)
	require.Equal(t, "loaded definition", schema.Get().GetV1().SchemaText)
	require.Equal(t, 1, loadCalls)

	// Second call should hit cache
	schema, err = shc.GetOrLoad(t.Context(), datastore.NoRevision, SchemaHash("hash1"), loader)
	require.NoError(t, err)
	require.NotNil(t, schema)
	require.Equal(t, 1, loadCalls)
}

func TestSchemaHashCache_GetOrLoadEmptyHash(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache())

	require.Panics(t, func() {
		_, _ = shc.GetOrLoad(t.Context(), datastore.NoRevision, SchemaHash(""), func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
			return makeTestSchema("loaded definition", ""), nil
		})
	}, "empty hash should panic")
}

func TestSchemaHashCache_Singleflight(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache())

	var loadCalls atomic.Int32
	loadStarted := make(chan struct{})
	loadContinue := make(chan struct{})

	loader := func(_ context.Context) (*datastore.ReadOnlyStoredSchema, error) {
		if loadCalls.Add(1) == 1 {
			close(loadStarted)
		}
		<-loadContinue
		return makeTestSchema("loaded definition", "hash1"), nil
	}

	const numGoroutines = 10
	var g errgroup.Group
	for range numGoroutines {
		g.Go(func() error {
			schema, err := shc.GetOrLoad(t.Context(), datastore.NoRevision, SchemaHash("hash1"), loader)
			if err != nil {
				return err
			}
			if schema == nil {
				return fmt.Errorf("schema is nil")
			}
			return nil
		})
	}

	<-loadStarted
	close(loadContinue)
	require.NoError(t, g.Wait())
	require.Equal(t, int32(1), loadCalls.Load())
}

func TestSchemaHashCache_LoadError(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache())

	expectedErr := fmt.Errorf("load failed")
	schema, err := shc.GetOrLoad(t.Context(), datastore.NoRevision, SchemaHash("hash1"), func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
		return nil, expectedErr
	})
	require.Error(t, err)
	require.Equal(t, expectedErr, err)
	require.Nil(t, schema)
}

func TestSchemaHashCache_SetUpdatesLatest(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache())

	err := shc.Set(SchemaHash("hash1"), makeTestSchema("definition v1 {}", "hash1"))
	require.NoError(t, err)

	latest := shc.latest.Load()
	require.NotNil(t, latest)
	require.Equal(t, SchemaHash("hash1"), latest.hash)

	err = shc.Set(SchemaHash("hash2"), makeTestSchema("definition v2 {}", "hash2"))
	require.NoError(t, err)

	latest = shc.latest.Load()
	require.Equal(t, SchemaHash("hash2"), latest.hash)
}

func TestSchemaHashCache_SentinelBypass(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache())

	allSentinels := []SchemaHash{
		NoSchemaHashInTransaction,
		NoSchemaHashForTesting,
		NoSchemaHashForWatch,
		NoSchemaHashForLegacyCursor,
		NoSchemaHashInDevelopment,
	}

	schema := makeTestSchema("definition user {}", "sentinel-hash")

	for _, sentinel := range allSentinels {
		// Set should no-op
		err := shc.Set(sentinel, schema)
		require.NoError(t, err)

		// GetOrLoad should always call loader
		loadCalled := false
		result, err := shc.GetOrLoad(t.Context(), datastore.NoRevision, sentinel, func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
			loadCalled = true
			return schema, nil
		})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.True(t, loadCalled)
	}
}

func TestSchemaHashCache_SingleflightTimeoutFallback(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache())

	originalTimeout := singleflightTimeout
	defer func() { singleflightTimeout = originalTimeout }()
	singleflightTimeout = 100 * time.Millisecond

	leaderStarted := make(chan struct{})
	leaderContinue := make(chan struct{})

	var loadCount atomic.Int32

	slowLoader := func(_ context.Context) (*datastore.ReadOnlyStoredSchema, error) {
		count := loadCount.Add(1)
		if count == 1 {
			close(leaderStarted)
			<-leaderContinue
		}
		return makeTestSchema("loaded schema", "timeout-hash"), nil
	}

	hash := SchemaHash("timeout-hash")

	var leaderWg sync.WaitGroup
	var leaderErr error
	leaderWg.Add(1)
	go func() {
		defer leaderWg.Done()
		_, leaderErr = shc.GetOrLoad(t.Context(), datastore.NoRevision, hash, slowLoader)
	}()

	<-leaderStarted

	// Waiter should fall back after timeout
	schema, err := shc.GetOrLoad(t.Context(), datastore.NoRevision, hash, slowLoader)
	require.NoError(t, err)
	require.NotNil(t, schema)

	require.GreaterOrEqual(t, int(loadCount.Load()), 2)

	close(leaderContinue)
	leaderWg.Wait()
	require.NoError(t, leaderErr)
}

func TestNoopSchemaCache(t *testing.T) {
	noop := noopSchemaCache{}

	// Set is a no-op
	err := noop.Set(SchemaHash("hash"), makeTestSchema("test", "hash"))
	require.NoError(t, err)

	// GetOrLoad always calls loader
	loadCalled := false
	schema, err := noop.GetOrLoad(t.Context(), datastore.NoRevision, SchemaHash("hash"), func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
		loadCalled = true
		return makeTestSchema("loaded", "hash"), nil
	})
	require.NoError(t, err)
	require.True(t, loadCalled)
	require.Equal(t, "loaded", schema.Get().GetV1().SchemaText)
}

func TestSchemaHashCache_SlowPathCacheHit(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache())

	schema1 := makeTestSchema("definition v1 {}", "hash1")
	schema2 := makeTestSchema("definition v2 {}", "hash2")

	// Set hash1, then hash2 — latest now points to hash2
	err := shc.Set(SchemaHash("hash1"), schema1)
	require.NoError(t, err)
	err = shc.Set(SchemaHash("hash2"), schema2)
	require.NoError(t, err)

	// Get hash1 — latest is hash2, so fast path misses,
	// but backing cache should have it (slow path hit, line 95)
	retrieved, err := shc.get(SchemaHash("hash1"))
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "definition v1 {}", retrieved.Get().GetV1().SchemaText)
}

func TestSchemaHashCache_GetOrLoadMismatchedHashNotCached(t *testing.T) {
	tc := newTestSchemaCache()
	shc := newSchemaHashCache(tc)

	oldSchema := makeTestSchema("definition old {}", "hash-old")
	newSchema := makeTestSchema("definition new {}", "hash-new")

	// Simulate a snapshot or replica mismatch: we request "hash-new", but the loader
	// returns a schema whose embedded hash is "hash-old".
	got, err := shc.GetOrLoad(t.Context(), datastore.NoRevision, SchemaHash("hash-new"), func(context.Context) (*datastore.ReadOnlyStoredSchema, error) {
		return oldSchema, nil
	})
	require.NoError(t, err)
	require.Equal(t, oldSchema, got)

	// Neither the backing cache nor the TTL-less latest pointer may be poisoned.
	_, inCache := tc.Get(SchemaCacheKey("hash-new"))
	require.False(t, inCache, "mismatched schema must not be stored in backing cache")
	require.Nil(t, shc.latest.Load(), "mismatched schema must not be stored in latest pointer")

	// A subsequent load returning the matching schema must invoke the loader and cache it.
	loadCalls := 0
	matchingLoader := func(context.Context) (*datastore.ReadOnlyStoredSchema, error) {
		loadCalls++
		return newSchema, nil
	}

	got, err = shc.GetOrLoad(t.Context(), datastore.NoRevision, SchemaHash("hash-new"), matchingLoader)
	require.NoError(t, err)
	require.Equal(t, newSchema, got)
	require.Equal(t, 1, loadCalls)

	_, inCache = tc.Get(SchemaCacheKey("hash-new"))
	require.True(t, inCache, "matching schema should be stored in backing cache")
	latest := shc.latest.Load()
	require.NotNil(t, latest)
	require.Equal(t, SchemaHash("hash-new"), latest.hash)
	require.Equal(t, newSchema, latest.schema)

	// Next read hits cache without calling loader again.
	got, err = shc.GetOrLoad(t.Context(), datastore.NoRevision, SchemaHash("hash-new"), matchingLoader)
	require.NoError(t, err)
	require.Equal(t, newSchema, got)
	require.Equal(t, 1, loadCalls)
}

func TestSchemaHashCache_GetOrLoadMismatchPreservesExistingLatest(t *testing.T) {
	tc := newTestSchemaCache()
	shc := newSchemaHashCache(tc)

	oldSchema := makeTestSchema("definition old {}", "hash-old")
	got, err := shc.GetOrLoad(t.Context(), datastore.NoRevision, SchemaHash("hash-old"), func(context.Context) (*datastore.ReadOnlyStoredSchema, error) {
		return oldSchema, nil
	})
	require.NoError(t, err)
	require.Equal(t, oldSchema, got)

	// Now request "hash-new", but loader still returns oldSchema ("hash-old").
	got, err = shc.GetOrLoad(t.Context(), datastore.NoRevision, SchemaHash("hash-new"), func(context.Context) (*datastore.ReadOnlyStoredSchema, error) {
		return oldSchema, nil
	})
	require.NoError(t, err)
	require.Equal(t, oldSchema, got)

	// Existing valid latest entry must remain untouched, and "hash-new" must not be cached.
	latest := shc.latest.Load()
	require.NotNil(t, latest)
	require.Equal(t, SchemaHash("hash-old"), latest.hash)
	require.Equal(t, oldSchema, latest.schema)

	_, inCache := tc.Get(SchemaCacheKey("hash-new"))
	require.False(t, inCache)
}

func TestSchemaHashCache_GetOrLoadEmptyEmbeddedHashNotCached(t *testing.T) {
	cases := []struct {
		name   string
		schema *datastore.ReadOnlyStoredSchema
	}{
		{
			name:   "empty v1 schema hash",
			schema: makeTestSchema("definition user {}", ""),
		},
		{
			name:   "nil v1 oneof",
			schema: datastore.NewReadOnlyStoredSchema(&core.StoredSchema{Version: 1}),
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			tc := newTestSchemaCache()
			shc := newSchemaHashCache(tc)

			got, err := shc.GetOrLoad(t.Context(), datastore.NoRevision, SchemaHash("hash-new"), func(context.Context) (*datastore.ReadOnlyStoredSchema, error) {
				return testCase.schema, nil
			})
			require.NoError(t, err)
			require.Equal(t, testCase.schema, got)

			_, inCache := tc.Get(SchemaCacheKey("hash-new"))
			require.False(t, inCache, "schema without matching embedded hash must not be cached")
			require.Nil(t, shc.latest.Load(), "schema without matching embedded hash must not update latest")
		})
	}
}

func TestSchemaHashCache_GetOrLoadNilSchemaReturnsError(t *testing.T) {
	for _, hash := range []SchemaHash{SchemaHash("hash-new"), NoSchemaHashForTesting} {
		t.Run(string(hash), func(t *testing.T) {
			tc := newTestSchemaCache()
			shc := newSchemaHashCache(tc)

			got, err := shc.GetOrLoad(t.Context(), datastore.NoRevision, hash, func(context.Context) (*datastore.ReadOnlyStoredSchema, error) {
				return nil, nil
			})
			require.ErrorIs(t, err, errNilLoadedSchema)
			require.Nil(t, got)

			_, inCache := tc.Get(SchemaCacheKey(hash))
			require.False(t, inCache, "nil schema must not be cached")
			require.Nil(t, shc.latest.Load(), "nil schema must not update latest")
		})
	}

	t.Run("noopSchemaCache", func(t *testing.T) {
		noop := noopSchemaCache{}
		got, err := noop.GetOrLoad(t.Context(), datastore.NoRevision, SchemaHash("hash-new"), func(context.Context) (*datastore.ReadOnlyStoredSchema, error) {
			return nil, nil
		})
		require.ErrorIs(t, err, errNilLoadedSchema)
		require.Nil(t, got)
	})
}

func TestSchemaHashCache_SingleflightMismatchDoesNotLeakToWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tc := newTestSchemaCache()
		shc := newSchemaHashCache(tc)

		oldSchema := makeTestSchema("definition old {}", "hash-old")
		newSchema := makeTestSchema("definition new {}", "hash-new")

		leaderStarted := make(chan struct{})
		leaderContinue := make(chan struct{})

		var leaderCalls atomic.Int32
		var waiterCalls atomic.Int32

		leaderLoader := func(_ context.Context) (*datastore.ReadOnlyStoredSchema, error) {
			leaderCalls.Add(1)
			close(leaderStarted)
			<-leaderContinue
			return oldSchema, nil
		}

		waiterLoader := func(_ context.Context) (*datastore.ReadOnlyStoredSchema, error) {
			waiterCalls.Add(1)
			return newSchema, nil
		}

		type result struct {
			schema *datastore.ReadOnlyStoredSchema
			err    error
		}
		leaderResCh := make(chan result, 1)
		waiterResCh := make(chan result, 1)

		go func() {
			s, err := shc.GetOrLoad(t.Context(), datastore.NoRevision, SchemaHash("hash-new"), leaderLoader)
			leaderResCh <- result{schema: s, err: err}
		}()

		<-leaderStarted

		go func() {
			s, err := shc.GetOrLoad(t.Context(), datastore.NoRevision, SchemaHash("hash-new"), waiterLoader)
			waiterResCh <- result{schema: s, err: err}
		}()

		// Wait until both the leader (blocked on leaderContinue) and the waiter
		// (blocked in singleflight waiting on the leader) are durably blocked.
		synctest.Wait()
		require.Zero(t, waiterCalls.Load(), "waiter must be joined to leader's singleflight before release")

		close(leaderContinue)

		leaderRes := <-leaderResCh
		waiterRes := <-waiterResCh

		require.NoError(t, leaderRes.err)
		require.Equal(t, oldSchema, leaderRes.schema)
		require.Equal(t, int32(1), leaderCalls.Load(), "leader should not re-run its loader")

		require.NoError(t, waiterRes.err)
		require.Equal(t, newSchema, waiterRes.schema, "concurrent waiter must reload its own revision rather than receive leader's mismatched schema")
		require.Equal(t, int32(1), waiterCalls.Load(), "waiter should reload via its own revision-bound loader")

		// The waiter's matching load should now populate both cache layers with newSchema.
		cached, inCache := tc.Get(SchemaCacheKey("hash-new"))
		require.True(t, inCache)
		require.Equal(t, newSchema, cached)
		latest := shc.latest.Load()
		require.NotNil(t, latest)
		require.Equal(t, SchemaHash("hash-new"), latest.hash)
		require.Equal(t, newSchema, latest.schema)
	})
}

func TestSchemaHashCache_SetMismatchedHashIgnored(t *testing.T) {
	tc := newTestSchemaCache()
	shc := newSchemaHashCache(tc)

	require.NoError(t, shc.Set(SchemaHash("hash-new"), makeTestSchema("definition old {}", "hash-old")))
	require.Nil(t, shc.latest.Load())
	_, inCache := tc.Get(SchemaCacheKey("hash-new"))
	require.False(t, inCache)

	require.NoError(t, shc.Set(SchemaHash("hash-new"), nil))
	require.Nil(t, shc.latest.Load())
	_, inCache = tc.Get(SchemaCacheKey("hash-new"))
	require.False(t, inCache)
}
