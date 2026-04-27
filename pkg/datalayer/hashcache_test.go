package datalayer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func makeTestSchema(text string) *datastore.ReadOnlyStoredSchema {
	return datastore.NewReadOnlyStoredSchema(&core.StoredSchema{
		Version: 1,
		VersionOneof: &core.StoredSchema_V1{
			V1: &core.StoredSchema_V1StoredSchema{
				SchemaText: text,
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
	schema := makeTestSchema("definition user {}")
	err = shc.Set(SchemaHash("hash1"), schema)
	require.NoError(t, err)

	shc.cache.Wait()

	retrieved, err = shc.get(SchemaHash("hash1"))
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, schema.Get().GetV1().SchemaText, retrieved.Get().GetV1().SchemaText)
}

func TestSchemaHashCache_EmptyHash(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache())

	require.Panics(t, func() {
		_ = shc.Set(SchemaHash(""), makeTestSchema("definition user {}"))
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
		return makeTestSchema("loaded definition"), nil
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
			return makeTestSchema("loaded definition"), nil
		})
	}, "empty hash should panic")
}

func TestSchemaHashCache_Singleflight(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache())

	loadCalls := 0
	loadStarted := make(chan struct{})
	loadContinue := make(chan struct{})

	loader := func(_ context.Context) (*datastore.ReadOnlyStoredSchema, error) {
		loadCalls++
		close(loadStarted)
		<-loadContinue
		return makeTestSchema("loaded definition"), nil
	}

	const numGoroutines = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	results := make(chan error, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			schema, err := shc.GetOrLoad(t.Context(), datastore.NoRevision, SchemaHash("hash1"), loader)
			if err != nil {
				results <- err
				return
			}
			if schema == nil {
				results <- fmt.Errorf("schema is nil")
				return
			}
			results <- nil
		}()
	}

	<-loadStarted
	close(loadContinue)
	wg.Wait()
	close(results)

	for err := range results {
		require.NoError(t, err)
	}

	require.Equal(t, 1, loadCalls)
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

	err := shc.Set(SchemaHash("hash1"), makeTestSchema("definition v1 {}"))
	require.NoError(t, err)

	latest := shc.latest.Load()
	require.NotNil(t, latest)
	require.Equal(t, SchemaHash("hash1"), latest.hash)

	err = shc.Set(SchemaHash("hash2"), makeTestSchema("definition v2 {}"))
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

	schema := makeTestSchema("definition user {}")

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
		return makeTestSchema("loaded schema"), nil
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
	err := noop.Set(SchemaHash("hash"), makeTestSchema("test"))
	require.NoError(t, err)

	// GetOrLoad always calls loader
	loadCalled := false
	schema, err := noop.GetOrLoad(t.Context(), datastore.NoRevision, SchemaHash("hash"), func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
		loadCalled = true
		return makeTestSchema("loaded"), nil
	})
	require.NoError(t, err)
	require.True(t, loadCalled)
	require.Equal(t, "loaded", schema.Get().GetV1().SchemaText)
}

func TestSchemaHashCache_SlowPathCacheHit(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache())

	schema1 := makeTestSchema("definition v1 {}")
	schema2 := makeTestSchema("definition v2 {}")

	// Set hash1, then hash2 — latest now points to hash2
	err := shc.Set(SchemaHash("hash1"), schema1)
	require.NoError(t, err)
	err = shc.Set(SchemaHash("hash2"), schema2)
	require.NoError(t, err)

	shc.cache.Wait()

	// Get hash1 — latest is hash2, so fast path misses,
	// but backing cache should have it (slow path hit, line 95)
	retrieved, err := shc.get(SchemaHash("hash1"))
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "definition v1 {}", retrieved.Get().GetV1().SchemaText)
}
