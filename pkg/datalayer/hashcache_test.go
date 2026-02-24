package datalayer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/revisions"
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
	shc := newSchemaHashCache(newTestSchemaCache()).(*schemaHashCache)

	// Cache miss
	retrieved, err := shc.get(datastore.NoRevision, SchemaHash("hash1"))
	require.NoError(t, err)
	require.Nil(t, retrieved)

	// Set and get
	schema := makeTestSchema("definition user {}")
	err = shc.Set(datastore.NoRevision, SchemaHash("hash1"), schema)
	require.NoError(t, err)

	shc.cache.Wait()

	retrieved, err = shc.get(datastore.NoRevision, SchemaHash("hash1"))
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, schema.Get().GetV1().SchemaText, retrieved.Get().GetV1().SchemaText)
}

func TestSchemaHashCache_EmptyHash(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache()).(*schemaHashCache)

	require.Panics(t, func() {
		_ = shc.Set(datastore.NoRevision, SchemaHash(""), makeTestSchema("definition user {}"))
	}, "empty hash should panic")

	require.Panics(t, func() {
		_, _ = shc.get(datastore.NoRevision, SchemaHash(""))
	}, "empty hash should panic")
}

func TestSchemaHashCache_NilRevision(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache())

	require.Panics(t, func() {
		_ = shc.Set(nil, SchemaHash("hash1"), makeTestSchema("definition user {}"))
	}, "nil revision should panic")
}

func TestSchemaHashCache_GetOrLoad(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache())

	loadCalls := 0
	loader := func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
		loadCalls++
		return makeTestSchema("loaded definition"), nil
	}

	// First call should load
	schema, err := shc.GetOrLoad(context.Background(), datastore.NoRevision, SchemaHash("hash1"), loader)
	require.NoError(t, err)
	require.NotNil(t, schema)
	require.Equal(t, "loaded definition", schema.Get().GetV1().SchemaText)
	require.Equal(t, 1, loadCalls)

	// Second call should hit cache
	schema, err = shc.GetOrLoad(context.Background(), datastore.NoRevision, SchemaHash("hash1"), loader)
	require.NoError(t, err)
	require.NotNil(t, schema)
	require.Equal(t, 1, loadCalls)
}

func TestSchemaHashCache_GetOrLoadEmptyHash(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache())

	require.Panics(t, func() {
		_, _ = shc.GetOrLoad(context.Background(), datastore.NoRevision, SchemaHash(""), func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
			return makeTestSchema("loaded definition"), nil
		})
	}, "empty hash should panic")
}

func TestSchemaHashCache_Singleflight(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache())

	loadCalls := 0
	loadStarted := make(chan struct{})
	loadContinue := make(chan struct{})

	loader := func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
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
			schema, err := shc.GetOrLoad(context.Background(), datastore.NoRevision, SchemaHash("hash1"), loader)
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
	schema, err := shc.GetOrLoad(context.Background(), datastore.NoRevision, SchemaHash("hash1"), func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
		return nil, expectedErr
	})
	require.Error(t, err)
	require.Equal(t, expectedErr, err)
	require.Nil(t, schema)
}

func TestSchemaHashCache_SetRevisionComparison(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache()).(*schemaHashCache)

	rev1 := revisions.NewForTransactionID(1)
	rev2 := revisions.NewForTransactionID(2)
	rev3 := revisions.NewForTransactionID(3)

	// Set with rev2
	err := shc.Set(rev2, SchemaHash("hash2"), makeTestSchema("definition v2 {}"))
	require.NoError(t, err)

	latest := shc.latest.Load()
	require.NotNil(t, latest)
	require.Equal(t, SchemaHash("hash2"), latest.hash)

	// Set with rev1 (older) - should NOT update latest
	err = shc.Set(rev1, SchemaHash("hash1"), makeTestSchema("definition v1 {}"))
	require.NoError(t, err)

	latest = shc.latest.Load()
	require.Equal(t, SchemaHash("hash2"), latest.hash)

	// Set with rev3 (newer) - should update latest
	err = shc.Set(rev3, SchemaHash("hash3"), makeTestSchema("definition v3 {}"))
	require.NoError(t, err)

	latest = shc.latest.Load()
	require.Equal(t, SchemaHash("hash3"), latest.hash)
}

func TestSchemaHashCache_SetNoRevisionUpdatesLatest(t *testing.T) {
	shc := newSchemaHashCache(newTestSchemaCache()).(*schemaHashCache)

	rev2 := revisions.NewForTransactionID(2)

	err := shc.Set(rev2, SchemaHash("hashRev"), makeTestSchema("definition rev2 {}"))
	require.NoError(t, err)

	latest := shc.latest.Load()
	require.Equal(t, SchemaHash("hashRev"), latest.hash)

	// NoRevision should always update latest (transaction case)
	err = shc.Set(datastore.NoRevision, SchemaHash("hashTxn"), makeTestSchema("definition txn {}"))
	require.NoError(t, err)

	latest = shc.latest.Load()
	require.Equal(t, SchemaHash("hashTxn"), latest.hash)
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
		err := shc.Set(datastore.NoRevision, sentinel, schema)
		require.NoError(t, err)

		// GetOrLoad should always call loader
		loadCalled := false
		result, err := shc.GetOrLoad(context.Background(), datastore.NoRevision, sentinel, func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
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

	slowLoader := func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
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
		_, leaderErr = shc.GetOrLoad(context.Background(), datastore.NoRevision, hash, slowLoader)
	}()

	<-leaderStarted

	// Waiter should fall back after timeout
	schema, err := shc.GetOrLoad(context.Background(), datastore.NoRevision, hash, slowLoader)
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
	err := noop.Set(datastore.NoRevision, SchemaHash("hash"), makeTestSchema("test"))
	require.NoError(t, err)

	// GetOrLoad always calls loader
	loadCalled := false
	schema, err := noop.GetOrLoad(context.Background(), datastore.NoRevision, SchemaHash("hash"), func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
		loadCalled = true
		return makeTestSchema("loaded"), nil
	})
	require.NoError(t, err)
	require.True(t, loadCalled)
	require.Equal(t, "loaded", schema.Get().GetV1().SchemaText)
}
