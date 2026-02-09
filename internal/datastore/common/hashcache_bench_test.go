package common

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/singleflight"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// AtomicSchemaHashCache is an atomic pointer-based implementation for comparison
type AtomicSchemaHashCache struct {
	cache        atomic.Pointer[lru.Cache[string, *core.StoredSchema]] // GUARDED_BY(mu)
	singleflight singleflight.Group
	mu           sync.Mutex // Only for writes
}

func NewAtomicSchemaHashCache(opts options.SchemaCacheOptions) (*AtomicSchemaHashCache, error) {
	maxEntries := int(opts.MaximumCacheEntries)
	if maxEntries == 0 {
		maxEntries = defaultMaxCacheEntries
	}

	cache, err := lru.New[string, *core.StoredSchema](maxEntries)
	if err != nil {
		return nil, fmt.Errorf("failed to create LRU cache: %w", err)
	}

	c := &AtomicSchemaHashCache{}
	c.cache.Store(cache)
	return c, nil
}

func (c *AtomicSchemaHashCache) Get(schemaHash string) *core.StoredSchema {
	if c == nil || schemaHash == "" {
		return nil
	}

	cache := c.cache.Load()
	if cache == nil {
		return nil
	}

	schema, ok := cache.Get(schemaHash)
	if !ok {
		schemaCacheMisses.Inc()
		return nil
	}

	schemaCacheHits.Inc()
	return schema
}

func (c *AtomicSchemaHashCache) Set(schemaHash string, schema *core.StoredSchema) {
	if c == nil || schemaHash == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	cache := c.cache.Load()
	if cache != nil {
		cache.Add(schemaHash, schema)
	}
}

func (c *AtomicSchemaHashCache) GetOrLoad(
	ctx context.Context,
	schemaHash string,
	loader func(ctx context.Context) (*core.StoredSchema, error),
) (*core.StoredSchema, error) {
	if c == nil || schemaHash == "" {
		schema, err := loader(ctx)
		if err != nil {
			return nil, err
		}
		return schema, nil
	}

	if schema := c.Get(schemaHash); schema != nil {
		return schema, nil
	}

	result, err, _ := c.singleflight.Do(schemaHash, func() (any, error) {
		if schema := c.Get(schemaHash); schema != nil {
			return schema, nil
		}

		schema, err := loader(ctx)
		if err != nil {
			return nil, err
		}

		c.Set(schemaHash, schema)

		return schema, nil
	})

	if err != nil {
		return nil, err
	}

	return result.(*core.StoredSchema), nil
}

// Benchmarks

func createTestSchema(id string) *core.StoredSchema {
	return &core.StoredSchema{
		Version: 1,
		VersionOneof: &core.StoredSchema_V1{
			V1: &core.StoredSchema_V1StoredSchema{
				SchemaText: "definition test_" + id + " {}",
			},
		},
	}
}

// BenchmarkHashCache_Get_Mutex tests read performance with mutex-based cache
func BenchmarkHashCache_Get_Mutex(b *testing.B) {
	cache, _ := NewSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 100,
	})

	// Pre-populate cache
	for i := 0; i < 10; i++ {
		hash := fmt.Sprintf("hash%d", i)
		cache.Set(datastore.NoRevision, datastore.SchemaHash(hash), createTestSchema(hash))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			hash := fmt.Sprintf("hash%d", i%10)
			_, _ = cache.Get(datastore.NoRevision, datastore.SchemaHash(hash))
			i++
		}
	})
}

// BenchmarkHashCache_Get_Atomic tests read performance with atomic-based cache
func BenchmarkHashCache_Get_Atomic(b *testing.B) {
	cache, _ := NewAtomicSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 100,
	})

	// Pre-populate cache
	for i := 0; i < 10; i++ {
		hash := fmt.Sprintf("hash%d", i)
		cache.Set(hash, createTestSchema(hash))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			hash := fmt.Sprintf("hash%d", i%10)
			_ = cache.Get(hash)
			i++
		}
	})
}

// BenchmarkHashCache_Set_Mutex tests write performance with mutex-based cache
func BenchmarkHashCache_Set_Mutex(b *testing.B) {
	cache, _ := NewSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 100,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash := fmt.Sprintf("hash%d", i%100)
		cache.Set(datastore.NoRevision, datastore.SchemaHash(hash), createTestSchema(hash))
	}
}

// BenchmarkHashCache_Set_Atomic tests write performance with atomic-based cache
func BenchmarkHashCache_Set_Atomic(b *testing.B) {
	cache, _ := NewAtomicSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 100,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash := fmt.Sprintf("hash%d", i%100)
		cache.Set(hash, createTestSchema(hash))
	}
}

// BenchmarkHashCache_Mixed_Mutex tests mixed read/write with mutex (90% reads, 10% writes)
func BenchmarkHashCache_Mixed_Mutex(b *testing.B) {
	cache, _ := NewSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 100,
	})

	// Pre-populate cache
	for i := 0; i < 50; i++ {
		hash := fmt.Sprintf("hash%d", i)
		cache.Set(datastore.NoRevision, datastore.SchemaHash(hash), createTestSchema(hash))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			hash := fmt.Sprintf("hash%d", i%100)
			if i%10 == 0 {
				// 10% writes
				cache.Set(datastore.NoRevision, datastore.SchemaHash(hash), createTestSchema(hash))
			} else {
				// 90% reads
				_, _ = cache.Get(datastore.NoRevision, datastore.SchemaHash(hash))
			}
			i++
		}
	})
}

// BenchmarkHashCache_Mixed_Atomic tests mixed read/write with atomic (90% reads, 10% writes)
func BenchmarkHashCache_Mixed_Atomic(b *testing.B) {
	cache, _ := NewAtomicSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 100,
	})

	// Pre-populate cache
	for i := 0; i < 50; i++ {
		hash := fmt.Sprintf("hash%d", i)
		cache.Set(hash, createTestSchema(hash))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			hash := fmt.Sprintf("hash%d", i%100)
			if i%10 == 0 {
				// 10% writes
				cache.Set(hash, createTestSchema(hash))
			} else {
				// 90% reads
				_ = cache.Get(hash)
			}
			i++
		}
	})
}

// BenchmarkHashCache_GetOrLoad_Mutex tests GetOrLoad with mutex
func BenchmarkHashCache_GetOrLoad_Mutex(b *testing.B) {
	cache, _ := NewSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 100,
	})

	ctx := context.Background()
	loader := func(ctx context.Context) (*core.StoredSchema, error) {
		return createTestSchema("loaded"), nil
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			hash := fmt.Sprintf("hash%d", i%10)
			_, _ = cache.GetOrLoad(ctx, datastore.NoRevision, datastore.SchemaHash(hash), loader)
			i++
		}
	})
}

// BenchmarkHashCache_GetOrLoad_Atomic tests GetOrLoad with atomic
func BenchmarkHashCache_GetOrLoad_Atomic(b *testing.B) {
	cache, _ := NewAtomicSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 100,
	})

	ctx := context.Background()
	loader := func(ctx context.Context) (*core.StoredSchema, error) {
		return createTestSchema("loaded"), nil
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			hash := fmt.Sprintf("hash%d", i%10)
			_, _ = cache.GetOrLoad(ctx, hash, loader)
			i++
		}
	})
}

// BenchmarkHashCache_HighContention_Mutex tests high contention scenario with mutex
func BenchmarkHashCache_HighContention_Mutex(b *testing.B) {
	cache, _ := NewSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 100,
	})

	// Pre-populate with single entry
	cache.Set(datastore.NoRevision, datastore.SchemaHash("shared"), createTestSchema("shared"))

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Everyone hits the same cache entry
			_, _ = cache.Get(datastore.NoRevision, datastore.SchemaHash("shared"))
		}
	})
}

// BenchmarkHashCache_HighContention_Atomic tests high contention scenario with atomic
func BenchmarkHashCache_HighContention_Atomic(b *testing.B) {
	cache, _ := NewAtomicSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 100,
	})

	// Pre-populate with single entry
	cache.Set("shared", createTestSchema("shared"))

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Everyone hits the same cache entry
			_ = cache.Get("shared")
		}
	})
}

// BenchmarkHashCache_LowContention_Mutex tests low contention with mutex
func BenchmarkHashCache_LowContention_Mutex(b *testing.B) {
	cache, _ := NewSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 100,
	})

	// Pre-populate with many entries
	for i := 0; i < 100; i++ {
		hash := fmt.Sprintf("hash%d", i)
		cache.Set(datastore.NoRevision, datastore.SchemaHash(hash), createTestSchema(hash))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Each goroutine tends to hit different entries
			hash := fmt.Sprintf("hash%d", i%100)
			_, _ = cache.Get(datastore.NoRevision, datastore.SchemaHash(hash))
			i++
		}
	})
}

// BenchmarkHashCache_LowContention_Atomic tests low contention with atomic
func BenchmarkHashCache_LowContention_Atomic(b *testing.B) {
	cache, _ := NewAtomicSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 100,
	})

	// Pre-populate with many entries
	for i := 0; i < 100; i++ {
		hash := fmt.Sprintf("hash%d", i)
		cache.Set(hash, createTestSchema(hash))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Each goroutine tends to hit different entries
			hash := fmt.Sprintf("hash%d", i%100)
			_ = cache.Get(hash)
			i++
		}
	})
}
