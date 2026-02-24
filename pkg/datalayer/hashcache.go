package datalayer

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"resenje.org/singleflight"

	"github.com/authzed/spicedb/internal/telemetry/otelconv"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var tracer = otel.Tracer("spicedb/pkg/datalayer")

// singleflightTimeout is the maximum time to wait for a singleflight peer to
// load a schema before falling back to a direct load. This prevents a possible
// deadlock when all connections in a pool are held by goroutines waiting on the
// singleflight while the singleflight leader is blocked waiting for a connection.
//
//nolint:revive // var instead of const to allow test overrides
var singleflightTimeout = 1 * time.Second

// SchemaCacheKey is the key type used for schema cache lookups.
// It implements cache.KeyString so it can be used with the cache package.
type SchemaCacheKey string

// KeyString implements cache.KeyString.
func (k SchemaCacheKey) KeyString() string { return string(k) }

// SchemaCache defines the interface for the backing cache used by schemaHashCache.
// This is satisfied by cache.Cache[SchemaCacheKey, *datastore.ReadOnlyStoredSchema].
type SchemaCache interface {
	Get(key SchemaCacheKey) (*datastore.ReadOnlyStoredSchema, bool)
	Set(key SchemaCacheKey, entry *datastore.ReadOnlyStoredSchema, cost int64) bool
	Wait()
}

// latestSchemaEntry holds the most recent schema entry for fast-path lookups.
type latestSchemaEntry struct {
	hash   SchemaHash
	schema *datastore.ReadOnlyStoredSchema
}

// schemaHashCache is a thread-safe cache for schemas indexed by hash.
// It maintains an atomic pointer to the latest schema for fast-path reads when
// multiple requests access the same (most recent) schema.
//
// The underlying cache is thread-safe, so no locks are needed for cache operations.
// The atomic latest entry provides lock-free fast-path for the common case.
type schemaHashCache struct {
	cache        SchemaCache
	latest       atomic.Pointer[latestSchemaEntry] // Fast path for latest schema
	singleflight singleflight.Group[string, *datastore.ReadOnlyStoredSchema]
}

// newSchemaHashCache creates a new hash-based schema cache wrapping the given cache.
func newSchemaHashCache(c SchemaCache) *schemaHashCache {
	return &schemaHashCache{
		cache: c,
	}
}

var _ storedSchemaCache = (*schemaHashCache)(nil)

// get retrieves a schema from the cache by revision and hash.
// Fast path: If the hash matches the atomic latest entry, return immediately.
// Slow path: Check the cache.
// Returns (nil, nil) if the hash is a bypass sentinel (NoSchemaHashInTransaction or NoSchemaHashForTesting).
// Returns error if hash is empty string (indicates a bug where schema hash wasn't properly provided).
func (c *schemaHashCache) get(schemaHash SchemaHash) (*datastore.ReadOnlyStoredSchema, error) {
	// Check for bypass sentinels - these intentionally skip the cache
	if schemaHash.IsBypassSentinel() {
		return nil, nil
	}

	// Empty hash indicates a bug - schema hash should always be provided or use a sentinel
	if schemaHash == "" {
		return nil, spiceerrors.MustBugf("empty schema hash passed to cache.Get() - use NoSchemaHashInTransaction or NoSchemaHashForTesting, or provide a real hash")
	}

	// Fast path: Check atomic latest entry
	if latest := c.latest.Load(); latest != nil && latest.hash == schemaHash {
		return latest.schema, nil
	}

	// Slow path: Check cache
	schema, ok := c.cache.Get(SchemaCacheKey(schemaHash))
	if !ok {
		return nil, nil
	}

	return schema, nil
}

// Set stores a schema in the cache by hash.
// Adds to the cache and updates the atomic latest entry.
// No-ops if hash is a bypass sentinel (NoSchemaHashInTransaction or NoSchemaHashForTesting).
// Returns error if hash is empty string (indicates a bug where it wasn't properly provided).
func (c *schemaHashCache) Set(schemaHash SchemaHash, schema *datastore.ReadOnlyStoredSchema) error {
	if schemaHash.IsBypassSentinel() {
		return nil
	}
	if schemaHash == "" {
		return spiceerrors.MustBugf("empty schema hash passed to cache.Set() - use NoSchemaHashInTransaction, NoSchemaHashForTesting, NoSchemaHashForWatch, or provide a real hash")
	}

	c.latest.Store(&latestSchemaEntry{
		hash:   schemaHash,
		schema: schema,
	})

	c.cache.Set(SchemaCacheKey(schemaHash), schema, 1)
	return nil
}

// GetOrLoad retrieves a schema from the cache, or loads it using the provided loader.
// Uses singleflight to deduplicate concurrent loads of the same hash.
// Bypasses cache for sentinel values (NoSchemaHashInTransaction or NoSchemaHashForTesting).
// Returns error if hash is empty string (indicates a bug where schema hash wasn't properly provided).
func (c *schemaHashCache) GetOrLoad(
	ctx context.Context,
	rev datastore.Revision,
	schemaHash SchemaHash,
	loader func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error),
) (*datastore.ReadOnlyStoredSchema, error) {
	// Check for bypass sentinels - load directly without caching
	if schemaHash.IsBypassSentinel() {
		schema, err := loader(ctx)
		if err != nil {
			return nil, err
		}
		return schema, nil
	}

	// Empty hash indicates a bug - schema hash should always be provided or use a sentinel
	if schemaHash == "" {
		return nil, spiceerrors.MustBugf("empty schema hash passed to cache.GetOrLoad() - use NoSchemaHashInTransaction, NoSchemaHashForTesting, NoSchemaHashForWatch, or provide a real hash")
	}

	ctx, span := tracer.Start(ctx, "SchemaCache.GetOrLoad")
	defer span.End()
	span.SetAttributes(attribute.String(otelconv.AttrSchemaHash, string(schemaHash)))

	// Try cache first
	schema, err := c.get(schemaHash)
	if err != nil {
		return nil, err
	}
	if schema != nil {
		span.SetAttributes(attribute.Bool(otelconv.AttrSchemaReadFromCache, true))
		return schema, nil
	}

	// Load with singleflight to prevent duplicate loads. Use a short timeout to
	// avoid deadlocks: if all connection pool slots are held by goroutines waiting
	// on this singleflight, the leader can't acquire a connection to load the
	// schema. The timeout lets waiters give up and fall back to loading directly,
	// freeing connections for progress to be made.
	sfCtx, cancel := context.WithTimeout(ctx, singleflightTimeout)
	defer cancel()

	result, _, err := c.singleflight.Do(sfCtx, string(schemaHash), func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
		// Check cache again in case another goroutine loaded it
		schema, err := c.get(schemaHash)
		if err != nil {
			return nil, err
		}
		if schema != nil {
			return schema, nil
		}

		// Load from datastore
		schema, err = loader(ctx)
		if err != nil {
			return nil, err
		}

		// Cache the result
		if err := c.Set(schemaHash, schema); err != nil {
			return nil, err
		}

		return schema, nil
	})
	if err != nil {
		// If the singleflight timed out but the caller's context is still valid,
		// check the cache once more (the leader may have finished between our
		// timeout and now), then fall back to loading directly.
		if sfCtx.Err() != nil && ctx.Err() == nil {
			schema, cacheErr := c.get(schemaHash)
			if cacheErr != nil {
				return nil, cacheErr
			}
			if schema != nil {
				return schema, nil
			}

			return loader(ctx)
		}
		return nil, err
	}

	span.SetAttributes(attribute.Bool(otelconv.AttrSchemaReadFromCache, false))
	return result, nil
}
