package datalayer

import (
	"context"
	"sync/atomic"
	"time"

	"resenje.org/singleflight"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

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
	revision datastore.Revision
	hash     SchemaHash
	schema   *datastore.ReadOnlyStoredSchema
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
func newSchemaHashCache(c SchemaCache) storedSchemaCache {
	return &schemaHashCache{
		cache: c,
	}
}

// get retrieves a schema from the cache by revision and hash.
// Fast path: If the hash matches the atomic latest entry, return immediately.
// Slow path: Check the cache.
// Returns (nil, nil) if the hash is a bypass sentinel (NoSchemaHashInTransaction or NoSchemaHashForTesting).
// Returns error if hash is empty string (indicates a bug where schema hash wasn't properly provided).
func (c *schemaHashCache) get(_ datastore.Revision, schemaHash SchemaHash) (*datastore.ReadOnlyStoredSchema, error) {
	// TODO(jschorr): Further optimize if we can use the revision in the case when the schema hash does not match,
	// and perhaps have a cache-by-revision. For now we just ignore the revision in Get since the cache is keyed by hash,
	// but we could potentially use it later.

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

// Set stores a schema in the cache by revision and hash.
// Adds to the cache and updates the atomic latest entry
// if the revision is newer or if revision is NoRevision (from transactions).
// No-ops if hash is a bypass sentinel (NoSchemaHashInTransaction or NoSchemaHashForTesting).
// Returns error if hash is empty string or if revision is nil (indicates a bug where they weren't properly provided).
func (c *schemaHashCache) Set(rev datastore.Revision, schemaHash SchemaHash, schema *datastore.ReadOnlyStoredSchema) error {
	// Check for bypass sentinels - these intentionally skip the cache
	if schemaHash.IsBypassSentinel() {
		return nil
	}

	// Empty hash indicates a bug - schema hash should always be provided or use a sentinel
	if schemaHash == "" {
		return spiceerrors.MustBugf("empty schema hash passed to cache.Set() - use NoSchemaHashInTransaction, NoSchemaHashForTesting, NoSchemaHashForWatch, or provide a real hash")
	}

	// Nil revision indicates a bug - should use NoRevision for transaction cases
	if rev == nil {
		return spiceerrors.MustBugf("nil revision passed to cache.Set() - use datastore.NoRevision for transaction cases")
	}

	// Update atomic latest via CAS loop
	newEntry := &latestSchemaEntry{
		revision: rev,
		hash:     schemaHash,
		schema:   schema,
	}
	for {
		current := c.latest.Load()
		if current != nil && rev != datastore.NoRevision &&
			current.revision != nil && current.revision != datastore.NoRevision &&
			!rev.GreaterThan(current.revision) {
			// Current latest is already at or ahead of this revision
			break
		}
		if c.latest.CompareAndSwap(current, newEntry) {
			break
		}
	}

	// Add to cache
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

	// Try cache first
	schema, err := c.get(rev, schemaHash)
	if err != nil {
		return nil, err
	}
	if schema != nil {
		return schema, nil
	}

	// Load with singleflight to prevent duplicate loads. Use a short timeout to
	// avoid deadlocks: if all connection pool slots are held by goroutines waiting
	// on this singleflight, the leader can't acquire a connection to load the
	// schema. The timeout lets waiters give up and fall back to loading directly,
	// freeing connections for progress to be made.
	sfCtx, cancel := context.WithTimeout(ctx, singleflightTimeout)
	defer cancel()

	result, _, err := c.singleflight.Do(sfCtx, string(schemaHash), func(_ context.Context) (*datastore.ReadOnlyStoredSchema, error) {
		// Check cache again in case another goroutine loaded it
		schema, err := c.get(rev, schemaHash)
		if err != nil {
			return nil, err
		}
		if schema != nil {
			return schema, nil
		}

		// Load from datastore
		schema, err = loader(sfCtx)
		if err != nil {
			return nil, err
		}

		// Cache the result
		if err := c.Set(rev, schemaHash, schema); err != nil {
			return nil, err
		}

		return schema, nil
	})
	if err != nil {
		// If the singleflight timed out but the caller's context is still valid,
		// check the cache once more (the leader may have finished between our
		// timeout and now), then fall back to loading directly.
		if sfCtx.Err() != nil && ctx.Err() == nil {
			schema, cacheErr := c.get(rev, schemaHash)
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

	return result, nil
}
