package common

import (
	"context"
	"fmt"
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/singleflight"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

const defaultMaxCacheEntries = 10

// bypassSentinels contains all schema hash values that intentionally bypass the cache.
// These are special sentinel values used in specific contexts where caching is not appropriate.
// Using a map for O(1) lookup instead of slice iteration.
var bypassSentinels = map[datastore.SchemaHash]bool{
	datastore.NoSchemaHashInTransaction:   true,
	datastore.NoSchemaHashForTesting:      true,
	datastore.NoSchemaHashForWatch:        true,
	datastore.NoSchemaHashForLegacyCursor: true,
}

// isBypassSentinel returns true if the given schema hash is a bypass sentinel value.
func isBypassSentinel(schemaHash datastore.SchemaHash) bool {
	return bypassSentinels[schemaHash]
}

// latestSchemaEntry holds the most recent schema entry for fast-path lookups.
type latestSchemaEntry struct {
	revision datastore.Revision
	hash     datastore.SchemaHash
	schema   *core.StoredSchema
}

// SchemaHashCache is a thread-safe LRU cache for schemas indexed by hash.
// It maintains an atomic pointer to the latest schema for fast-path reads when
// multiple requests access the same (most recent) schema.
//
// The LRU cache itself is thread-safe, so no locks are needed for cache operations.
// The atomic latest entry provides lock-free fast-path for the common case.
type SchemaHashCache struct {
	cache        *lru.Cache[string, *core.StoredSchema] // Thread-safe LRU
	latest       atomic.Pointer[latestSchemaEntry]      // Fast path for latest schema
	singleflight singleflight.Group
}

// NewSchemaHashCache creates a new hash-based schema cache.
func NewSchemaHashCache(opts options.SchemaCacheOptions) (*SchemaHashCache, error) {
	maxEntries := int(opts.MaximumCacheEntries)
	if maxEntries == 0 {
		maxEntries = defaultMaxCacheEntries
	}

	cache, err := lru.New[string, *core.StoredSchema](maxEntries)
	if err != nil {
		return nil, fmt.Errorf("failed to create LRU cache: %w", err)
	}

	return &SchemaHashCache{
		cache: cache,
	}, nil
}

// Get retrieves a schema from the cache by revision and hash.
// Fast path: If the hash matches the atomic latest entry, return immediately.
// Slow path: Check the LRU cache (which is thread-safe).
// Returns (nil, nil) if the hash is a bypass sentinel (NoSchemaHashInTransaction or NoSchemaHashForTesting).
// Returns error if hash is empty string (indicates a bug where schema hash wasn't properly provided).
func (c *SchemaHashCache) Get(rev datastore.Revision, schemaHash datastore.SchemaHash) (*core.StoredSchema, error) {
	if c == nil {
		return nil, nil
	}

	// Check for bypass sentinels - these intentionally skip the cache
	if isBypassSentinel(schemaHash) {
		return nil, nil
	}

	// Empty hash indicates a bug - schema hash should always be provided or use a sentinel
	if schemaHash == "" {
		return nil, spiceerrors.MustBugf("empty schema hash passed to cache.Get() - use NoSchemaHashInTransaction or NoSchemaHashForTesting, or provide a real hash")
	}

	// Fast path: Check atomic latest entry
	if latest := c.latest.Load(); latest != nil && latest.hash == schemaHash {
		schemaCacheHits.Inc()
		return latest.schema, nil
	}

	// Slow path: Check LRU cache (thread-safe, no lock needed)
	schema, ok := c.cache.Get(string(schemaHash))
	if !ok {
		schemaCacheMisses.Inc()
		return nil, nil
	}

	schemaCacheHits.Inc()
	return schema, nil
}

// Set stores a schema in the cache by revision and hash.
// Adds to the LRU cache (thread-safe) and updates the atomic latest entry
// if the revision is newer or if revision is NoRevision (from transactions).
// No-ops if hash is a bypass sentinel (NoSchemaHashInTransaction or NoSchemaHashForTesting).
// Returns error if hash is empty string or if revision is nil (indicates a bug where they weren't properly provided).
func (c *SchemaHashCache) Set(rev datastore.Revision, schemaHash datastore.SchemaHash, schema *core.StoredSchema) error {
	if c == nil {
		return nil
	}

	// Check for bypass sentinels - these intentionally skip the cache
	if isBypassSentinel(schemaHash) {
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

	// Add to LRU cache (thread-safe, no lock needed)
	c.cache.Add(string(schemaHash), schema)

	// Update atomic latest if this is newer or if no revision check (txn case)
	shouldUpdateLatest := rev == datastore.NoRevision

	if !shouldUpdateLatest {
		// Check if this revision is newer than the current latest
		if latest := c.latest.Load(); latest != nil {
			// If we have a latest entry with a revision, compare
			if latest.revision != nil && latest.revision != datastore.NoRevision {
				shouldUpdateLatest = rev.GreaterThan(latest.revision)
			} else {
				// Current latest has no revision, so update with this one
				shouldUpdateLatest = true
			}
		} else {
			// No latest entry yet, so set it
			shouldUpdateLatest = true
		}
	}

	if shouldUpdateLatest {
		c.latest.Store(&latestSchemaEntry{
			revision: rev,
			hash:     schemaHash,
			schema:   schema,
		})
	}

	return nil
}

// GetOrLoad retrieves a schema from the cache, or loads it using the provided loader.
// Uses singleflight to deduplicate concurrent loads of the same hash.
// Bypasses cache for sentinel values (NoSchemaHashInTransaction or NoSchemaHashForTesting).
// Returns error if hash is empty string (indicates a bug where schema hash wasn't properly provided).
func (c *SchemaHashCache) GetOrLoad(
	ctx context.Context,
	rev datastore.Revision,
	schemaHash datastore.SchemaHash,
	loader func(ctx context.Context) (*core.StoredSchema, error),
) (*core.StoredSchema, error) {
	// Check for bypass sentinels - load directly without caching
	if c == nil || isBypassSentinel(schemaHash) {
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
	schema, err := c.Get(rev, schemaHash)
	if err != nil {
		return nil, err
	}
	if schema != nil {
		return schema, nil
	}

	// Load with singleflight to prevent duplicate loads
	result, err, _ := c.singleflight.Do(string(schemaHash), func() (any, error) {
		// Check cache again in case another goroutine loaded it
		schema, err := c.Get(rev, schemaHash)
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
		if err := c.Set(rev, schemaHash, schema); err != nil {
			return nil, err
		}

		return schema, nil
	})

	if err != nil {
		return nil, err
	}

	return result.(*core.StoredSchema), nil
}
