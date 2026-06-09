package datalayer

import (
	"fmt"
	"sync"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// CachedSchema bundles a read-only stored schema with lazily-built, schema-derived caches
// (for example compiled caveats, type systems, or reachability). Instances are produced and
// shared by the datalayer's stored-schema cache, so the derived caches live exactly as long
// as the schema version they belong to and are discarded, together, when the schema changes.
type CachedSchema struct {
	schema  *datastore.ReadOnlyStoredSchema
	derived sync.Map // map[DerivedCacheKey]any
}

// NewCachedSchema wraps a stored schema. Returns nil if schema is nil.
func NewCachedSchema(schema *datastore.ReadOnlyStoredSchema) *CachedSchema {
	if schema == nil {
		return nil
	}
	return &CachedSchema{schema: schema}
}

// Schema returns the underlying read-only stored schema.
func (c *CachedSchema) Schema() *datastore.ReadOnlyStoredSchema { return c.schema }

// DerivedCacheKey identifies a kind of schema-derived cache (e.g. compiled caveats). Create
// one per kind, typically as a package-level var, via NewDerivedCacheKey.
type DerivedCacheKey struct{ name string }

// NewDerivedCacheKey returns a DerivedCacheKey with the given (debug) name.
func NewDerivedCacheKey(name string) DerivedCacheKey { return DerivedCacheKey{name: name} }

// Name returns the human-readable name of the key.
func (k DerivedCacheKey) Name() string { return k.name }

// derivedCacheFactories maps a DerivedCacheKey to a factory that builds an empty cache
// instance. Registered once at init time via RegisterDerivedCache.
var derivedCacheFactories sync.Map // map[DerivedCacheKey]func() any

// RegisterDerivedCache registers a factory used to lazily build a schema-derived cache of the
// given kind. The factory returns a fresh, empty cache and is invoked at most once per
// CachedSchema instance (i.e. once per schema version). Intended to be called from an init()
// function. It returns an error if a factory is already registered for the key.
func RegisterDerivedCache(key DerivedCacheKey, factory func() any) error {
	if _, loaded := derivedCacheFactories.LoadOrStore(key, factory); loaded {
		return fmt.Errorf("derived schema cache already registered for key %q", key.name)
	}
	return nil
}

// derivedCache returns the derived cache registered under key for this schema, building it
// once (lazily) on first access. It panics if no factory is registered for key, which
// indicates a programming error (a cache kind accessed without being registered at init).
func (c *CachedSchema) derivedCache(key DerivedCacheKey) any {
	if v, ok := c.derived.Load(key); ok {
		return v
	}
	factory, ok := derivedCacheFactories.Load(key)
	if !ok {
		spiceerrors.MustPanicf("no derived schema cache registered for key %q", key.name)
		return nil
	}
	built := factory.(func() any)()
	actual, _ := c.derived.LoadOrStore(key, built)
	return actual
}

// GetDerivedCache returns the schema-derived cache of type T registered under key for the
// given cached schema, building it lazily on first access.
func GetDerivedCache[T any](c *CachedSchema, key DerivedCacheKey) T {
	return c.derivedCache(key).(T)
}
