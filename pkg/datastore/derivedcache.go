package datastore

import (
	"fmt"
	"sync"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// DerivedCacheKey identifies a kind of schema-derived cache (e.g. compiled caveats) hung off a
// ReadOnlyStoredSchema. Create one per kind, typically as a package-level var, via
// NewDerivedCacheKey.
type DerivedCacheKey struct{ name string }

// NewDerivedCacheKey returns a DerivedCacheKey with the given (debug) name.
func NewDerivedCacheKey(name string) DerivedCacheKey { return DerivedCacheKey{name: name} }

// Name returns the human-readable name of the key.
func (k DerivedCacheKey) Name() string { return k.name }

// derivedCacheRegistration bundles the lazy factory for a derived cache kind with an estimator
// of how many bytes that cache adds, when fully populated, on top of the schema it hangs off.
type derivedCacheRegistration struct {
	factory   func() any
	estimator func(*ReadOnlyStoredSchema) int64
}

// derivedCacheFactories maps a DerivedCacheKey to its registration. Registered once at init
// time via RegisterDerivedCache.
var derivedCacheFactories sync.Map // map[DerivedCacheKey]derivedCacheRegistration

// RegisterDerivedCache registers a derived cache kind. factory returns a fresh, empty cache and
// is invoked at most once per ReadOnlyStoredSchema instance (i.e. once per schema version).
// estimator returns a rough byte size that this cache adds, when populated, on top of the
// schema; it is summed into the schema's cache cost (see ReadOnlyStoredSchema.EstimatedSize) and
// may be nil to contribute nothing. Intended to be called from an init() function. It returns an
// error if a kind is already registered for the key.
func RegisterDerivedCache(key DerivedCacheKey, factory func() any, estimator func(*ReadOnlyStoredSchema) int64) error {
	reg := derivedCacheRegistration{factory: factory, estimator: estimator}
	if _, loaded := derivedCacheFactories.LoadOrStore(key, reg); loaded {
		return fmt.Errorf("derived schema cache already registered for key %q", key.name)
	}
	return nil
}

// EstimatedSize returns a rough byte size for this stored schema: the schema's own size plus,
// for every registered derived cache kind, that kind's estimate of the additional bytes it adds
// when populated. It is intended as the cost when caching the schema, so the cache's max-cost
// budget accounts for the derived caches the schema will accrete (compiled caveats, etc.), not
// just the schema blob. The estimate is deliberately rough and conservative (it assumes every
// kind will be populated).
func (r *ReadOnlyStoredSchema) EstimatedSize() int64 {
	if r == nil {
		return 0
	}
	size := r.schemaSize
	derivedCacheFactories.Range(func(_, v any) bool {
		if reg := v.(derivedCacheRegistration); reg.estimator != nil {
			size += reg.estimator(r)
		}
		return true
	})
	return size
}

// derivedCache returns the derived cache registered under key for this schema, building it
// once (lazily) on first access. It returns an error if no factory is registered for key,
// which indicates a programming error (a cache kind accessed without being registered at
// init).
func (r *ReadOnlyStoredSchema) derivedCache(key DerivedCacheKey) (any, error) {
	if v, ok := r.derived.Load(key); ok {
		return v, nil
	}
	reg, ok := derivedCacheFactories.Load(key)
	if !ok {
		return nil, spiceerrors.MustBugf("no derived schema cache registered for key %q", key.name)
	}
	built := reg.(derivedCacheRegistration).factory()
	actual, _ := r.derived.LoadOrStore(key, built)
	return actual, nil
}

// GetDerivedCache returns the schema-derived cache of type T registered under key for the given
// stored schema, building it lazily on first access. It returns an error if no factory is
// registered for key or if the registered factory produced a value of a type other than T,
// both of which indicate a programming error.
func GetDerivedCache[T any](r *ReadOnlyStoredSchema, key DerivedCacheKey) (T, error) {
	var zero T
	v, err := r.derivedCache(key)
	if err != nil {
		return zero, err
	}
	typed, ok := v.(T)
	if !ok {
		return zero, spiceerrors.MustBugf("derived schema cache for key %q has type %T, wanted %T", key.name, v, zero)
	}
	return typed, nil
}
