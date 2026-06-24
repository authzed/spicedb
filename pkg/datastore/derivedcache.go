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

// derivedCacheFactories maps a DerivedCacheKey to a factory that builds an empty cache
// instance. Registered once at init time via RegisterDerivedCache.
var derivedCacheFactories sync.Map // map[DerivedCacheKey]func() any

// RegisterDerivedCache registers a factory used to lazily build a schema-derived cache of the
// given kind. The factory returns a fresh, empty cache and is invoked at most once per
// ReadOnlyStoredSchema instance (i.e. once per schema version). Intended to be called from an
// init() function. It returns an error if a factory is already registered for the key.
func RegisterDerivedCache(key DerivedCacheKey, factory func() any) error {
	if _, loaded := derivedCacheFactories.LoadOrStore(key, factory); loaded {
		return fmt.Errorf("derived schema cache already registered for key %q", key.name)
	}
	return nil
}

// derivedCache returns the derived cache registered under key for this schema, building it
// once (lazily) on first access. It returns an error if no factory is registered for key,
// which indicates a programming error (a cache kind accessed without being registered at
// init).
func (r *ReadOnlyStoredSchema) derivedCache(key DerivedCacheKey) (any, error) {
	if v, ok := r.derived.Load(key); ok {
		return v, nil
	}
	factory, ok := derivedCacheFactories.Load(key)
	if !ok {
		return nil, spiceerrors.MustBugf("no derived schema cache registered for key %q", key.name)
	}
	built := factory.(func() any)()
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
