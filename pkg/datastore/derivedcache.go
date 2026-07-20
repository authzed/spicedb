package datastore

import (
	"sync/atomic"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// compiledCaveatOverheadBytes is a rough per-caveat allowance for the memory a compiled caveat
// (a built CEL environment and program) adds on top of its serialized expression. It is used
// only for cache-cost budgeting of the schema-derived compiled-caveat cache (built lazily by
// internal/caveats and hung off the stored schema); it is a deliberately conservative,
// order-of-magnitude figure.
const compiledCaveatOverheadBytes = 8 * 1024

// derivedCacheKeyIDs hands each DerivedCacheKey a distinct identity, so different cache kinds
// never collide even though they share a schema's derived-cache map.
var derivedCacheKeyIDs atomic.Uint64

// DerivedCacheKey identifies one kind of schema-derived cache (compiled caveats, type systems,
// reachability, ...) hung off a ReadOnlyStoredSchema, and binds that kind to the cache's Go type
// T. Create exactly one per kind, as a package-level var, via NewDerivedCacheKey. The type
// parameter makes LoadOrStoreDerived type-safe at compile time — there are no string keys and no
// reflection; lookup is a single uint64-keyed map access.
type DerivedCacheKey[T any] struct {
	id uint64
}

// NewDerivedCacheKey mints a fresh key identifying a new kind of schema-derived cache of type T.
// Call it once per kind and store the result in a package-level var; minting a key per call would
// defeat sharing (each key is a distinct cache).
func NewDerivedCacheKey[T any]() DerivedCacheKey[T] {
	return DerivedCacheKey[T]{id: derivedCacheKeyIDs.Add(1)}
}

// LoadOrStoreDerived returns the schema-derived cache identified by key for the given stored
// schema, building it lazily (via build) on first access and reusing it thereafter. The cache
// lives exactly as long as the ReadOnlyStoredSchema it hangs off — a single schema version — and
// is discarded with it. This lets internal packages (compiled caveats, type systems,
// reachability, ...) attach schema-version-scoped caches to the stored schema without
// pkg/datastore depending on them.
//
// build is invoked at most once per (schema, key), on the goroutine that wins the race;
// concurrent first accesses may each build, but only one result is retained. key's type parameter
// binds the stored value's type, so the returned error is a defensive guard that should never
// fire in practice.
func LoadOrStoreDerived[T any](r *ReadOnlyStoredSchema, key DerivedCacheKey[T], build func() T) (T, error) {
	var zero T
	if v, ok := r.derived.Load(key.id); ok {
		typed, ok := v.(T)
		if !ok {
			return zero, spiceerrors.MustBugf("derived schema cache has type %T, wanted %T", v, zero)
		}
		return typed, nil
	}

	actual, _ := r.derived.LoadOrStore(key.id, build())
	typed, ok := actual.(T)
	if !ok {
		return zero, spiceerrors.MustBugf("derived schema cache has type %T, wanted %T", actual, zero)
	}
	return typed, nil
}

// EstimatedSize returns a rough byte size for this stored schema, used as the cost when caching
// it so the cache's max-cost budget reflects memory rather than entry count. It is the schema's
// own size plus a conservative allowance for the compiled-caveat cache the schema accretes when
// checks run: per caveat, its serialized expression plus a fixed compiled-CEL overhead. The
// estimate is deliberately rough and assumes every caveat will be compiled and cached.
func (r *ReadOnlyStoredSchema) EstimatedSize() int64 {
	if r == nil {
		return 0
	}

	size := r.schemaSize
	if v1 := r.schema.GetV1(); v1 != nil {
		for _, caveat := range v1.GetCaveatDefinitions() {
			size += spiceerrors.MustSafecast[int64](len(caveat.GetSerializedExpression())) + compiledCaveatOverheadBytes
		}
	}
	return size
}
