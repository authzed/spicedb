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
// Concurrent first accesses may each invoke build, but only one result is retained. key's type parameter
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

// CompiledQueryPlanCacheBudget reserves a bounded byte allowance for lazy,
// schema-dependent compiled plans. Small schemas receive 512 KiB; larger schemas
// scale with their stored size up to 4 MiB. The schema cache charges this allowance
// before admission so later plan construction cannot expand its accounted budget.
func (r *ReadOnlyStoredSchema) CompiledQueryPlanCacheBudget() int64 {
	if r == nil {
		return 0
	}

	return min(4<<20, max(512<<10, 16*r.schemaSize))
}

// EstimatedSize returns a rough byte size for this stored schema, used as the cost when caching
// it so the cache's max-cost budget reflects memory rather than entry count. It is the schema's
// own size plus conservative allowances for the converted query-planner schema and
// compiled-caveat caches and bounded compiled plans the schema accretes when checks run. Compiled caveats
// reserve their serialized expressions plus a fixed compiled-CEL overhead. The
// estimate assumes the complete view and every compiled caveat will be retained.
func (r *ReadOnlyStoredSchema) EstimatedSize() int64 {
	if r == nil {
		return 0
	}

	size := r.schemaSize + r.CompiledQueryPlanCacheBudget()

	if v1 := r.schema.GetV1(); v1 != nil {
		// Converted nodes include maps, parent pointers, and operation objects.
		// Reserve their cost before lazy construction, without coupling datastore
		// to the planner model. Four times the wire size plus 512 bytes per
		// definition is a rough allowance, rather than measured heap accounting.
		for _, namespace := range v1.GetNamespaceDefinitions() {
			size += 4*spiceerrors.MustSafecast[int64](namespace.SizeVT()) + 512
		}

		for _, caveat := range v1.GetCaveatDefinitions() {
			size += 4*spiceerrors.MustSafecast[int64](caveat.SizeVT()) + 512
			size += spiceerrors.MustSafecast[int64](len(caveat.GetSerializedExpression())) + compiledCaveatOverheadBytes
		}
	}
	return size
}
