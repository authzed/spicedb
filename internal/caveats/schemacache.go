package caveats

import (
	"sync"

	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// compiledCaveatCacheKey identifies the schema-derived cache of compiled (deserialized)
// caveats. The cache is tied to a single stored-schema version (via datastore.ReadOnlyStoredSchema)
// and is discarded when the schema changes.
var compiledCaveatCacheKey = datastore.NewDerivedCacheKey("caveats.compiled")

// compiledCaveatOverheadBytes is a rough per-caveat estimate of the memory a compiled caveat
// adds on top of its serialized expression. A compiled caveat embeds a built CEL environment
// and program, which dwarf the serialized expression; this is a deliberately conservative,
// order-of-magnitude figure used only for cache-cost budgeting.
const compiledCaveatOverheadBytes = 8 * 1024

func init() {
	if err := datastore.RegisterDerivedCache(compiledCaveatCacheKey, func() any { return &CompiledCaveatCache{} }, estimateCompiledCaveatCacheSize); err != nil {
		spiceerrors.MustPanicf("failed to register compiled caveat cache: %v", err)
	}
}

// estimateCompiledCaveatCacheSize roughly estimates the bytes the compiled-caveat cache adds for
// the given schema when fully populated: per caveat, the serialized expression plus a fixed
// overhead for its compiled CEL environment.
func estimateCompiledCaveatCacheSize(s *datastore.ReadOnlyStoredSchema) int64 {
	v1 := s.Get().GetV1()
	if v1 == nil {
		return 0
	}
	total := 0
	for _, caveat := range v1.GetCaveatDefinitions() {
		total += len(caveat.GetSerializedExpression()) + compiledCaveatOverheadBytes
	}
	return int64(total)
}

// CompiledCaveatCache caches deserialized caveats (which embed a built CEL environment) by
// caveat name, for a single schema version. Deserializing a caveat rebuilds its CEL
// environment, which is expensive; caching it on the (shared) stored schema avoids paying
// that cost on every check.
type CompiledCaveatCache struct {
	m sync.Map // map[string]*caveats.CompiledCaveat
}

// GetOrCompile returns the cached compiled caveat for name, or invokes compile and caches
// the result. compile is only called on a miss; concurrent misses may call compile more
// than once but only one result is retained.
func (c *CompiledCaveatCache) GetOrCompile(name string, compile func() (*caveats.CompiledCaveat, error)) (*caveats.CompiledCaveat, error) {
	if v, ok := c.m.Load(name); ok {
		return v.(*caveats.CompiledCaveat), nil
	}
	compiled, err := compile()
	if err != nil {
		return nil, err
	}
	actual, _ := c.m.LoadOrStore(name, compiled)
	return actual.(*caveats.CompiledCaveat), nil
}

// CompiledCaveatCacheFor returns the compiled-caveat cache tied to the given stored schema,
// building it lazily on first access.
func CompiledCaveatCacheFor(s *datastore.ReadOnlyStoredSchema) (*CompiledCaveatCache, error) {
	return datastore.GetDerivedCache[*CompiledCaveatCache](s, compiledCaveatCacheKey)
}
