package caveats

import (
	"sync"

	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// compiledCaveatCacheKey identifies the schema-derived cache of compiled (deserialized)
// caveats. The cache is tied to a single stored-schema version (via datalayer.CachedSchema)
// and is discarded when the schema changes.
var compiledCaveatCacheKey = datalayer.NewDerivedCacheKey("caveats.compiled")

func init() {
	if err := datalayer.RegisterDerivedCache(compiledCaveatCacheKey, func() any { return &CompiledCaveatCache{} }); err != nil {
		spiceerrors.MustPanicf("failed to register compiled caveat cache: %v", err)
	}
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

// CompiledCaveatCacheFor returns the compiled-caveat cache tied to the given cached schema,
// building it lazily on first access.
func CompiledCaveatCacheFor(s *datalayer.CachedSchema) *CompiledCaveatCache {
	return datalayer.GetDerivedCache[*CompiledCaveatCache](s, compiledCaveatCacheKey)
}
