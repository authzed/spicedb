//go:build wasm

package keys

func dispatchCacheKeyHash(prefix cachePrefix, atRevision string, computeOption dispatchCacheKeyHashComputeOption, args ...hashableValue) DispatchCacheKey {
	panic("Caching is not implemented under WASM")
}
