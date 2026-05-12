//go:build !wasm

/*
NOTE: This is the non-wasm path because the otter package contains
references to runtime code that works differently in the wasm environment
than in a "standard" golang environment and therefore cannot be built for
wasm.
*/

package cache

// NewStandardCache creates a new cache with the given configuration.
func NewStandardCache[K KeyString, V any](config *Config) (Cache[K, V], error) {
	// TODO: check name here
	return NewOtterCache[K, V]("", config)
}

func NewStandardCacheWithMetrics[K KeyString, V any](name string, config *Config) (Cache[K, V], error) {
	return NewOtterCacheWithMetrics[K, V](name, config)
}
