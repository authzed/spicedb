//go:build !wasm
// +build !wasm

package cache

// NewStandardCache creates a new cache with the given configuration.
func NewStandardCache[K KeyString, V any](config *Config) (Cache[K, V], error) {
	return NewRistrettoCache[K, V](config)
}

func NewStandardCacheWithMetrics[K KeyString, V any](name string, config *Config) (Cache[K, V], error) {
	return NewRistrettoCacheWithMetrics[K, V](name, config)
}
