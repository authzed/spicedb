package cache

import (
	"fmt"
)

// NewStandardCache returns an error for caching.
// This is used because ristretto cannot be built under WASM.
func NewStandardCache[K KeyString, V any](config *Config) (Cache[K, V], error) {
	return nil, fmt.Errorf("caching is currently unsupported in WASM")
}

func NewStandardCacheWithMetrics[K KeyString, V any](name string, config *Config) (Cache[K, V], error) {
	return nil, fmt.Errorf("caching is currently unsupported in WASM")
}

func NewRistrettoCache[K KeyString, V any](config *Config) (Cache[K, V], error) {
	return nil, fmt.Errorf("caching is currently unsupported in WASM")
}

func NewTheineCache[K KeyString, V any](config *Config) (Cache[K, V], error) {
	return nil, fmt.Errorf("caching is currently unsupported in WASM")
}

func NewOtterCache[K KeyString, V any](config *Config) (Cache[K, V], error) {
	return nil, fmt.Errorf("caching is currently unsupported in WASM")
}
