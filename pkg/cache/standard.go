//go:build !wasm

/*
NOTE: This is the non-wasm path because the otter package contains
references to runtime code that works differently in the wasm environment
than in a "standard" golang environment and therefore cannot be built for
wasm.
*/

package cache

import "github.com/prometheus/client_golang/prometheus"

// NewStandardCache creates a new cache with the given configuration. The cache
// tracks its own metrics (see Cache.GetMetrics) but does not export them.
func NewStandardCache[K KeyString, V any](config *Config) (Cache[K, V], error) {
	return NewOtterCache[K, V]("", config)
}

// NewStandardCacheWithMetrics creates a new cache and registers its metrics,
// labeled by name, with the given registerer.
func NewStandardCacheWithMetrics[K KeyString, V any](registerer prometheus.Registerer, name string, config *Config) (Cache[K, V], error) {
	return NewOtterCacheWithMetrics[K, V](registerer, name, config)
}
