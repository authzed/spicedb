//go:build wasm

package cache

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// NewStandardCache returns an error for caching.
// This is used because ristretto cannot be built under WASM.
func NewStandardCache[K KeyString, V any](config *Config) (Cache[K, V], error) {
	return nil, fmt.Errorf("caching is currently unsupported in WASM")
}

func NewStandardCacheWithMetrics[K KeyString, V any](registerer prometheus.Registerer, name string, config *Config) (Cache[K, V], error) {
	return nil, fmt.Errorf("caching is currently unsupported in WASM")
}

func NewOtterCache[K KeyString, V any](name string, config *Config) (Cache[K, V], error) {
	return nil, fmt.Errorf("caching is currently unsupported in WASM")
}

func NewOtterCacheWithMetrics[K KeyString, V any](registerer prometheus.Registerer, name string, config *Config) (Cache[K, V], error) {
	return nil, fmt.Errorf("caching is currently unsupported in WASM")
}
