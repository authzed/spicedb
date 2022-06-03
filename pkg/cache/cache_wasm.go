package cache

import (
	"fmt"
)

// NewCache returns an error for caching.
// This is used because ristretto cannot be built under WASM.
func NewCache(config *Config) (Cache, error) {
	return nil, fmt.Errorf("caching is currently unsupported in WASM")
}
