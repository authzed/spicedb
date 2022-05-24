//go:build !wasm
// +build !wasm

package cache

import (
	"github.com/dgraph-io/ristretto"
)

// NewCache creates a new ristretto cache from the given config.
func NewCache(config *Config) (Cache, error) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: config.NumCounters,
		MaxCost:     config.MaxCost,
		BufferItems: config.BufferItems,
		Metrics:     config.Metrics,
	})
	if err != nil {
		return nil, err
	}
	return wrapped{cache}, nil
}

type wrapped struct {
	*ristretto.Cache
}

func (w wrapped) GetMetrics() Metrics {
	return w.Cache.Metrics
}

var _ Cache = &wrapped{}
