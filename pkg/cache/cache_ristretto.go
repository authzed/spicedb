//go:build !wasm
// +build !wasm

package cache

import (
	"github.com/outcaste-io/ristretto"
	"github.com/outcaste-io/ristretto/z"
	"github.com/rs/zerolog"

	"github.com/authzed/spicedb/internal/dispatch/keys"
)

// NewCache creates a new ristretto cache from the given config.
func NewCache(config *Config) (Cache, error) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: config.NumCounters,
		MaxCost:     config.MaxCost,
		BufferItems: 64, // Recommended constant by Ristretto authors.
		Metrics:     config.Metrics,
		KeyToHash: func(key interface{}) (uint64, uint64) {
			dispatchCacheKey, ok := key.(keys.DispatchCacheKey)
			if !ok {
				return z.KeyToHash(key)
			}
			return dispatchCacheKey.AsUInt64s()
		},
	})
	if err != nil {
		return nil, err
	}
	return wrapped{config, cache}, nil
}

type wrapped struct {
	config *Config
	*ristretto.Cache
}

var _ Cache = (*wrapped)(nil)

func (w wrapped) GetMetrics() Metrics                   { return w.Cache.Metrics }
func (w wrapped) MarshalZerologObject(e *zerolog.Event) { e.EmbedObject(w.config) }
