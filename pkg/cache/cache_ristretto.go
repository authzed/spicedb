//go:build !wasm
// +build !wasm

package cache

import (
	"time"

	"github.com/outcaste-io/ristretto"
	"github.com/outcaste-io/ristretto/z"
	"github.com/rs/zerolog"

	"github.com/authzed/spicedb/internal/dispatch/keys"
)

func ristrettoConfig(config *Config) *ristretto.Config {
	return &ristretto.Config{
		NumCounters: config.NumCounters,
		MaxCost:     config.MaxCost,
		BufferItems: 64, // Recommended constant by Ristretto authors.
		KeyToHash: func(key any) (uint64, uint64) {
			dispatchCacheKey, ok := key.(keys.DispatchCacheKey)
			if !ok {
				return z.KeyToHash(key)
			}
			return dispatchCacheKey.AsUInt64s()
		},
	}
}

// NewCacheWithMetrics creates a new ristretto cache from the given config
// that also reports metrics to the default Prometheus registry.
func NewCacheWithMetrics(name string, config *Config) (Cache, error) {
	cfg := ristrettoConfig(config)
	cfg.Metrics = true

	rcache, err := ristretto.NewCache(cfg)
	if err != nil {
		return nil, err
	}

	cache := wrapped{name, config, config.DefaultTTL, rcache}
	mustRegisterCache(name, cache)
	return &cache, nil
}

// NewCache creates a new ristretto cache from the given config.
func NewCache(config *Config) (Cache, error) {
	rcache, err := ristretto.NewCache(ristrettoConfig(config))
	return &wrapped{"", config, config.DefaultTTL, rcache}, err
}

type wrapped struct {
	name       string
	config     *Config
	defaultTTL time.Duration
	*ristretto.Cache
}

func (w wrapped) Set(key, entry any, cost int64) bool {
	if w.defaultTTL <= 0 {
		return w.Cache.Set(key, entry, cost)
	}
	return w.Cache.SetWithTTL(key, entry, cost, w.defaultTTL)
}

var _ Cache = (*wrapped)(nil)

func (w wrapped) GetMetrics() Metrics                   { return w.Cache.Metrics }
func (w wrapped) MarshalZerologObject(e *zerolog.Event) { e.EmbedObject(w.config) }

func (w wrapped) Close() {
	w.Cache.Close()
	unregisterCache(w.name)
}
