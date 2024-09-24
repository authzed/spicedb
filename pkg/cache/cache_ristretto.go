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
				stringValue, ok := key.(StringKey)
				if ok {
					return z.KeyToHash(string(stringValue))
				}

				return z.KeyToHash(key)
			}
			return dispatchCacheKey.AsUInt64s()
		},
	}
}

// NewRistrettoCacheWithMetrics creates a new ristretto cache from the given config
// that also reports metrics to the default Prometheus registry.
func NewRistrettoCacheWithMetrics[K KeyString, V any](name string, config *Config) (Cache[K, V], error) {
	cfg := ristrettoConfig(config)
	cfg.Metrics = true

	rcache, err := ristretto.NewCache(cfg)
	if err != nil {
		return nil, err
	}

	cache := wrapped[K, V]{name, config, config.DefaultTTL, rcache}
	mustRegisterCache(name, cache)
	return &cache, nil
}

// NewRistrettoCache creates a new ristretto cache from the given config.
func NewRistrettoCache[K KeyString, V any](config *Config) (Cache[K, V], error) {
	rcache, err := ristretto.NewCache(ristrettoConfig(config))
	return &wrapped[K, V]{"", config, config.DefaultTTL, rcache}, err
}

type wrapped[K any, V any] struct {
	name       string
	config     *Config
	defaultTTL time.Duration
	ristretto  *ristretto.Cache
}

func (w wrapped[K, V]) Set(key K, entry V, cost int64) bool {
	if w.defaultTTL <= 0 {
		return w.ristretto.Set(key, entry, cost)
	}
	return w.ristretto.SetWithTTL(key, entry, cost, w.defaultTTL)
}

func (w wrapped[K, V]) Get(key K) (V, bool) {
	found, ok := w.ristretto.Get(key)
	if !ok {
		return *new(V), false
	}

	return found.(V), true
}

func (w wrapped[K, V]) Wait() {
	w.ristretto.Wait()
}

var _ Cache[StringKey, any] = (*wrapped[StringKey, any])(nil)

func (w wrapped[K, V]) GetMetrics() Metrics                   { return w.ristretto.Metrics }
func (w wrapped[K, V]) MarshalZerologObject(e *zerolog.Event) { e.EmbedObject(w.config) }

func (w wrapped[K, V]) Close() {
	w.ristretto.Close()
	unregisterCache(w.name)
}
