//go:build !wasm

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

	cache := ristretoCache[K, V]{name, config, config.DefaultTTL, rcache}
	mustRegisterCache(name, cache)
	return &cache, nil
}

// NewRistrettoCache creates a new ristretto cache from the given config.
func NewRistrettoCache[K KeyString, V any](config *Config) (Cache[K, V], error) {
	rcache, err := ristretto.NewCache(ristrettoConfig(config))
	return &ristretoCache[K, V]{"", config, config.DefaultTTL, rcache}, err
}

type ristretoCache[K any, V any] struct {
	name       string
	config     *Config
	defaultTTL time.Duration
	ristretto  *ristretto.Cache
}

func (w ristretoCache[K, V]) Set(key K, entry V, cost int64) bool {
	if w.defaultTTL <= 0 {
		return w.ristretto.Set(key, entry, cost)
	}
	return w.ristretto.SetWithTTL(key, entry, cost, w.defaultTTL)
}

func (w ristretoCache[K, V]) Get(key K) (V, bool) {
	found, ok := w.ristretto.Get(key)
	if !ok {
		return *new(V), false
	}

	return found.(V), true
}

func (w ristretoCache[K, V]) Wait() {
	w.ristretto.Wait()
}

func (w ristretoCache[K, V]) GetTTL() time.Duration {
	return w.defaultTTL
}

var _ Cache[StringKey, any] = (*ristretoCache[StringKey, any])(nil)

func (w ristretoCache[K, V]) GetMetrics() Metrics                   { return w.ristretto.Metrics }
func (w ristretoCache[K, V]) MarshalZerologObject(e *zerolog.Event) { e.EmbedObject(w.config) }

func (w ristretoCache[K, V]) Close() {
	w.ristretto.Close()
	unregisterCache(w.name)
}
