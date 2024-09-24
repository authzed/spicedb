//go:build !wasm
// +build !wasm

package cache

import (
	"math"
	"sync"

	"github.com/ccoveille/go-safecast"
	"github.com/maypok86/otter"
	"github.com/rs/zerolog"
)

func NewOtterCacheWithMetrics[K KeyString, V any](name string, config *Config) (Cache[K, V], error) {
	// TODO: support metrics
	return NewOtterCache[K, V](config)
}

type valueAndCost[V any] struct {
	value V
	cost  uint32
}

func NewOtterCache[K KeyString, V any](config *Config) (Cache[K, V], error) {
	if config.DefaultTTL <= 0 {
		cache, err := otter.MustBuilder[string, valueAndCost[V]](int(config.MaxCost)).
			CollectStats().
			Cost(func(key string, value valueAndCost[V]) uint32 {
				return value.cost
			}).
			Build()
		if err != nil {
			return nil, err
		}

		return &otterCache[K, V]{cache, sync.Once{}}, nil
	}

	cache, err := otter.MustBuilder[string, valueAndCost[V]](int(config.MaxCost)).
		CollectStats().
		Cost(func(key string, value valueAndCost[V]) uint32 {
			return value.cost
		}).
		WithTTL(config.DefaultTTL).
		Build()
	if err != nil {
		return nil, err
	}

	return &otterCache[K, V]{cache, sync.Once{}}, nil
}

type otterCache[K KeyString, V any] struct {
	cache  otter.Cache[string, valueAndCost[V]]
	closed sync.Once
}

func (wtc *otterCache[K, V]) Get(key K) (V, bool) {
	keyString := key.KeyString()
	vac, ok := wtc.cache.Get(keyString)
	if !ok {
		return *new(V), false
	}

	return vac.value, true
}

func (wtc *otterCache[K, V]) Set(key K, value V, cost int64) bool {
	keyString := key.KeyString()
	uintCost, err := safecast.ToUint32(cost)
	if err != nil {
		// We make an assumption that if the cast fails, it's because the value
		// was too big, so we set to maxint in that case.
		uintCost = math.MaxUint32
	}
	return wtc.cache.Set(keyString, valueAndCost[V]{value, uintCost})
}

func (wtc *otterCache[K, V]) Wait() {
	// No-op because otter doesn't have a wait function.
}

func (wtc *otterCache[K, V]) Close() {
	wtc.closed.Do(func() {
		wtc.cache.Close()
	})
}

func (wtc *otterCache[K, V]) GetMetrics() Metrics { return &noopMetrics{} }
func (wtc *otterCache[K, V]) MarshalZerologObject(e *zerolog.Event) {
	e.Bool("otter", true)
}
