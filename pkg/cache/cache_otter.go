//go:build !wasm

package cache

import (
	"math"
	"sync/atomic"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/maypok86/otter/v2"
	"github.com/maypok86/otter/v2/stats"
	"github.com/rs/zerolog"
)

func NewOtterCacheWithMetrics[K KeyString, V any](name string, config *Config) (Cache[K, V], error) {
	return NewOtterCache[K, V](config)
}

type valueAndCost[V any] struct {
	value V
	cost  uint32
}

func NewOtterCache[K KeyString, V any](config *Config) (Cache[K, V], error) {
	uintCost, err := safecast.Convert[uint64](config.MaxCost)
	if err != nil {
		return nil, err
	}

	counter := stats.NewCounter()
	opts := &otter.Options[string, valueAndCost[V]]{
		MaximumWeight: uintCost,
		Weigher: func(key string, value valueAndCost[V]) uint32 {
			return value.cost
		},
		StatsRecorder: counter,
	}
	if config.DefaultTTL > 0 {
		opts.ExpiryCalculator = otter.ExpiryAccessing[string, valueAndCost[V]](config.DefaultTTL)
	}

	cache, err := otter.New(opts)
	return &otterCache[K, V]{
		cache,
		otterMetrics{atomic.Uint64{}, counter},
	}, err
}

type otterCache[K KeyString, V any] struct {
	cache   *otter.Cache[string, valueAndCost[V]]
	metrics otterMetrics
}

func (wtc *otterCache[K, V]) Get(key K) (V, bool) {
	vac, ok := wtc.cache.GetIfPresent(key.KeyString())
	if !ok {
		return *new(V), false
	}

	return vac.value, true
}

func (wtc *otterCache[K, V]) Set(key K, value V, cost int64) bool {
	uintCost, err := safecast.Convert[uint32](cost)
	if err != nil {
		// We make an assumption that if the cast fails, it's because the value
		// was too big, so we set to maxint in that case.
		uintCost = math.MaxUint32
	}
	wtc.metrics.costAdded.Add(uint64(uintCost))
	wtc.cache.Set(key.KeyString(), valueAndCost[V]{value, uintCost})
	return true // Otter doesn't drop insertions for performance
}

func (wtc *otterCache[K, V]) Wait()  {}
func (wtc *otterCache[K, V]) Close() {}

type otterMetrics struct {
	costAdded atomic.Uint64
	*stats.Counter
}

func (o *otterMetrics) CostAdded() uint64   { return o.costAdded.Load() }
func (o *otterMetrics) CostEvicted() uint64 { return o.Counter.Snapshot().EvictionWeight }
func (o *otterMetrics) Hits() uint64        { return o.Counter.Snapshot().Hits }
func (o *otterMetrics) Misses() uint64      { return o.Counter.Snapshot().Misses }

func (wtc *otterCache[K, V]) GetMetrics() Metrics { return &wtc.metrics }
func (wtc *otterCache[K, V]) MarshalZerologObject(e *zerolog.Event) {
	e.Bool("otter", true)
}
