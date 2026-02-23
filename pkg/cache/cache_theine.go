//go:build !wasm

package cache

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Yiling-J/theine-go"
	"github.com/ccoveille/go-safecast/v2"
	"github.com/rs/zerolog"
)

func NewTheineCacheWithMetrics[K KeyString, V any](name string, config *Config) (Cache[K, V], error) {
	builder := theine.NewBuilder[K, V](config.MaxCost)
	built, err := builder.Build()
	if err != nil {
		return nil, err
	}
	cc := theineCache[K, V]{name, built, config.DefaultTTL, sync.Once{}, theineMetrics[K, V]{atomic.Uint64{}, built}}
	mustRegisterCache(name, &cc)
	return &cc, nil
}

type theineCache[K KeyString, V any] struct {
	name       string
	cache      *theine.Cache[K, V]
	defaultTTL time.Duration
	closed     sync.Once
	metrics    theineMetrics[K, V]
}

func (wtc *theineCache[K, V]) GetTTL() time.Duration {
	return wtc.defaultTTL
}

func (wtc *theineCache[K, V]) Get(key K) (V, bool) {
	return wtc.cache.Get(key)
}

func (wtc *theineCache[K, V]) Set(key K, value V, cost int64) bool {
	uintCost, err := safecast.Convert[uint64](cost)
	if err != nil {
		// We make an assumption that if the cast fails, it's because the value
		// was too big, so we set to maxint in that case.
		uintCost = math.MaxUint32
	}
	wtc.metrics.costAdded.Add(uintCost)
	if wtc.defaultTTL <= 0 {
		return wtc.cache.Set(key, value, cost)
	}
	return wtc.cache.SetWithTTL(key, value, cost, wtc.defaultTTL)
}

func (wtc *theineCache[K, V]) Wait() {
	// No-op because theine doesn't have a wait function.
}

func (wtc *theineCache[K, V]) Close() {
	wtc.closed.Do(func() {
		wtc.cache.Close()
	})
	unregisterCache(wtc.name)
}

func (wtc *theineCache[K, V]) GetMetrics() Metrics { return &wtc.metrics }
func (wtc *theineCache[K, V]) MarshalZerologObject(e *zerolog.Event) {
	e.Bool("theine", true)
}

type theineMetrics[K KeyString, V any] struct {
	costAdded atomic.Uint64
	cache     *theine.Cache[K, V]
}

func (tm *theineMetrics[K, V]) CostAdded() uint64   { return tm.costAdded.Load() }
func (tm *theineMetrics[K, V]) CostEvicted() uint64 { return 0 }
func (tm *theineMetrics[K, V]) Hits() uint64        { return tm.cache.Stats().Hits() }
func (tm *theineMetrics[K, V]) Misses() uint64      { return tm.cache.Stats().Misses() }
