//go:build !wasm
// +build !wasm

package cache

import (
	"sync"
	"time"

	"github.com/Yiling-J/theine-go"
	"github.com/rs/zerolog"
)

func NewTheineCacheWithMetrics[K KeyString, V any](name string, config *Config) (Cache[K, V], error) {
	// TODO: figure out metrics
	return NewTheineCache[K, V](config)
}

func NewTheineCache[K KeyString, V any](config *Config) (Cache[K, V], error) {
	builder := theine.NewBuilder[K, V](config.MaxCost)
	builder.StringKey(func(key K) string {
		return key.KeyString()
	})
	built, err := builder.Build()
	if err != nil {
		return nil, err
	}
	return &theineCache[K, V]{built, config.DefaultTTL, sync.Once{}}, nil
}

type theineCache[K KeyString, V any] struct {
	cache      *theine.Cache[K, V]
	defaultTTL time.Duration
	closed     sync.Once
}

func (wtc *theineCache[K, V]) Get(key K) (V, bool) {
	return wtc.cache.Get(key)
}

func (wtc *theineCache[K, V]) Set(key K, value V, cost int64) bool {
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
}

func (wtc *theineCache[K, V]) GetMetrics() Metrics { return &noopMetrics{} }
func (wtc *theineCache[K, V]) MarshalZerologObject(e *zerolog.Event) {
	e.Bool("theine", true)
}
