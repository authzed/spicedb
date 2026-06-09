//go:build !wasm

package cache

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/maypok86/otter/v2"
	"github.com/maypok86/otter/v2/stats"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

const (
	promNamespace = "spicedb"
	promSubsystem = "cache"
)

type valueAndCost[V any] struct {
	value V
	cost  uint32
}

// NewOtterCache creates an Otter-backed cache. It tracks its own metrics (see
// Cache.GetMetrics) but does not export them; use NewOtterCacheWithMetrics to
// also register those metrics with a Prometheus registerer.
func NewOtterCache[K KeyString, V any](name string, config *Config) (Cache[K, V], error) {
	return newOtterCache[K, V](name, config)
}

// NewOtterCacheWithMetrics creates an Otter-backed cache and registers its
// metrics (labeled by name) with the given registerer. The metrics are
// unregistered when the cache is Closed.
func NewOtterCacheWithMetrics[K KeyString, V any](registerer prometheus.Registerer, name string, config *Config) (Cache[K, V], error) {
	cache, err := newOtterCache[K, V](name, config)
	if err != nil {
		return nil, err
	}
	if err := cache.registerMetrics(registerer); err != nil {
		return nil, err
	}
	return cache, nil
}

func newOtterCache[K KeyString, V any](name string, config *Config) (*otterCache[K, V], error) {
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
		name:    name,
		cache:   cache,
		metrics: otterMetrics{atomic.Uint64{}, counter},
		ttl:     config.DefaultTTL,
	}, err
}

type otterCache[K KeyString, V any] struct {
	name    string
	cache   *otter.Cache[string, valueAndCost[V]]
	metrics otterMetrics
	ttl     time.Duration

	// registerer and collectors are set when metrics are registered (via
	// NewOtterCacheWithMetrics) and used to unregister them on Close.
	registerer prometheus.Registerer
	collectors []prometheus.Collector
}

// registerMetrics registers this cache's metrics, labeled by its name, with the
// given registerer. The metrics are read from the cache at scrape time. On any
// registration failure, already-registered metrics are rolled back.
func (wtc *otterCache[K, V]) registerMetrics(registerer prometheus.Registerer) error {
	labels := prometheus.Labels{"cache": wtc.name}
	collectors := []prometheus.Collector{
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: promNamespace, Subsystem: promSubsystem, Name: "hits_total",
			Help: "Number of cache hits", ConstLabels: labels,
		}, func() float64 { return float64(wtc.metrics.Hits()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: promNamespace, Subsystem: promSubsystem, Name: "misses_total",
			Help: "Number of cache misses", ConstLabels: labels,
		}, func() float64 { return float64(wtc.metrics.Misses()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{ //nolint:promlinter // don't add _total
			Namespace: promNamespace, Subsystem: promSubsystem, Name: "cost_added_bytes",
			Help: "Cost of entries added to the cache", ConstLabels: labels,
		}, func() float64 { return float64(wtc.metrics.CostAdded()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{ //nolint:promlinter // don't add _total
			Namespace: promNamespace, Subsystem: promSubsystem, Name: "cost_evicted_bytes",
			Help: "Cost of entries evicted from the cache", ConstLabels: labels,
		}, func() float64 { return float64(wtc.metrics.CostEvicted()) }),
	}

	for i, c := range collectors {
		if err := registerer.Register(c); err != nil {
			for _, registered := range collectors[:i] {
				registerer.Unregister(registered)
			}
			return fmt.Errorf("could not register metrics for cache %q: %w", wtc.name, err)
		}
	}

	wtc.registerer = registerer
	wtc.collectors = collectors
	return nil
}

func (wtc *otterCache[K, V]) GetTTL() time.Duration {
	return wtc.ttl
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
	_, ok := wtc.Get(key)
	if ok {
		wtc.cache.Invalidate(key.KeyString())
	}
	wtc.cache.Set(key.KeyString(), valueAndCost[V]{value, uintCost})
	if wtc.ttl > 0 {
		wtc.cache.SetExpiresAfter(key.KeyString(), wtc.ttl)
	}
	return true
}

func (wtc *otterCache[K, V]) Wait() {}
func (wtc *otterCache[K, V]) Close() {
	// Stops the pending goroutine that Otter spins off
	wtc.cache.StopAllGoroutines()

	// Unregister any metrics this cache registered with the registerer.
	for _, c := range wtc.collectors {
		wtc.registerer.Unregister(c)
	}
	wtc.collectors = nil
}

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
