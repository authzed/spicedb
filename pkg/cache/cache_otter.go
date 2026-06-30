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

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

const (
	promNamespace = "spicedb"
	promSubsystem = "cache"
)

type valueAndCost[V any] struct {
	value V
	cost  uint32
}

// entryWeight computes the weight Otter should account for an entry: the
// caller-supplied payload cost plus the key bytes. Otter's MaximumWeight bounds
// only the sum of Weigher outputs (the node weight is exactly the weigher's
// return value), and the key is retained on the node for the entry's lifetime
// without being included in that weight. Callers supply a cost covering only the
// payload, so folding in the key length stops the cache from systematically
// undercounting real memory and overfilling its configured MaximumWeight. It
// saturates at math.MaxUint32 rather than overflowing.
//
// Fixed per-entry structural overhead (the node struct, hash-table slot, and
// frequency-sketch state) is deliberately not added here: it is internal to
// Otter, varies by version, and is instead absorbed by the headroom ratio
// applied in pkgruntime.AvailableMemory.
func entryWeight(key string, payloadCost uint32) uint32 {
	weight := uint64(payloadCost) + uint64(len(key))
	if weight > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(weight)
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
	if registerer == nil {
		return spiceerrors.MustBugf("error attempting to register metrics in cache: nil Prometheus registerer")
	}
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

	keyStr := key.KeyString()
	// Account for the key bytes and fixed per-entry overhead in addition to the
	// caller-supplied payload cost so the cache's weight tracks real memory usage
	// rather than systematically undercounting it. The weigher returns this
	// stored cost, and the costAdded metric uses the same figure, keeping the
	// weight bound and the exported metrics consistent.
	weight := entryWeight(keyStr, uintCost)

	wtc.metrics.costAdded.Add(uint64(weight))
	if _, ok := wtc.cache.GetIfPresent(keyStr); ok {
		wtc.cache.Invalidate(keyStr)
	}
	wtc.cache.Set(keyStr, valueAndCost[V]{value, weight})
	if wtc.ttl > 0 {
		wtc.cache.SetExpiresAfter(keyStr, wtc.ttl)
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
