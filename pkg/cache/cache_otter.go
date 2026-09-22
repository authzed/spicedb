//go:build !wasm

package cache

import (
	"fmt"
	"math"
	"sync/atomic"

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

// entryStructuralOverhead is the fixed per-entry cost of holding an entry in an
// Otter cache, beyond the key bytes and the caller-supplied payload cost: the
// node struct, the hash-table slot, the frequency-sketch state, the key string's
// backing allocation and the allocation that boxes the value into an interface.
//
// Measured against otter v2.3.0 by replaying entries into a real cache and
// reading back the heap delta (see TestEntryStructuralOverhead), per entry:
//
//	V=[]byte,      no TTL: 109 B
//	V=[]byte,      TTL set: 141 B
//	V=any([]byte), no TTL: 133 B
//	V=any([]byte), TTL set: 149 B  <- the dispatch caches
//
// One constant is used for every cache rather than one per shape. The shapes
// span only ~40 B, the largest of them is the dispatch caches that hold
// essentially all the cached bytes in a real deployment, and whether V is an
// interface is not something this function can see. Charging the other shapes
// the largest figure over-counts by at most ~40 B per entry, which costs a
// little capacity; under-counting is the failure this constant exists to fix.
const entryStructuralOverhead = 152

// entryWeight computes the weight Otter should account for an entry: the
// caller-supplied payload cost, plus the key bytes, plus the fixed per-entry
// structural overhead. Otter's MaximumWeight bounds only the sum of Weigher
// outputs (the node weight is exactly the weigher's return value), so anything
// left out of this figure is memory the cache holds but never counts against its
// budget and therefore never evicts for. Callers supply a cost covering only the
// payload. It saturates at math.MaxUint32 rather than overflowing.
//
// The headroom ratio in pkgruntime.AvailableMemory does not cover this: it is
// documented as covering memory outside the caller's accounting entirely
// (goroutine stacks, GC metadata, fragmentation), and with cluster dispatch
// enabled the two dispatch caches are configured to divide all of it between
// them.
func entryWeight(key string, payloadCost uint32) uint32 {
	weight := uint64(payloadCost) + uint64(len(key)) + entryStructuralOverhead
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
	}, err
}

type otterCache[K KeyString, V any] struct {
	name    string
	cache   *otter.Cache[string, valueAndCost[V]]
	metrics otterMetrics

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
	wtc.cache.Set(keyStr, valueAndCost[V]{value, weight})
	return true
}

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
