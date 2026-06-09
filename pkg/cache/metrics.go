package cache

import (
	"sync"

	"github.com/jzelinskie/stringz"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	promNamespace = "spicedb"
	promSubsystem = "cache"
)

var (
	descCacheHitsTotal = prometheus.NewDesc(
		stringz.Join("_", promNamespace, promSubsystem, "hits_total"),
		"Number of cache hits",
		[]string{"cache"},
		nil,
	)

	descCacheMissesTotal = prometheus.NewDesc(
		stringz.Join("_", promNamespace, promSubsystem, "misses_total"),
		"Number of cache misses",
		[]string{"cache"},
		nil,
	)

	descCostAddedBytes = prometheus.NewDesc(
		stringz.Join("_", promNamespace, promSubsystem, "cost_added_bytes"),
		"Cost of entries added to the cache",
		[]string{"cache"},
		nil,
	)

	descCostEvictedBytes = prometheus.NewDesc(
		stringz.Join("_", promNamespace, promSubsystem, "cost_evicted_bytes"),
		"Cost of entries evicted from the cache",
		[]string{"cache"},
		nil,
	)
)

// Collector is a prometheus.Collector that exports the metrics of every cache
// tracked in the map it is given. The map is owned by the caller (e.g. a
// SpiceDB server, which tracks its own caches) rather than by global package
// state. Construct one with NewCollector and register it with a
// prometheus.Registerer. The map's values must implement the Cache interface
// (specifically GetMetrics) and be keyed by the cache name.
type Collector struct {
	caches *sync.Map
}

var _ prometheus.Collector = (*Collector)(nil)

// NewCollector returns a Collector that exports metrics for the caches tracked
// in the given map.
func NewCollector(caches *sync.Map) *Collector {
	return &Collector{caches: caches}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, ch)
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.caches.Range(func(name, cache any) bool {
		cacheName := name.(string)
		metrics := cache.(withMetrics).GetMetrics()
		ch <- prometheus.MustNewConstMetric(descCacheHitsTotal, prometheus.CounterValue, float64(metrics.Hits()), cacheName)
		ch <- prometheus.MustNewConstMetric(descCacheMissesTotal, prometheus.CounterValue, float64(metrics.Misses()), cacheName)
		ch <- prometheus.MustNewConstMetric(descCostAddedBytes, prometheus.CounterValue, float64(metrics.CostAdded()), cacheName)
		ch <- prometheus.MustNewConstMetric(descCostEvictedBytes, prometheus.CounterValue, float64(metrics.CostEvicted()), cacheName)
		return true
	})
}

type withMetrics interface {
	GetMetrics() Metrics
}
