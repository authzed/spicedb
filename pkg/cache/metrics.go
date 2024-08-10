package cache

import (
	"sync"

	"github.com/jzelinskie/stringz"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	prometheus.MustRegister(defaultCollector)
}

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

var caches sync.Map

func mustRegisterCache(name string, c withMetrics) {
	if _, loaded := caches.LoadOrStore(name, c); loaded {
		panic("two caches with the same name")
	}
}

func unregisterCache(name string) {
	caches.Delete(name)
}

var (
	defaultCollector collector

	_ prometheus.Collector = (*collector)(nil)
)

type collector struct{}

func (c collector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, ch)
}

func (c collector) Collect(ch chan<- prometheus.Metric) {
	caches.Range(func(name, cache any) bool {
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
