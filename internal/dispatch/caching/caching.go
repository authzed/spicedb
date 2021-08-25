package caching

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/dgraph-io/ristretto"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/authzed/spicedb/internal/dispatch"
	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const errCachingInitialization = "error initializing caching dispatcher: %w"

type cachingDispatcher struct {
	d dispatch.Dispatcher
	c *ristretto.Cache

	totalCounter     prometheus.Counter
	fromCacheCounter prometheus.Counter
}

type checkResultEntry struct {
	result                     *v1.DispatchCheckResponse
	computedWithDepthRemaining uint32
}

var checkResultEntryCost = int64(unsafe.Sizeof(checkResultEntry{}))

// NewCachingDispatcher creates a new dispatch.Dispatcher which delegates dispatch requests
// and caches the responses when possible and desirable.
func NewCachingDispatcher(
	delegate dispatch.Dispatcher,
	cacheConfig *ristretto.Config,
	prometheusSubsystem string,
) (dispatch.Dispatcher, error) {
	if cacheConfig == nil {
		cacheConfig = &ristretto.Config{
			NumCounters: 1e4,     // number of keys to track frequency of (10k).
			MaxCost:     1 << 24, // maximum cost of cache (16MB).
			BufferItems: 64,      // number of keys per Get buffer.
			Metrics:     true,    // collect metrics.
		}
	}

	cache, err := ristretto.NewCache(cacheConfig)
	if err != nil {
		return nil, fmt.Errorf(errCachingInitialization, err)
	}

	totalCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: prometheusSubsystem,
		Name:      "check_total",
	})
	fromCacheCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: prometheusSubsystem,
		Name:      "check_from_cache_total",
	})

	if prometheusSubsystem != "" {
		err = prometheus.Register(totalCounter)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}

		err = prometheus.Register(fromCacheCounter)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}

		// Export some ristretto metrics
		err = registerMetricsFunc("cache_hits_total", prometheusSubsystem, cache.Metrics.Hits)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}

		err = registerMetricsFunc("cache_misses_total", prometheusSubsystem, cache.Metrics.Misses)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}

		err = registerMetricsFunc("cost_added_bytes", prometheusSubsystem, cache.Metrics.CostAdded)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}

		err = registerMetricsFunc("cost_evicted_bytes", prometheusSubsystem, cache.Metrics.CostEvicted)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}
	}

	return &cachingDispatcher{delegate, cache, totalCounter, fromCacheCounter}, nil
}

func registerMetricsFunc(name string, subsystem string, metricsFunc func() uint64) error {
	return prometheus.Register(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: subsystem,
		Name:      name,
	}, func() float64 {
		return float64(metricsFunc())
	}))
}

// DispatchCheck implements dispatch.Check interface
func (cd *cachingDispatcher) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	cd.totalCounter.Inc()
	requestKey := requestToKey(req)

	if cachedResultRaw, found := cd.c.Get(requestKey); found {
		cachedResult := cachedResultRaw.(checkResultEntry)
		if req.Metadata.DepthRemaining >= cachedResult.computedWithDepthRemaining {
			cd.fromCacheCounter.Inc()
			return cachedResult.result, nil
		}
	}

	computed, err := cd.d.DispatchCheck(ctx, req)
	if err == nil {
		toCache := checkResultEntry{computed, req.Metadata.DepthRemaining}
		toCache.result.Metadata.DispatchCount = 0
		cd.c.Set(requestKey, toCache, checkResultEntryCost)
	}

	return computed, err
}

// DispatchExpand implements dispatch.Expand interface
func (cd *cachingDispatcher) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	return cd.d.DispatchExpand(ctx, req)
}

// DispatchLookup implements dispatch.Lookup interface
func (cd *cachingDispatcher) DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest) (*v1.DispatchLookupResponse, error) {
	return cd.d.DispatchLookup(ctx, req)
}

func requestToKey(req *v1.DispatchCheckRequest) string {
	return fmt.Sprintf("%s@%s@%s", tuple.StringONR(req.ObjectAndRelation), tuple.StringONR(req.Subject), req.Metadata.AtRevision)
}
