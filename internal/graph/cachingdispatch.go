package graph

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/dgraph-io/ristretto"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/authzed/spicedb/pkg/tuple"
)

const errCachingInitialization = "error initializing caching dispatcher: %w"

type cachingDispatcher struct {
	d Dispatcher
	c *ristretto.Cache
}

type checkResultEntry struct {
	result                     CheckResult
	computedWithDepthRemaining uint16
}

var checkResultEntryCost = int64(unsafe.Sizeof(checkResultEntry{}))

type registerPromMetricsIntention bool

var (
	RegisterPromMetrics registerPromMetricsIntention = true
	DisablePromMetrics  registerPromMetricsIntention = false
)

var (
	counterDispatchCheckRequest = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "dispatch",
		Name:      "check_total",
	})
	counterDispatchCheckRequestFromCache = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "dispatch",
		Name:      "check_from_cache_total",
	})
)

func NewCachingDispatcher(
	delegate Dispatcher,
	cacheConfig *ristretto.Config,
	registerPromMetrics registerPromMetricsIntention,
) (Dispatcher, error) {
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

	if registerPromMetrics {
		err := prometheus.Register(counterDispatchCheckRequest)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}

		err = prometheus.Register(counterDispatchCheckRequestFromCache)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}

		// Export some ristretto metrics
		err = registerMetricsFunc("cache_hits_total", cache.Metrics.Hits)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}

		err = registerMetricsFunc("cache_misses_total", cache.Metrics.Misses)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}

		err = registerMetricsFunc("cost_added_bytes", cache.Metrics.CostAdded)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}

		err = registerMetricsFunc("cost_evicted_bytes", cache.Metrics.CostEvicted)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}
	}

	return &cachingDispatcher{d: delegate, c: cache}, nil
}

func registerMetricsFunc(name string, metricsFunc func() uint64) error {
	return prometheus.Register(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "dispatch",
		Name:      name,
	}, func() float64 {
		return float64(metricsFunc())
	}))
}

func (cd *cachingDispatcher) Check(ctx context.Context, req CheckRequest) CheckResult {
	counterDispatchCheckRequest.Inc()
	requestKey := requestToKey(req)

	if cachedResultRaw, found := cd.c.Get(requestKey); found {
		cachedResult := cachedResultRaw.(checkResultEntry)
		if req.DepthRemaining >= cachedResult.computedWithDepthRemaining {
			counterDispatchCheckRequestFromCache.Inc()
			return cachedResult.result
		}
	}

	computed := cd.d.Check(ctx, req)
	if computed.Err == nil {
		toCache := checkResultEntry{computed, req.DepthRemaining}
		cd.c.Set(requestKey, toCache, checkResultEntryCost)
	}

	return computed
}

func (cd *cachingDispatcher) Expand(ctx context.Context, req ExpandRequest) ExpandResult {
	return cd.d.Expand(ctx, req)
}

func (cd *cachingDispatcher) Lookup(ctx context.Context, req LookupRequest) LookupResult {
	return cd.d.Lookup(ctx, req)
}

func requestToKey(req CheckRequest) string {
	return fmt.Sprintf("%s@%s@%d", tuple.StringONR(req.Start), tuple.StringONR(req.Goal), req.AtRevision.IntPart())
}
