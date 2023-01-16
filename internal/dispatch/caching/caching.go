package caching

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"unsafe"

	"golang.org/x/exp/maps"

	"github.com/dustin/go-humanize"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cache"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

const (
	errCachingInitialization = "error initializing caching dispatcher: %w"

	prometheusNamespace = "spicedb"
)

// Dispatcher is a dispatcher with cacheInst-in caching.
type Dispatcher struct {
	d          dispatch.Dispatcher
	c          cache.Cache
	keyHandler keys.Handler

	checkTotalCounter                  prometheus.Counter
	checkFromCacheCounter              prometheus.Counter
	lookupTotalCounter                 prometheus.Counter
	lookupFromCacheCounter             prometheus.Counter
	reachableResourcesTotalCounter     prometheus.Counter
	reachableResourcesFromCacheCounter prometheus.Counter
	lookupSubjectsTotalCounter         prometheus.Counter
	lookupSubjectsFromCacheCounter     prometheus.Counter

	cacheHits        prometheus.CounterFunc
	cacheMisses      prometheus.CounterFunc
	costAddedBytes   prometheus.CounterFunc
	costEvictedBytes prometheus.CounterFunc
}

func DispatchTestCache(t testing.TB) cache.Cache {
	cache, err := cache.NewCache(&cache.Config{
		NumCounters: 1000,
		MaxCost:     1 * humanize.MiByte,
		Metrics:     true,
	})
	require.Nil(t, err)
	return cache
}

// NewCachingDispatcher creates a new dispatch.Dispatcher which delegates
// dispatch requests and caches the responses when possible and desirable.
func NewCachingDispatcher(cacheInst cache.Cache, prometheusSubsystem string, keyHandler keys.Handler) (*Dispatcher, error) {
	if cacheInst == nil {
		cacheInst = cache.NoopCache()
	}

	checkTotalCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: prometheusNamespace,
		Subsystem: prometheusSubsystem,
		Name:      "check_total",
	})
	checkFromCacheCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: prometheusNamespace,
		Subsystem: prometheusSubsystem,
		Name:      "check_from_cache_total",
	})

	lookupTotalCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: prometheusNamespace,
		Subsystem: prometheusSubsystem,
		Name:      "lookup_total",
	})
	lookupFromCacheCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: prometheusNamespace,
		Subsystem: prometheusSubsystem,
		Name:      "lookup_from_cache_total",
	})

	reachableResourcesTotalCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: prometheusNamespace,
		Subsystem: prometheusSubsystem,
		Name:      "reachable_resources_total",
	})
	reachableResourcesFromCacheCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: prometheusNamespace,
		Subsystem: prometheusSubsystem,
		Name:      "reachable_resources_from_cache_total",
	})

	lookupSubjectsTotalCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: prometheusNamespace,
		Subsystem: prometheusSubsystem,
		Name:      "lookup_subjects_total",
	})
	lookupSubjectsFromCacheCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: prometheusNamespace,
		Subsystem: prometheusSubsystem,
		Name:      "lookup_subjects_from_cache_total",
	})

	cacheHitsTotal := prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: prometheusNamespace,
		Subsystem: prometheusSubsystem,
		Name:      "cache_hits_total",
	}, func() float64 {
		return float64(cacheInst.GetMetrics().Hits())
	})
	cacheMissesTotal := prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: prometheusNamespace,
		Subsystem: prometheusSubsystem,
		Name:      "cache_misses_total",
	}, func() float64 {
		return float64(cacheInst.GetMetrics().Misses())
	})

	costAddedBytes := prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: prometheusNamespace,
		Subsystem: prometheusSubsystem,
		Name:      "cost_added_bytes",
	}, func() float64 {
		return float64(cacheInst.GetMetrics().CostAdded())
	})

	costEvictedBytes := prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: prometheusNamespace,
		Subsystem: prometheusSubsystem,
		Name:      "cost_evicted_bytes",
	}, func() float64 {
		return float64(cacheInst.GetMetrics().CostEvicted())
	})

	if prometheusSubsystem != "" {
		err := prometheus.Register(checkTotalCounter)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}
		err = prometheus.Register(checkFromCacheCounter)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}
		err = prometheus.Register(lookupTotalCounter)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}
		err = prometheus.Register(lookupFromCacheCounter)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}
		err = prometheus.Register(reachableResourcesTotalCounter)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}
		err = prometheus.Register(reachableResourcesFromCacheCounter)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}
		err = prometheus.Register(lookupSubjectsTotalCounter)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}
		err = prometheus.Register(lookupSubjectsFromCacheCounter)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}

		// Export some ristretto metrics
		err = prometheus.Register(cacheHitsTotal)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}
		err = prometheus.Register(cacheMissesTotal)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}
		err = prometheus.Register(costAddedBytes)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}
		err = prometheus.Register(costEvictedBytes)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}
	}

	if keyHandler == nil {
		keyHandler = &keys.DirectKeyHandler{}
	}

	return &Dispatcher{
		d:                                  fakeDelegate{},
		c:                                  cacheInst,
		keyHandler:                         keyHandler,
		checkTotalCounter:                  checkTotalCounter,
		checkFromCacheCounter:              checkFromCacheCounter,
		lookupTotalCounter:                 lookupTotalCounter,
		lookupFromCacheCounter:             lookupFromCacheCounter,
		reachableResourcesTotalCounter:     reachableResourcesTotalCounter,
		reachableResourcesFromCacheCounter: reachableResourcesFromCacheCounter,
		lookupSubjectsTotalCounter:         lookupSubjectsTotalCounter,
		lookupSubjectsFromCacheCounter:     lookupSubjectsFromCacheCounter,
		cacheHits:                          cacheHitsTotal,
		cacheMisses:                        cacheMissesTotal,
		costAddedBytes:                     costAddedBytes,
		costEvictedBytes:                   costEvictedBytes,
	}, nil
}

// SetDelegate sets the internal delegate to the specific dispatcher instance.
func (cd *Dispatcher) SetDelegate(delegate dispatch.Dispatcher) {
	cd.d = delegate
}

// DispatchCheck implements dispatch.Check interface
func (cd *Dispatcher) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	cd.checkTotalCounter.Inc()

	requestKey, err := cd.keyHandler.CheckCacheKey(ctx, req)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{}}, err
	}

	// Disable caching when debugging is enabled.
	if cachedResultRaw, found := cd.c.Get(requestKey); found {
		var response v1.DispatchCheckResponse
		if err := response.UnmarshalVT(cachedResultRaw.([]byte)); err != nil {
			return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{}}, err
		}

		if req.Metadata.DepthRemaining >= response.Metadata.DepthRequired {
			cd.checkFromCacheCounter.Inc()
			// If debugging is requested, add the req and the response to the trace.
			if req.Debug == v1.DispatchCheckRequest_ENABLE_BASIC_DEBUGGING {
				response.Metadata.DebugInfo = &v1.DebugInformation{
					Check: &v1.CheckDebugTrace{
						Request:        req,
						Results:        maps.Clone(response.ResultsByResourceId),
						IsCachedResult: true,
					},
				}
			}

			return &response, nil
		}
	}
	computed, err := cd.d.DispatchCheck(ctx, req)

	// We only want to cache the result if there was no error
	if err == nil {
		adjustedComputed := computed.CloneVT()
		adjustedComputed.Metadata.CachedDispatchCount = adjustedComputed.Metadata.DispatchCount
		adjustedComputed.Metadata.DispatchCount = 0
		adjustedComputed.Metadata.DebugInfo = nil

		adjustedBytes, err := adjustedComputed.MarshalVT()
		if err != nil {
			return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{}}, err
		}

		cd.c.Set(requestKey, adjustedBytes, sliceSize(adjustedBytes))
	}

	// Return both the computed and err in ALL cases: computed contains resolved
	// metadata even if there was an error.
	return computed, err
}

// DispatchExpand implements dispatch.Expand interface and does not do any caching yet.
func (cd *Dispatcher) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	resp, err := cd.d.DispatchExpand(ctx, req)
	return resp, err
}

// DispatchLookup implements dispatch.Lookup interface and does not do any caching yet.
func (cd *Dispatcher) DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest) (*v1.DispatchLookupResponse, error) {
	cd.lookupTotalCounter.Inc()

	requestKey, err := cd.keyHandler.LookupResourcesCacheKey(ctx, req)
	if err != nil {
		return &v1.DispatchLookupResponse{Metadata: &v1.ResponseMeta{}}, err
	}

	if cachedResultRaw, found := cd.c.Get(requestKey); found {
		var response v1.DispatchLookupResponse
		if err := response.UnmarshalVT(cachedResultRaw.([]byte)); err != nil {
			return &v1.DispatchLookupResponse{Metadata: &v1.ResponseMeta{}}, err
		}

		if req.Metadata.DepthRemaining >= response.Metadata.DepthRequired {
			log.Ctx(ctx).Trace().Object("cachedLookup", req).Int("resultCount", len(response.ResolvedResources)).Send()
			cd.lookupFromCacheCounter.Inc()
			return &response, nil
		}
	}
	computed, err := cd.d.DispatchLookup(ctx, req)

	// We only want to cache the result if there was no error.
	if err == nil {
		log.Ctx(ctx).Trace().Object("cachingLookup", req).Int("resultCount", len(computed.ResolvedResources)).Send()

		adjustedComputed := computed.CloneVT()
		adjustedComputed.Metadata.CachedDispatchCount = adjustedComputed.Metadata.DispatchCount
		adjustedComputed.Metadata.DispatchCount = 0
		adjustedComputed.Metadata.DebugInfo = nil

		adjustedBytes, err := adjustedComputed.MarshalVT()
		if err != nil {
			return &v1.DispatchLookupResponse{Metadata: &v1.ResponseMeta{}}, err
		}

		cd.c.Set(requestKey, adjustedBytes, sliceSize(adjustedBytes))
	}

	// Return both the computed and err in ALL cases: computed contains resolved
	// metadata even if there was an error.
	return computed, err
}

// DispatchReachableResources implements dispatch.ReachableResources interface.
func (cd *Dispatcher) DispatchReachableResources(req *v1.DispatchReachableResourcesRequest, stream dispatch.ReachableResourcesStream) error {
	cd.reachableResourcesTotalCounter.Inc()

	requestKey, err := cd.keyHandler.ReachableResourcesCacheKey(stream.Context(), req)
	if err != nil {
		return err
	}

	if cachedResultRaw, found := cd.c.Get(requestKey); found {
		cd.reachableResourcesFromCacheCounter.Inc()
		for _, slice := range cachedResultRaw.([][]byte) {
			var response v1.DispatchReachableResourcesResponse
			if err := response.UnmarshalVT(slice); err != nil {
				return fmt.Errorf("could not publish cached reachable resources result: %w", err)
			}
			if err := stream.Publish(&response); err != nil {
				return fmt.Errorf("could not publish cached reachable resources result: %w", err)
			}
		}

		return nil
	}

	var (
		mu             sync.Mutex
		toCacheResults [][]byte
	)
	wrapped := &dispatch.WrappedDispatchStream[*v1.DispatchReachableResourcesResponse]{
		Stream: stream,
		Ctx:    stream.Context(),
		Processor: func(result *v1.DispatchReachableResourcesResponse) (*v1.DispatchReachableResourcesResponse, bool, error) {
			adjustedResult := result.CloneVT()
			adjustedResult.Metadata.CachedDispatchCount = adjustedResult.Metadata.DispatchCount
			adjustedResult.Metadata.DispatchCount = 0
			adjustedResult.Metadata.DebugInfo = nil

			adjustedBytes, err := adjustedResult.MarshalVT()
			if err != nil {
				return nil, false, err
			}

			mu.Lock()
			toCacheResults = append(toCacheResults, adjustedBytes)
			mu.Unlock()

			return result, true, nil
		},
	}

	if err := cd.d.DispatchReachableResources(req, wrapped); err != nil {
		return err
	}

	var size int64
	for _, slice := range toCacheResults {
		size += sliceSize(slice)
	}

	cd.c.Set(requestKey, toCacheResults, size)
	return nil
}

func sliceSize(xs []byte) int64 {
	// Slice Header + Slice Contents
	return int64(int(unsafe.Sizeof(xs)) + len(xs))
}

// DispatchLookupSubjects implements dispatch.LookupSubjects interface.
func (cd *Dispatcher) DispatchLookupSubjects(req *v1.DispatchLookupSubjectsRequest, stream dispatch.LookupSubjectsStream) error {
	cd.lookupSubjectsTotalCounter.Inc()

	requestKey, err := cd.keyHandler.LookupSubjectsCacheKey(stream.Context(), req)
	if err != nil {
		return err
	}

	if cachedResultRaw, found := cd.c.Get(requestKey); found {
		cd.lookupSubjectsFromCacheCounter.Inc()
		for _, slice := range cachedResultRaw.([][]byte) {
			var response v1.DispatchLookupSubjectsResponse
			if err := response.UnmarshalVT(slice); err != nil {
				return err
			}
			if err := stream.Publish(&response); err != nil {
				// don't wrap error with additional context, as it may be a grpc status.Status.
				// status.FromError() is unable to unwrap status.Status values, and as a consequence
				// the Dispatcher wouldn't properly propagate the gRPC error code
				return err
			}
		}
		return nil
	}

	var (
		mu             sync.Mutex
		toCacheResults [][]byte
	)
	wrapped := &dispatch.WrappedDispatchStream[*v1.DispatchLookupSubjectsResponse]{
		Stream: stream,
		Ctx:    stream.Context(),
		Processor: func(result *v1.DispatchLookupSubjectsResponse) (*v1.DispatchLookupSubjectsResponse, bool, error) {
			adjustedResult := result.CloneVT()
			adjustedResult.Metadata.CachedDispatchCount = adjustedResult.Metadata.DispatchCount
			adjustedResult.Metadata.DispatchCount = 0
			adjustedResult.Metadata.DebugInfo = nil

			adjustedBytes, err := adjustedResult.MarshalVT()
			if err != nil {
				return &v1.DispatchLookupSubjectsResponse{Metadata: &v1.ResponseMeta{}}, false, err
			}

			mu.Lock()
			toCacheResults = append(toCacheResults, adjustedBytes)
			mu.Unlock()

			return result, true, nil
		},
	}

	if err := cd.d.DispatchLookupSubjects(req, wrapped); err != nil {
		return err
	}

	var size int64
	for _, slice := range toCacheResults {
		size += sliceSize(slice)
	}

	cd.c.Set(requestKey, toCacheResults, size)
	return nil
}

func (cd *Dispatcher) Close() error {
	prometheus.Unregister(cd.checkTotalCounter)
	prometheus.Unregister(cd.lookupTotalCounter)
	prometheus.Unregister(cd.reachableResourcesTotalCounter)
	prometheus.Unregister(cd.lookupFromCacheCounter)
	prometheus.Unregister(cd.checkFromCacheCounter)
	prometheus.Unregister(cd.reachableResourcesFromCacheCounter)
	prometheus.Unregister(cd.lookupSubjectsFromCacheCounter)
	prometheus.Unregister(cd.lookupSubjectsTotalCounter)
	prometheus.Unregister(cd.cacheHits)
	prometheus.Unregister(cd.cacheMisses)
	prometheus.Unregister(cd.costAddedBytes)
	prometheus.Unregister(cd.costEvictedBytes)
	if cache := cd.c; cache != nil {
		cache.Close()
	}

	return nil
}

func (cd *Dispatcher) IsReady() bool {
	return cd.c != nil && cd.d.IsReady()
}

// Always verify that we implement the interfaces
var _ dispatch.Dispatcher = &Dispatcher{}
