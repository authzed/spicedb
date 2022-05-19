package caching

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/dgraph-io/ristretto"
	"github.com/dustin/go-humanize"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

const (
	errCachingInitialization = "error initializing caching dispatcher: %w"

	prometheusNamespace = "spicedb"
)

// Dispatcher is a dispatcher with built-in caching.
type Dispatcher struct {
	v1.UnimplementedDispatchServiceServer

	d          dispatch.Dispatcher
	c          *ristretto.Cache
	keyHandler keys.Handler

	checkTotalCounter                  prometheus.Counter
	checkFromCacheCounter              prometheus.Counter
	lookupTotalCounter                 prometheus.Counter
	lookupFromCacheCounter             prometheus.Counter
	reachableResourcesTotalCounter     prometheus.Counter
	reachableResourcesFromCacheCounter prometheus.Counter

	cacheHits        prometheus.CounterFunc
	cacheMisses      prometheus.CounterFunc
	costAddedBytes   prometheus.CounterFunc
	costEvictedBytes prometheus.CounterFunc
}

type checkResultEntry struct {
	response *v1.DispatchCheckResponse
}

type lookupResultEntry struct {
	response *v1.DispatchLookupResponse
}

type reachableResourcesResultEntry struct {
	responses []*v1.DispatchReachableResourcesResponse
}

var (
	checkResultEntryCost            = int64(unsafe.Sizeof(checkResultEntry{}))
	lookupResultEntryEmptyCost      = int64(unsafe.Sizeof(lookupResultEntry{}))
	reachbleResourcesEntryEmptyCost = int64(unsafe.Sizeof(reachableResourcesResultEntry{}))
)

// NewCachingDispatcher creates a new dispatch.Dispatcher which delegates dispatch requests
// and caches the responses when possible and desirable.
func NewCachingDispatcher(
	cacheConfig *ristretto.Config,
	prometheusSubsystem string,
	keyHandler keys.Handler,
) (*Dispatcher, error) {
	if cacheConfig == nil {
		cacheConfig = &ristretto.Config{
			NumCounters: 1e4,     // number of keys to track frequency of (10k).
			MaxCost:     1 << 24, // maximum cost of cache (16MB).
			BufferItems: 64,      // number of keys per Get buffer.
			Metrics:     true,    // collect metrics.
		}
	} else {
		log.Info().Int64("numCounters", cacheConfig.NumCounters).Str("maxCost", humanize.Bytes(uint64(cacheConfig.MaxCost))).Msg("configured caching dispatcher")
	}

	cache, err := ristretto.NewCache(cacheConfig)
	if err != nil {
		return nil, fmt.Errorf(errCachingInitialization, err)
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

	cacheHitsTotal := prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: prometheusNamespace,
		Subsystem: prometheusSubsystem,
		Name:      "cache_hits_total",
	}, func() float64 {
		return float64(cache.Metrics.Hits())
	})
	cacheMissesTotal := prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: prometheusNamespace,
		Subsystem: prometheusSubsystem,
		Name:      "cache_misses_total",
	}, func() float64 {
		return float64(cache.Metrics.Misses())
	})

	costAddedBytes := prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: prometheusNamespace,
		Subsystem: prometheusSubsystem,
		Name:      "cost_added_bytes",
	}, func() float64 {
		return float64(cache.Metrics.CostAdded())
	})

	costEvictedBytes := prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: prometheusNamespace,
		Subsystem: prometheusSubsystem,
		Name:      "cost_evicted_bytes",
	}, func() float64 {
		return float64(cache.Metrics.CostEvicted())
	})

	if prometheusSubsystem != "" {
		err = prometheus.Register(checkTotalCounter)
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
		c:                                  cache,
		keyHandler:                         keyHandler,
		checkTotalCounter:                  checkTotalCounter,
		checkFromCacheCounter:              checkFromCacheCounter,
		lookupTotalCounter:                 lookupTotalCounter,
		lookupFromCacheCounter:             lookupFromCacheCounter,
		reachableResourcesTotalCounter:     reachableResourcesTotalCounter,
		reachableResourcesFromCacheCounter: reachableResourcesFromCacheCounter,
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

	requestKey, err := cd.keyHandler.ComputeCheckKey(ctx, req)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{}}, err
	}

	if cachedResultRaw, found := cd.c.Get(requestKey); found {
		cachedResult := cachedResultRaw.(checkResultEntry)
		if req.Metadata.DepthRemaining >= cachedResult.response.Metadata.DepthRequired {
			cd.checkFromCacheCounter.Inc()
			return cachedResult.response, nil
		}
	}

	computed, err := cd.d.DispatchCheck(ctx, req)

	// We only want to cache the result if there was no error
	if err == nil {
		adjustedComputed := proto.Clone(computed).(*v1.DispatchCheckResponse)
		adjustedComputed.Metadata.CachedDispatchCount = adjustedComputed.Metadata.DispatchCount
		adjustedComputed.Metadata.DispatchCount = 0

		toCache := checkResultEntry{adjustedComputed}
		cd.c.Set(requestKey, toCache, checkResultEntryCost)
	}

	// Return both the computed and err in ALL cases: computed contains resolved metadata even
	// if there was an error.
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

	requestKey := dispatch.LookupRequestToKey(req)
	if cachedResultRaw, found := cd.c.Get(requestKey); found {
		cachedResult := cachedResultRaw.(lookupResultEntry)
		if req.Metadata.DepthRemaining >= cachedResult.response.Metadata.DepthRequired {
			log.Trace().Object("cachedLookup", req).Int("resultCount", len(cachedResult.response.ResolvedOnrs)).Send()
			cd.lookupFromCacheCounter.Inc()
			return cachedResult.response, nil
		}
	}

	computed, err := cd.d.DispatchLookup(ctx, req)

	// We only want to cache the result if there was no error and nothing was excluded.
	if err == nil && len(computed.Metadata.LookupExcludedDirect) == 0 && len(computed.Metadata.LookupExcludedTtu) == 0 {
		log.Trace().Object("cachingLookup", req).Int("resultCount", len(computed.ResolvedOnrs)).Send()

		adjustedComputed := proto.Clone(computed).(*v1.DispatchLookupResponse)
		adjustedComputed.Metadata.CachedDispatchCount = adjustedComputed.Metadata.DispatchCount
		adjustedComputed.Metadata.DispatchCount = 0
		adjustedComputed.Metadata.LookupExcludedDirect = nil
		adjustedComputed.Metadata.LookupExcludedTtu = nil

		requestKey := dispatch.LookupRequestToKey(req)
		toCache := lookupResultEntry{adjustedComputed}

		estimatedSize := lookupResultEntryEmptyCost
		for _, onr := range toCache.response.ResolvedOnrs {
			estimatedSize += int64(len(onr.Namespace) + len(onr.ObjectId) + len(onr.Relation))
		}

		cd.c.Set(requestKey, toCache, estimatedSize)
	}

	// Return both the computed and err in ALL cases: computed contains resolved metadata even
	// if there was an error.
	return computed, err
}

// DispatchReachableResources implements dispatch.ReachableResources interface and does not do any caching yet.
func (cd *Dispatcher) DispatchReachableResources(req *v1.DispatchReachableResourcesRequest, stream v1.DispatchService_DispatchReachableResourcesServer) error {
	cd.reachableResourcesTotalCounter.Inc()

	requestKey := dispatch.ReachableResourcesRequestToKey(req)
	if cachedResultRaw, found := cd.c.Get(requestKey); found {
		cachedResult := cachedResultRaw.(reachableResourcesResultEntry)
		cd.reachableResourcesFromCacheCounter.Inc()
		for _, result := range cachedResult.responses {
			// Adjust the metadata to show the cached counts.
			if result.Metadata.CachedDispatchCount == 0 {
				result.Metadata.CachedDispatchCount = result.Metadata.DispatchCount
				result.Metadata.DispatchCount = 0
			}

			err := stream.Send(result)
			if err != nil {
				return err
			}
		}

		return nil
	}

	estimatedSize := reachbleResourcesEntryEmptyCost
	toCacheResults := []*v1.DispatchReachableResourcesResponse{}
	wrapped := &dispatch.WrappedDispatchStream[*v1.DispatchReachableResourcesResponse]{
		DispatchStream: stream,
		Ctx:            stream.Context(),
		Processor: func(result *v1.DispatchReachableResourcesResponse) (*v1.DispatchReachableResourcesResponse, error) {
			toCacheResults = append(toCacheResults, result)
			estimatedSize += int64(len(result.Resource.Resource.Namespace) + len(result.Resource.Resource.ObjectId) + len(result.Resource.Resource.Relation))
			return result, nil
		},
	}

	err := cd.d.DispatchReachableResources(req, wrapped)

	// We only want to cache the result if there was no error
	if err == nil {
		toCache := reachableResourcesResultEntry{toCacheResults}
		cd.c.Set(requestKey, toCache, estimatedSize)
	}

	return err
}

func (cd *Dispatcher) Close() error {
	prometheus.Unregister(cd.checkTotalCounter)
	prometheus.Unregister(cd.lookupTotalCounter)
	prometheus.Unregister(cd.lookupFromCacheCounter)
	prometheus.Unregister(cd.checkFromCacheCounter)
	prometheus.Unregister(cd.cacheHits)
	prometheus.Unregister(cd.cacheMisses)
	prometheus.Unregister(cd.costAddedBytes)
	prometheus.Unregister(cd.costEvictedBytes)
	if cache := cd.c; cache != nil {
		cache.Close()
	}

	return nil
}

// Always verify that we implement the interfaces
var _ dispatch.Dispatcher = &Dispatcher{}
var _ v1.DispatchServiceServer = &Dispatcher{}
