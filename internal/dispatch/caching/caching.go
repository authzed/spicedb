package caching

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"testing"
	"unsafe"

	"github.com/dustin/go-humanize"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cache"
	"github.com/authzed/spicedb/pkg/middleware/nodeid"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

const (
	errCachingInitialization = "error initializing caching dispatcher: %w"

	prometheusNamespace = "spicedb"
)

// Dispatcher is a dispatcher with cacheInst-in caching.
type Dispatcher struct {
	d          dispatch.Dispatcher
	c          cache.Cache[keys.DispatchCacheKey, any]
	keyHandler keys.Handler

	checkTotalCounter               prometheus.Counter
	checkFromCacheCounter           prometheus.Counter
	lookupResourcesTotalCounter     prometheus.Counter
	lookupResourcesFromCacheCounter prometheus.Counter
	lookupSubjectsTotalCounter      prometheus.Counter
	lookupSubjectsFromCacheCounter  prometheus.Counter
}

func DispatchTestCache(t testing.TB) cache.Cache[keys.DispatchCacheKey, any] {
	cache, err := cache.NewStandardCache[keys.DispatchCacheKey, any](&cache.Config{
		NumCounters: 1000,
		MaxCost:     1 * humanize.MiByte,
	})
	require.Nil(t, err)
	return cache
}

// NewCachingDispatcher creates a new dispatch.Dispatcher which delegates
// dispatch requests and caches the responses when possible and desirable.
func NewCachingDispatcher(cacheInst cache.Cache[keys.DispatchCacheKey, any], metricsEnabled bool, prometheusSubsystem string, keyHandler keys.Handler) (*Dispatcher, error) {
	if cacheInst == nil {
		cacheInst = cache.NoopCache[keys.DispatchCacheKey, any]()
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

	lookupResourcesTotalCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: prometheusNamespace,
		Subsystem: prometheusSubsystem,
		Name:      "lookup_resources_total",
	})
	lookupResourcesFromCacheCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: prometheusNamespace,
		Subsystem: prometheusSubsystem,
		Name:      "lookup_resources_from_cache_total",
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

	if metricsEnabled && prometheusSubsystem != "" {
		err := prometheus.Register(checkTotalCounter)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}
		err = prometheus.Register(checkFromCacheCounter)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}
		err = prometheus.Register(lookupResourcesTotalCounter)
		if err != nil {
			return nil, fmt.Errorf(errCachingInitialization, err)
		}
		err = prometheus.Register(lookupResourcesFromCacheCounter)
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
	}

	if keyHandler == nil {
		keyHandler = &keys.DirectKeyHandler{}
	}

	return &Dispatcher{
		d:                               fakeDelegate{},
		c:                               cacheInst,
		keyHandler:                      keyHandler,
		checkTotalCounter:               checkTotalCounter,
		checkFromCacheCounter:           checkFromCacheCounter,
		lookupResourcesTotalCounter:     lookupResourcesTotalCounter,
		lookupResourcesFromCacheCounter: lookupResourcesFromCacheCounter,
		lookupSubjectsTotalCounter:      lookupSubjectsTotalCounter,
		lookupSubjectsFromCacheCounter:  lookupSubjectsFromCacheCounter,
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
	span := trace.SpanFromContext(ctx)
	if cachedResultRaw, found := cd.c.Get(requestKey); found {
		var response v1.DispatchCheckResponse
		if err := response.UnmarshalVT(cachedResultRaw.([]byte)); err != nil {
			return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{}}, err
		}

		if req.Metadata.DepthRemaining >= response.Metadata.DepthRequired {
			cd.checkFromCacheCounter.Inc()
			// If debugging is requested, add the req and the response to the trace.
			if req.Debug == v1.DispatchCheckRequest_ENABLE_BASIC_DEBUGGING {
				nodeID, err := nodeid.FromContext(ctx)
				if err != nil {
					log.Err(err).Msg("failed to get nodeID from context")
				}

				response.Metadata.DebugInfo = &v1.DebugInformation{
					Check: &v1.CheckDebugTrace{
						Request:        req,
						Results:        maps.Clone(response.ResultsByResourceId),
						IsCachedResult: true,
						SourceId:       nodeID,
					},
				}
			}

			span.SetAttributes(attribute.Bool("cached", true))
			return &response, nil
		}
	}
	span.SetAttributes(attribute.Bool("cached", false))
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

func sliceSize(xs []byte) int64 {
	// Slice Header + Slice Contents
	return int64(int(unsafe.Sizeof(xs)) + len(xs))
}

func (cd *Dispatcher) DispatchLookupResources2(req *v1.DispatchLookupResources2Request, stream dispatch.LookupResources2Stream) error {
	cd.lookupResourcesTotalCounter.Inc()

	requestKey, err := cd.keyHandler.LookupResources2CacheKey(stream.Context(), req)
	if err != nil {
		return err
	}

	if cachedResultRaw, found := cd.c.Get(requestKey); found {
		cd.lookupResourcesFromCacheCounter.Inc()
		for _, slice := range cachedResultRaw.([][]byte) {
			var response v1.DispatchLookupResources2Response
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
	wrapped := &dispatch.WrappedDispatchStream[*v1.DispatchLookupResources2Response]{
		Stream: stream,
		Ctx:    stream.Context(),
		Processor: func(result *v1.DispatchLookupResources2Response) (*v1.DispatchLookupResources2Response, bool, error) {
			adjustedResult := result.CloneVT()
			adjustedResult.Metadata.CachedDispatchCount = adjustedResult.Metadata.DispatchCount
			adjustedResult.Metadata.DispatchCount = 0
			adjustedResult.Metadata.DebugInfo = nil

			adjustedBytes, err := adjustedResult.MarshalVT()
			if err != nil {
				return &v1.DispatchLookupResources2Response{Metadata: &v1.ResponseMeta{}}, false, err
			}

			mu.Lock()
			toCacheResults = append(toCacheResults, adjustedBytes)
			mu.Unlock()

			return result, true, nil
		},
	}

	if err := cd.d.DispatchLookupResources2(req, wrapped); err != nil {
		return err
	}

	var size int64
	for _, slice := range toCacheResults {
		size += sliceSize(slice)
	}

	cd.c.Set(requestKey, toCacheResults, size)
	return nil
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
	prometheus.Unregister(cd.checkFromCacheCounter)
	prometheus.Unregister(cd.lookupResourcesTotalCounter)
	prometheus.Unregister(cd.lookupResourcesFromCacheCounter)
	prometheus.Unregister(cd.lookupSubjectsFromCacheCounter)
	prometheus.Unregister(cd.lookupSubjectsTotalCounter)
	if cache := cd.c; cache != nil {
		cache.Close()
	}

	return nil
}

func (cd *Dispatcher) ReadyState() dispatch.ReadyState {
	if cd.c == nil {
		return dispatch.ReadyState{
			IsReady: false,
			Message: "caching dispatcher is missing cache",
		}
	}

	if cd.d == nil {
		return dispatch.ReadyState{
			IsReady: false,
			Message: "caching dispatcher is missing delegate dispatcher",
		}
	}

	return cd.d.ReadyState()
}

// Always verify that we implement the interfaces
var _ dispatch.Dispatcher = &Dispatcher{}
