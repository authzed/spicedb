package usagemetrics

import (
	"context"
	"strconv"
	"time"

	"github.com/authzed/authzed-go/pkg/responsemeta"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	prometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"

	dispatch "github.com/authzed/spicedb/internal/proto/dispatch/v1"
)

var dispatchBuckets = []float64{1, 5, 10, 25, 50, 100, 250}

var dispatchedCountHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "spicedb",
	Subsystem: "services",
	Name:      "dispatched_count_histogram",
	Help:      "dispatch count per api call distribution in seconds.",
	Buckets:   dispatchBuckets,
}, []string{"method"})

var cachedCountHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "spicedb",
	Subsystem: "services",
	Name:      "cached_count_histogram",
	Help:      "dispatches avoid by caching per api call in seconds.",
	Buckets:   dispatchBuckets,
}, []string{"method"})

var dispatchedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "services",
	Name:      "dispatched_counter",
	Help:      "dispatch counts.",
}, []string{"method"})

var cachedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "services",
	Name:      "cached_counter",
	Help:      "dispatches avoid by caching.",
}, []string{"method"})

type reporter struct{}

func (r *reporter) ServerReporter(ctx context.Context, callMeta interceptors.CallMeta) (interceptors.Reporter, context.Context) {
	_, methodName := interceptors.SplitMethodName(callMeta.FullMethod())
	ctx = ContextWithHandle(ctx)
	return &serverReporter{ctx: ctx, methodName: methodName}, ctx
}

type serverReporter struct {
	interceptors.NoopReporter
	ctx        context.Context
	methodName string
}

func (r *serverReporter) PostCall(err error, rpcDuration time.Duration) {
	responseMeta := FromContext(r.ctx)
	if responseMeta != nil {
		err := annotateAndReportForMetadata(r.ctx, r.methodName, responseMeta)
		if err != nil {
			log.Ctx(r.ctx).Err(err).Msg("could not report metadata")
		}
	}
}

// UnaryServerInterceptor implements a gRPC Middleware for reporting usage metrics
// in both the trailer of the request, as well as to the registered prometheus
// metrics.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return interceptors.UnaryServerInterceptor(&reporter{})
}

// StreamServerInterceptor implements a gRPC Middleware for reporting usage metrics
// in both the trailer of the request, as well as to the registered prometheus
// metrics
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return interceptors.StreamServerInterceptor(&reporter{})
}

func annotateAndReportForMetadata(ctx context.Context, methodName string, metadata *dispatch.ResponseMeta) error {
	dispatchedCountHistogram.WithLabelValues(methodName).Observe(float64(metadata.DispatchCount))
	cachedCountHistogram.WithLabelValues(methodName).Observe(float64(metadata.CachedDispatchCount))

	dispatchedCounter.WithLabelValues(methodName).Add(float64(metadata.DispatchCount))
	cachedCounter.WithLabelValues(methodName).Add(float64(metadata.CachedDispatchCount))

	return responsemeta.SetResponseTrailerMetadata(ctx, map[responsemeta.ResponseMetadataTrailerKey]string{
		responsemeta.DispatchedOperationsCount: strconv.Itoa(int(metadata.DispatchCount)),
		responsemeta.CachedOperationsCount:     strconv.Itoa(int(metadata.CachedDispatchCount)),
	})
}

// Create a new type to prevent context collisions
type responseMetaKey string

var metadataCtxKey responseMetaKey = "dispatched-response-meta"

type metaHandle struct{ metadata *dispatch.ResponseMeta }

// SetInContext should be called in a gRPC handler to correctly set the response metadata
// for the dispatched request.
func SetInContext(ctx context.Context, metadata *dispatch.ResponseMeta) {
	possibleHandle := ctx.Value(metadataCtxKey)
	if possibleHandle == nil {
		return
	}

	handle := possibleHandle.(*metaHandle)
	handle.metadata = metadata
}

// FromContext returns any metadata that was stored in the context.
//
// This is useful for testing that a handler is properly setting the context.
func FromContext(ctx context.Context) *dispatch.ResponseMeta {
	possibleHandle := ctx.Value(metadataCtxKey)
	if possibleHandle == nil {
		return nil
	}
	return possibleHandle.(*metaHandle).metadata
}

// ContextWithHandle creates a new context with a location to store metadata
// returned from a dispatched request.
//
// This should only be called in middleware or testing functions.
func ContextWithHandle(ctx context.Context) context.Context {
	var handle metaHandle
	return context.WithValue(ctx, metadataCtxKey, &handle)
}
