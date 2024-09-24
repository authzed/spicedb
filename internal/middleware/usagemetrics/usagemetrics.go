package usagemetrics

import (
	"context"
	"strconv"
	"time"

	"github.com/authzed/authzed-go/pkg/responsemeta"
	"github.com/authzed/grpcutil"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"

	log "github.com/authzed/spicedb/internal/logging"
	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

var (
	// DispatchedCountLabels are the labels that DispatchedCountHistogram will
	// have have by default.
	DispatchedCountLabels = []string{"method", "cached"}

	// DispatchedCountHistogram is the metric that SpiceDB uses to keep track
	// of the number of downstream dispatches that are performed to answer a
	// single query.
	DispatchedCountHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "spicedb",
		Subsystem: "services",
		Name:      "dispatches",
		Help:      "Histogram of cluster dispatches performed by the instance.",
		Buckets:   []float64{1, 5, 10, 25, 50, 100, 250},
	}, DispatchedCountLabels)
)

type reporter struct{}

func (r *reporter) ServerReporter(ctx context.Context, callMeta interceptors.CallMeta) (interceptors.Reporter, context.Context) {
	_, methodName := grpcutil.SplitMethodName(callMeta.FullMethod())
	ctx = ContextWithHandle(ctx)
	return &serverReporter{ctx: ctx, methodName: methodName}, ctx
}

type serverReporter struct {
	interceptors.NoopReporter
	ctx        context.Context
	methodName string
}

func (r *serverReporter) PostCall(_ error, _ time.Duration) {
	responseMeta := FromContext(r.ctx)
	if responseMeta == nil {
		responseMeta = &dispatch.ResponseMeta{}
	}

	err := annotateAndReportForMetadata(r.ctx, r.methodName, responseMeta)
	// if context is cancelled, the stream will be closed, and gRPC will return ErrIllegalHeaderWrite
	// this prevents logging unnecessary error messages
	if r.ctx.Err() != nil {
		return
	}
	if err != nil {
		log.Ctx(r.ctx).Warn().Err(err).Msg("usagemetrics: could not report metadata")
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
	DispatchedCountHistogram.WithLabelValues(methodName, "false").Observe(float64(metadata.DispatchCount))
	DispatchedCountHistogram.WithLabelValues(methodName, "true").Observe(float64(metadata.CachedDispatchCount))

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
