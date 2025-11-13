package perfinsights

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"

	"github.com/authzed/ctxkey"
	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

type APIShapeLabel string

const (
	ResourceTypeLabel     APIShapeLabel = "resource_type"
	ResourceRelationLabel APIShapeLabel = "resource_relation"
	SubjectTypeLabel      APIShapeLabel = "subject_type"
	SubjectRelationLabel  APIShapeLabel = "subject_relation"
	NameLabel             APIShapeLabel = "name"
	FilterLabel           APIShapeLabel = "filter"
)

var allLabels = []string{
	string(ResourceTypeLabel),
	string(ResourceRelationLabel),
	string(SubjectTypeLabel),
	string(SubjectRelationLabel),
	string(NameLabel),
	string(FilterLabel),
}

// APIShapeLabels is a map of APIShapeLabel to any value.
type APIShapeLabels map[APIShapeLabel]any

// NoLabels returns an empty APIShapeLabels map.
func NoLabels() APIShapeLabels {
	return APIShapeLabels{}
}

// APIShapeLatency is the metric that SpiceDB uses to keep track of the latency
// of API calls, by shape. The "shape" of an API call is defined as the portions
// of the API call that are not the specific object IDs, e.g. the resource type,
// the relation, the subject type, etc.
//
// NOTE: This metric is a *native* histogram, which means that instead of predefined
// the buckets, it uses a factor to determine the buckets. This is useful for latency-based
// metrics, as the latency can vary widely and having a fixed set of buckets can lead to
// imprecise results.
//
// To use make use of native histograms, a special flag must be set on Prometheus:
// https://prometheus.io/docs/prometheus/latest/feature_flags/#native-histograms
var APIShapeLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace:                   "spicedb",
	Subsystem:                   "perf_insights",
	Name:                        "api_shape_latency_seconds",
	Help:                        "The latency of API calls, by shape",
	Buckets:                     nil,
	NativeHistogramBucketFactor: 1.1,
}, append([]string{"api_kind"}, allLabels...))

// ShapeBuilder is a function that returns a slice of strings representing the shape of the API call.
// This is used to report the shape of the API call to Prometheus.
type ShapeBuilder func() APIShapeLabels

// ObserveShapeLatency observes the latency of the API call with the given shape.
func ObserveShapeLatency(ctx context.Context, methodName string, shape APIShapeLabels, duration time.Duration) {
	observeShapeLatency(ctx, APIShapeLatency, methodName, shape, duration)
}

func observeShapeLatency(ctx context.Context, metric *prometheus.HistogramVec, methodName string, shape APIShapeLabels, duration time.Duration) {
	labels := buildLabels(methodName, shape)
	if len(labels) == 0 {
		log.Warn().Str("method", methodName).
			Msg("ShapeBuilder returned an empty shape, skipping reporting")
		return
	}

	o := metric.WithLabelValues(labels...)
	if exemplarObserver, ok := o.(prometheus.ExemplarObserver); ok {
		if spanCtx := trace.SpanContextFromContext(ctx); spanCtx.HasTraceID() {
			traceID := prometheus.Labels{"traceID": spanCtx.TraceID().String()}
			exemplarObserver.ObserveWithExemplar(duration.Seconds(), traceID)
			return
		}
	}

	o.Observe(duration.Seconds())
}

func buildLabels(methodName string, shape APIShapeLabels) []string {
	labels := make([]string, len(allLabels)+1)
	labels[0] = methodName

	for index, label := range allLabels {
		shapeValue, ok := shape[APIShapeLabel(label)]
		if !ok {
			continue
		}

		switch t := shapeValue.(type) {
		case string:
			labels[index+1] = t

		case int:
			labels[index+1] = strconv.Itoa(t)

		case uint32:
			intValue, err := safecast.Convert[int](t)
			if err != nil {
				log.Warn().Str("method", methodName).
					Str("key", label).
					Str("type", fmt.Sprintf("%T", t)).
					Msg("ShapeBuilder could not convert int, skipping reporting")
				return nil
			}
			labels[index+1] = strconv.Itoa(intValue)

		case int64:
			intValue, err := safecast.Convert[int](t)
			if err != nil {
				log.Warn().Str("method", methodName).
					Str("key", label).
					Str("type", fmt.Sprintf("%T", t)).
					Msg("ShapeBuilder could not convert int, skipping reporting")
				return nil
			}
			labels[index+1] = strconv.Itoa(intValue)

		default:
			spiceerrors.DebugAssertf(func() bool { return false }, "unsupported type %T", t)
			log.Warn().Str("method", methodName).
				Str("key", label).
				Str("type", fmt.Sprintf("%T", t)).
				Msg("ShapeBuilder returned an unsupported type, skipping reporting")
			return nil
		}
	}

	return labels
}

type reporter struct {
	isEnabled bool
}

func (r *reporter) ServerReporter(ctx context.Context, callMeta interceptors.CallMeta) (interceptors.Reporter, context.Context) {
	if !r.isEnabled {
		return &interceptors.NoopReporter{}, ctx
	}

	_, methodName := grpcutil.SplitMethodName(callMeta.FullMethod())
	ctx = contextWithHandle(ctx)
	return &serverReporter{ctx: ctx, methodName: methodName}, ctx
}

type serverReporter struct {
	interceptors.NoopReporter
	ctx        context.Context
	methodName string
}

var ctxKey = ctxkey.NewBoxedWithDefault[ShapeBuilder](nil)

func (r *serverReporter) PostCall(err error, duration time.Duration) {
	// Do not report if the call resulted in an error.
	if err != nil {
		return
	}

	shapeClosure := ctxKey.Value(r.ctx)
	spiceerrors.DebugAssertNotNilf(shapeClosure, "ShapeBuilder should not be nil")

	// If not testing and nil, simply skip.
	if shapeClosure == nil {
		log.Warn().Str("method", r.methodName).
			Msg("ShapeBuilder is nil, skipping reporting")
		return
	}

	ObserveShapeLatency(r.ctx, r.methodName, shapeClosure(), duration)
}

// UnaryServerInterceptor returns a gRPC server-side interceptor that provides reporting for Unary RPCs.
func UnaryServerInterceptor(isEnabled bool) grpc.UnaryServerInterceptor {
	return interceptors.UnaryServerInterceptor(&reporter{isEnabled: isEnabled})
}

// StreamServerInterceptor returns a gRPC server-side interceptor that provides reporting for Streaming RPCs.
func StreamServerInterceptor(isEnabled bool) grpc.StreamServerInterceptor {
	return interceptors.StreamServerInterceptor(&reporter{isEnabled: isEnabled})
}

// SetInContext sets the ShapeBuilder in the context.
func SetInContext(ctx context.Context, builder ShapeBuilder) {
	ctxKey.Set(ctx, builder)
}

// contextWithHandle returns a context with the ShapeBuilder set.
// Should only be called by the server reporter or a test.
func contextWithHandle(ctx context.Context) context.Context {
	return ctxKey.SetBox(ctx)
}
