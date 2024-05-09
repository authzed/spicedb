package grpcutil

import (
	"context"
	"sync"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

var (
	serverMetricsOnce sync.Once
	serverMetrics     *grpcprom.ServerMetrics
)

func exemplarFromContextFunc(ctx context.Context) prometheus.Labels {
	if span := trace.SpanContextFromContext(ctx); span.IsSampled() {
		return prometheus.Labels{"traceID": span.TraceID().String()}
	}
	return nil
}

func srvMetrics() *grpcprom.ServerMetrics {
	serverMetricsOnce.Do(func() {
		serverMetrics = grpcprom.NewServerMetrics([]grpcprom.ServerMetricsOption{
			grpcprom.WithServerHandlingTimeHistogram(
				grpcprom.WithHistogramBuckets([]float64{.001, .003, .006, .010, .018, .024, .032, .042, .056, .075, .100, .178, .316, .562, 1, 5}),
			),
		}...)

		// Deliberately ignore if these metrics were already registered, so that
		// these metrics can be optionally registered with custom labels.
		_ = prometheus.Register(serverMetrics)
	})

	return serverMetrics
}

func PrometheusUnaryInterceptor() grpc.UnaryServerInterceptor {
	return srvMetrics().UnaryServerInterceptor(grpcprom.WithExemplarFromContext(exemplarFromContextFunc))
}

func PrometheusStreamInterceptor() grpc.StreamServerInterceptor {
	return srvMetrics().StreamServerInterceptor(grpcprom.WithExemplarFromContext(exemplarFromContextFunc))
}
