// Package gateway implements an HTTP server that forwards JSON requests to
// an upstream SpiceDB gRPC server.
package gateway

import (
	"context"
	"io"
	"net/http"

	"github.com/authzed/authzed-go/proto"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

var histogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "spicedb",
	Subsystem: "rest_gateway",
	Name:      "request_duration_seconds",
	Help:      "A histogram of the duration spent processing requests to the SpiceDB REST Gateway.",
}, []string{"method"})

// NewHandler creates an REST gateway HTTP Handler with the provided upstream
// configuration.
func NewHandler(ctx context.Context, upstreamAddr, upstreamTLSCertPath string) (http.Handler, error) {
	opts := []grpc.DialOption{
		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
		grpc.WithStreamInterceptor(otelgrpc.StreamClientInterceptor()),
	}
	if upstreamTLSCertPath == "" {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		opts = append(opts, grpcutil.WithCustomCerts(upstreamTLSCertPath, grpcutil.SkipVerifyCA))
	}

	healthConn, err := grpc.Dial(upstreamAddr, opts...)
	if err != nil {
		return nil, err
	}

	gwMux := runtime.NewServeMux(runtime.WithMetadata(OtelAnnotator), runtime.WithHealthzEndpoint(healthpb.NewHealthClient(healthConn)))
	if err := v1.RegisterSchemaServiceHandlerFromEndpoint(ctx, gwMux, upstreamAddr, opts); err != nil {
		return nil, err
	}
	if err := v1.RegisterPermissionsServiceHandlerFromEndpoint(ctx, gwMux, upstreamAddr, opts); err != nil {
		return nil, err
	}
	if err := v1.RegisterWatchServiceHandlerFromEndpoint(ctx, gwMux, upstreamAddr, opts); err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.Handle("/openapi.json", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, proto.OpenAPISchema)
	}))
	mux.Handle("/", gwMux)

	return promhttp.InstrumentHandlerDuration(histogram, otelhttp.NewHandler(mux, "gateway")), nil
}

var defaultOtelOpts = []otelgrpc.Option{
	otelgrpc.WithPropagators(otel.GetTextMapPropagator()),
	otelgrpc.WithTracerProvider(otel.GetTracerProvider()),
}

// OtelAnnotator propagates the OpenTelemetry tracing context to the outgoing
// gRPC metadata.
func OtelAnnotator(ctx context.Context, r *http.Request) metadata.MD {
	requestMetadata, _ := metadata.FromOutgoingContext(ctx)
	metadataCopy := requestMetadata.Copy()

	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(r.Header))
	otelgrpc.Inject(ctx, &metadataCopy, defaultOtelOpts...)
	return metadataCopy
}
