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
	"google.golang.org/grpc/metadata"
)

var histogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "spicedb",
	Subsystem: "rest_gateway",
	Name:      "request_duration_seconds",
	Help:      "A histogram of the duration spent processing requests to the SpiceDB REST Gateway.",
}, []string{"method"})

// Config represents the require configuration for initializing a REST gateway.
type Config struct {
	// Addr is the address on which the HTTP server will be configured to listen.
	Addr string

	// UpstreamAddr is the address of the gRPC server to which requests will be
	// forwarded.
	UpstreamAddr string

	// UpstreamTLSDisabled toggles whether or not the upstream connection will be
	// secure.
	UpstreamTLSDisabled bool

	// UpstreamTLSCertPath is the filesystem location of the certificate used to
	// secure the upstream connection.
	UpstreamTLSCertPath string
}

// NewHTTPServer initializes a new HTTP server with the provided configuration.
func NewHTTPServer(ctx context.Context, cfg Config) (*http.Server, error) {
	opts := []grpc.DialOption{
		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
		grpc.WithStreamInterceptor(otelgrpc.StreamClientInterceptor()),
	}
	if cfg.UpstreamTLSDisabled {
		opts = append(opts, grpc.WithInsecure())
	} else {
		opts = append(opts, grpcutil.WithCustomCerts(cfg.UpstreamTLSCertPath, grpcutil.SkipVerifyCA))
	}

	gwMux := runtime.NewServeMux(runtime.WithMetadata(OtelAnnotator))
	if err := v1.RegisterSchemaServiceHandlerFromEndpoint(ctx, gwMux, cfg.UpstreamAddr, opts); err != nil {
		return nil, err
	}
	if err := v1.RegisterPermissionsServiceHandlerFromEndpoint(ctx, gwMux, cfg.UpstreamAddr, opts); err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.Handle("/openapi.json", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, proto.OpenAPISchema)
	}))
	mux.Handle("/", gwMux)

	return &http.Server{
		Addr:    cfg.Addr,
		Handler: promhttp.InstrumentHandlerDuration(histogram, otelhttp.NewHandler(mux, "gateway")),
	}, nil
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
