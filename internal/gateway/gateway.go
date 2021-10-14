// Package gateway implements an HTTP server that forwards JSON requests to
// an upstream SpiceDB gRPC server.
package gateway

import (
	"context"
	"net/http"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/auth"
)

var histogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name: "spicedb_rest_gateway_request_duration_seconds",
	Help: "A histogram of the duration spent processing requests to the SpiceDB REST Gateway.",
}, []string{"method"})

// Config represents the require configuration for initializing a REST gateway.
type Config struct {
	Addr                string
	UpstreamAddr        string
	UpstreamTlsDisabled bool
	UpstreamTlsCertPath string
}

// NewHttpServer initializes a new HTTP server with the provided configuration.
func NewHttpServer(ctx context.Context, cfg Config) (*http.Server, error) {
	opts := []grpc.DialOption{grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor())}
	if cfg.UpstreamTlsDisabled {
		opts = append(opts, grpc.WithInsecure())
	} else {
		opts = append(opts, grpcutil.WithCustomCerts(cfg.UpstreamTlsCertPath, grpcutil.SkipVerifyCA))
	}

	mux := runtime.NewServeMux(runtime.WithMetadata(auth.PresharedKeyAnnotator))
	if err := v1.RegisterSchemaServiceHandlerFromEndpoint(ctx, mux, cfg.UpstreamAddr, opts); err != nil {
		return nil, err
	}
	if err := v1.RegisterPermissionsServiceHandlerFromEndpoint(ctx, mux, cfg.UpstreamAddr, opts); err != nil {
		return nil, err
	}

	return &http.Server{
		Addr:    cfg.Addr,
		Handler: promhttp.InstrumentHandlerDuration(histogram, mux),
	}, nil
}
