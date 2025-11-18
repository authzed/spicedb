package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	grpcfilters "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/filters"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	httpfilters "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp/filters"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	protobuf "google.golang.org/protobuf/proto"

	"github.com/authzed/authzed-go/proto"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/internal/grpchelpers"
)

var histogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "spicedb",
	Subsystem: "rest_gateway",
	Name:      "request_duration_seconds",
	Help:      "A histogram of the duration spent processing requests to the SpiceDB REST Gateway.",
}, []string{"method"})

// NewHandler creates an REST gateway HTTP CloserHandler with the provided upstream
// configuration.
func NewHandler(ctx context.Context, upstreamAddr, upstreamTLSCertPath string) (*CloserHandler, error) {
	if upstreamAddr == "" {
		return nil, errors.New("upstreamAddr must not be empty")
	}

	// Always disable health check tracing to reduce trace volume
	clientHandlerOpts := []otelgrpc.Option{
		otelgrpc.WithFilter(grpcfilters.Not(grpcfilters.HealthCheck())),
	}

	opts := []grpc.DialOption{
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(clientHandlerOpts...)),
	}
	if upstreamTLSCertPath == "" {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		certsOpt, err := grpcutil.WithCustomCerts(grpcutil.SkipVerifyCA, upstreamTLSCertPath)
		if err != nil {
			return nil, err
		}
		opts = append(opts, certsOpt)
	}

	healthConn, err := grpchelpers.Dial(ctx, upstreamAddr, opts...)
	if err != nil {
		return nil, err
	}

	gwMux := runtime.NewServeMux(
		runtime.WithMetadata(OtelAnnotator),
		runtime.WithIncomingHeaderMatcher(customIncomingHeaderMatcher),
		runtime.WithForwardResponseOption(forwardRequestIDTrailer),
		runtime.WithHealthzEndpoint(healthpb.NewHealthClient(healthConn)),
	)
	schemaConn, err := registerHandler(ctx, gwMux, upstreamAddr, opts, v1.RegisterSchemaServiceHandler)
	if err != nil {
		return nil, err
	}

	permissionsConn, err := registerHandler(ctx, gwMux, upstreamAddr, opts, v1.RegisterPermissionsServiceHandler)
	if err != nil {
		return nil, err
	}

	watchConn, err := registerHandler(ctx, gwMux, upstreamAddr, opts, v1.RegisterWatchServiceHandler)
	if err != nil {
		return nil, err
	}

	experimentalConn, err := registerHandler(ctx, gwMux, upstreamAddr, opts, v1.RegisterExperimentalServiceHandler)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.Handle("/openapi.json", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, proto.OpenAPISchema)
	}))
	mux.Handle("/", gwMux)

	// Always disable health check tracing to reduce trace volume
	otelHandlerOpts := []otelhttp.Option{
		otelhttp.WithFilter(httpfilters.Not(httpfilters.Path("/healthz"))),
	}

	finalHandler := promhttp.InstrumentHandlerDuration(histogram, otelhttp.NewHandler(mux, "gateway", otelHandlerOpts...))
	return newCloserHandler(finalHandler, schemaConn, permissionsConn, watchConn, healthConn, experimentalConn), nil
}

// CloserHandler is a http.Handler and a io.Closer. Meant to keep track of resources to closer
// for a handler.
type CloserHandler struct {
	closers  []io.Closer
	delegate http.Handler
}

func (cdh CloserHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	cdh.delegate.ServeHTTP(writer, request)
}

// newCloserHandler creates a new delegated http.Handler that will keep track of io.Closer to closer
func newCloserHandler(delegate http.Handler, closers ...io.Closer) *CloserHandler {
	return &CloserHandler{
		closers:  closers,
		delegate: delegate,
	}
}

func (cdh CloserHandler) Close() error {
	for _, closer := range cdh.closers {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	return nil
}

// HandlerRegisterer is a function that registers a Gateway Handler in a ServeMux
type HandlerRegisterer func(ctx context.Context, mux *runtime.ServeMux, conn *grpc.ClientConn) error

// registerHandler will open a connection with the provided grpc.DialOptions against the endpoint, and
// will use it to invoke an HTTP Gateway handler factory method HandlerRegisterer. It returns the gRPC
// connection.
//
// gRPC generated code does not expose a means to close the opened connections other than implicitly via
// context cancellation. This factory method makes closing them explicit.
func registerHandler(ctx context.Context, mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption,
	registerer HandlerRegisterer,
) (*grpc.ClientConn, error) {
	conn, err := grpchelpers.Dial(ctx, endpoint, opts...)
	if err != nil {
		return nil, err
	}
	if err := registerer(ctx, mux, conn); err != nil {
		if connerr := conn.Close(); connerr != nil {
			return nil, err
		}
		return nil, err
	}

	return conn, nil
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
	otelgrpc.Inject(ctx, &metadataCopy, defaultOtelOpts...) // nolint:staticcheck
	return metadataCopy
}

// forwardRequestIDTrailer copies the request ID from gRPC trailers to HTTP response headers.
// This ensures x-request-id is visible to HTTP clients while maintaining gRPC retry policy compliance.
func forwardRequestIDTrailer(ctx context.Context, w http.ResponseWriter, _ protobuf.Message) error {
	md, ok := runtime.ServerMetadataFromContext(ctx)
	if !ok {
		return nil
	}

	// Check standard x-request-id key
	if vals := md.TrailerMD.Get("x-request-id"); len(vals) > 0 {
		w.Header().Set("X-Request-Id", vals[0])
		return nil
	}

	// Check legacy key for backward compatibility
	if vals := md.TrailerMD.Get("io.spicedb.respmeta.requestid"); len(vals) > 0 {
		w.Header().Set("X-Request-Id", vals[0])
	}

	return nil
}

// customIncomingHeaderMatcher translates HTTP headers to gRPC metadata.
// This ensures x-request-id from HTTP clients is passed through without the
// grpcgateway- prefix, making it available to the request ID middleware.
func customIncomingHeaderMatcher(key string) (string, bool) {
	switch strings.ToLower(key) {
	case "x-request-id":
		return "x-request-id", true
	default:
		return runtime.DefaultHeaderMatcher(key)
	}
}
