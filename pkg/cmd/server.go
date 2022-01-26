package cmd

import (
	"time"

	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpczerolog "github.com/grpc-ecosystem/go-grpc-middleware/providers/zerolog/v2"
	grpclog "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/auth"
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
	logmw "github.com/authzed/spicedb/pkg/middleware/logging"
	"github.com/authzed/spicedb/pkg/middleware/requestid"
)

type ServerOption func(*ServerConfig)

//go:generate go run github.com/ecordell/optgen -output zz_generated.server_options.go . ServerConfig
type ServerConfig struct {
	// API config
	GRPCServer          GRPCServerConfig
	PresharedKey        string
	ShutdownGracePeriod time.Duration

	// GRPC Gateway config
	HTTPGateway                    HTTPServerConfig
	HTTPGatewayUpstreamAddr        string
	HTTPGatewayUpstreamTLSCertPath string
	HTTPGatewayCorsEnabled         bool
	HTTPGatewayCorsAllowedOrigins  []string

	// Datastore Config
	Datastore          DatastoreConfig
	ReadOnly           bool
	BootstrapFiles     []string
	BootstrapOverwrite bool

	// Request Hedging
	RequestHedgingEnabled          bool
	RequestHedgingInitialSlowValue time.Duration
	RequestHedgingMaxRequests      uint64
	RequestHedgingQuantile         float64

	// Namespace cache
	NamespaceCacheExpiration time.Duration

	// Schema options
	SchemaPrefixesRequired bool

	// Dispatch options
	DispatchServer         GRPCServerConfig
	DispatchMaxDepth       uint32
	DispatchUpstreamAddr   string
	DispatchUpstreamCAPath string

	// API Behavior
	DisableV1SchemaAPI bool

	// Additional Services
	DashboardAPI HTTPServerConfig
	MetricsAPI   HTTPServerConfig

	// Middleware
	UnaryMiddleware     []grpc.UnaryServerInterceptor
	StreamingMiddleware []grpc.StreamServerInterceptor
}

func DefaultMiddleware(logger zerolog.Logger, presharedKey string) ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
	return []grpc.UnaryServerInterceptor{
			requestid.UnaryServerInterceptor(requestid.GenerateIfMissing(true)),
			logmw.UnaryServerInterceptor(logmw.ExtractMetadataField("x-request-id", "requestID")),
			grpclog.UnaryServerInterceptor(grpczerolog.InterceptorLogger(logger)),
			otelgrpc.UnaryServerInterceptor(),
			grpcauth.UnaryServerInterceptor(auth.RequirePresharedKey(presharedKey)),
			grpcprom.UnaryServerInterceptor,
			servicespecific.UnaryServerInterceptor,
		}, []grpc.StreamServerInterceptor{
			requestid.StreamServerInterceptor(requestid.GenerateIfMissing(true)),
			logmw.StreamServerInterceptor(logmw.ExtractMetadataField("x-request-id", "requestID")),
			grpclog.StreamServerInterceptor(grpczerolog.InterceptorLogger(logger)),
			otelgrpc.StreamServerInterceptor(),
			grpcauth.StreamServerInterceptor(auth.RequirePresharedKey(presharedKey)),
			grpcprom.StreamServerInterceptor,
			servicespecific.StreamServerInterceptor,
		}
}
