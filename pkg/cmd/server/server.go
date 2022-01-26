package server

import (
	"context"
	"fmt"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/rs/cors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/dashboard"
	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/dispatch"
	combineddispatch "github.com/authzed/spicedb/internal/dispatch/combined"
	"github.com/authzed/spicedb/internal/gateway"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services"
	dispatchSvc "github.com/authzed/spicedb/internal/services/dispatch"
	v1alpha1svc "github.com/authzed/spicedb/internal/services/v1alpha1"
	datastorecfg "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/cmd/util"
)

//go:generate go run github.com/ecordell/optgen -output zz_generated.options.go . Config
type Config struct {
	// API config
	GRPCServer          util.GRPCServerConfig
	PresharedKey        string
	ShutdownGracePeriod time.Duration

	// GRPC Gateway config
	HTTPGateway                    util.HTTPServerConfig
	HTTPGatewayUpstreamAddr        string
	HTTPGatewayUpstreamTLSCertPath string
	HTTPGatewayCorsEnabled         bool
	HTTPGatewayCorsAllowedOrigins  []string

	// Datastore
	Datastore datastorecfg.Config

	// Namespace cache
	NamespaceCacheExpiration time.Duration

	// Schema options
	SchemaPrefixesRequired bool

	// Dispatch options
	DispatchServer         util.GRPCServerConfig
	DispatchMaxDepth       uint32
	DispatchUpstreamAddr   string
	DispatchUpstreamCAPath string

	// API Behavior
	DisableV1SchemaAPI bool

	// Additional Services
	DashboardAPI util.HTTPServerConfig
	MetricsAPI   util.HTTPServerConfig

	// Shared middleware is standard middleware used for both grpc and dispatch
	SharedUnaryMiddleware     []grpc.UnaryServerInterceptor
	SharedStreamingMiddleware []grpc.StreamServerInterceptor

	// Additional middleware for grpc
	UnaryMiddleware     []grpc.UnaryServerInterceptor
	StreamingMiddleware []grpc.StreamServerInterceptor

	// Additional middleware for dispatch
	DispatchUnaryMiddleware     []grpc.UnaryServerInterceptor
	DispatchStreamingMiddleware []grpc.StreamServerInterceptor
}

// Complete validates the config and fills out defaults.
// if there is no error, a completedServerConfig (with limited options for
// mutation) is returned.
func (c *Config) Complete() (RunnableServer, error) {
	if len(c.PresharedKey) < 1 {
		return nil, fmt.Errorf("a preshared key must be provided to authenticate API requests")
	}

	ds, err := datastorecfg.NewDatastore(c.Datastore.ToOption())
	if err != nil {
		return nil, fmt.Errorf("failed to create datastore: %w", err)
	}

	nsm, err := namespace.NewCachingNamespaceManager(ds, c.NamespaceCacheExpiration, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create namespace manager: %w", err)
	}

	grpcprom.EnableHandlingTimeHistogram(grpcprom.WithHistogramBuckets(
		[]float64{.006, .010, .018, .024, .032, .042, .056, .075, .100, .178, .316, .562, 1.000},
	))

	dispatcher, err := combineddispatch.NewDispatcher(nsm, ds,
		combineddispatch.UpstreamAddr(c.DispatchUpstreamAddr),
		combineddispatch.UpstreamCAPath(c.DispatchUpstreamCAPath),
		combineddispatch.GrpcPresharedKey(c.PresharedKey),
		combineddispatch.GrpcDialOpts(
			grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
			grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"consistent-hashring"}`),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create dispatcher: %w", err)
	}

	if len(c.SharedUnaryMiddleware) == 0 && len(c.SharedStreamingMiddleware) == 0 {
		c.SharedUnaryMiddleware, c.SharedStreamingMiddleware = DefaultMiddleware(log.Logger, c.PresharedKey, dispatcher, ds)
	}

	cachingClusterDispatch, err := combineddispatch.NewClusterDispatcher(dispatcher, nsm, ds)
	if err != nil {
		return nil, fmt.Errorf("failed to configure cluster dispatch: %w", err)
	}
	dispatchGrpcServer, err := c.DispatchServer.Complete(zerolog.InfoLevel,
		func(server *grpc.Server) {
			dispatchSvc.RegisterGrpcServices(server, cachingClusterDispatch)
		},
		grpc.ChainUnaryInterceptor(append(c.SharedUnaryMiddleware, c.DispatchUnaryMiddleware...)...),
		grpc.ChainStreamInterceptor(append(c.SharedStreamingMiddleware, c.DispatchStreamingMiddleware...)...),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create dispatch gRPC server: %w", err)
	}

	prefixRequiredOption := v1alpha1svc.PrefixRequired
	if !c.SchemaPrefixesRequired {
		prefixRequiredOption = v1alpha1svc.PrefixNotRequired
	}

	v1SchemaServiceOption := services.V1SchemaServiceEnabled
	if !c.DisableV1SchemaAPI {
		v1SchemaServiceOption = services.V1SchemaServiceDisabled
	}

	grpcServer, err := c.GRPCServer.Complete(zerolog.InfoLevel,
		func(server *grpc.Server) {
			services.RegisterGrpcServices(
				server,
				ds,
				nsm,
				dispatcher,
				c.DispatchMaxDepth,
				prefixRequiredOption,
				v1SchemaServiceOption,
			)
		},
		grpc.ChainUnaryInterceptor(append(c.SharedUnaryMiddleware, c.UnaryMiddleware...)...),
		grpc.ChainStreamInterceptor(append(c.SharedStreamingMiddleware, c.StreamingMiddleware...)...),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC server: %w", err)
	}

	// Configure the gateway to serve HTTP
	gatewayHandler, err := gateway.NewHandler(context.TODO(), c.HTTPGatewayUpstreamAddr, c.HTTPGatewayUpstreamTLSCertPath)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize rest gateway")
	}

	if c.HTTPGatewayCorsEnabled {
		log.Info().Strs("origins", c.HTTPGatewayCorsAllowedOrigins).Msg("Setting REST gateway CORS policy")
		gatewayHandler = cors.New(cors.Options{
			AllowedOrigins:   c.HTTPGatewayCorsAllowedOrigins,
			AllowCredentials: true,
			AllowedHeaders:   []string{"Authorization", "Content-Type"},
			Debug:            log.Debug().Enabled(),
		}).Handler(gatewayHandler)
	}

	gatewayServer, err := c.HTTPGateway.Complete(zerolog.InfoLevel, gatewayHandler)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize rest gateway: %w", err)
	}

	metricsServer, err := c.MetricsAPI.Complete(zerolog.InfoLevel, MetricsHandler())
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metrics server: %w", err)
	}

	dashboardServer, err := c.DashboardAPI.Complete(zerolog.InfoLevel, dashboard.NewHandler(
		c.GRPCServer.Address,
		c.GRPCServer.TLSKeyPath != "" || c.GRPCServer.TLSCertPath != "",
		c.Datastore.Engine,
		ds,
	))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize dashboard server: %w", err)
	}

	return &completedServerConfig{
		gRPCServer:          grpcServer,
		dispatchGRPCServer:  dispatchGrpcServer,
		gatewayServer:       gatewayServer,
		metricsServer:       metricsServer,
		dashboardServer:     dashboardServer,
		ds:                  ds,
		nsm:                 nsm,
		dispatcher:          dispatcher,
		unaryMiddleware:     c.UnaryMiddleware,
		streamingMiddleware: c.StreamingMiddleware,
	}, nil
}

// RunnableServer is a spicedb service set ready to run
type RunnableServer interface {
	Run(ctx context.Context) error
	Dispatcher() dispatch.Dispatcher
	Datastore() datastore.Datastore
	Middleware() ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor)
	SetMiddleware(unaryInterceptors []grpc.UnaryServerInterceptor, streamingInterceptors []grpc.StreamServerInterceptor) RunnableServer
}

// completedServerConfig holds the full configuration to run a spicedb server,
// but is assumed have already been validated via `Complete()` on Config.
// It offers limited options for mutation before Run() starts the services.
type completedServerConfig struct {
	gRPCServer         util.RunnableGRPCServer
	dispatchGRPCServer util.RunnableGRPCServer
	gatewayServer      util.RunnableHTTPServer
	metricsServer      util.RunnableHTTPServer
	dashboardServer    util.RunnableHTTPServer

	ds                  datastore.Datastore
	dispatcher          dispatch.Dispatcher
	nsm                 namespace.Manager
	unaryMiddleware     []grpc.UnaryServerInterceptor
	streamingMiddleware []grpc.StreamServerInterceptor
}

func (c *completedServerConfig) Middleware() ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
	return c.unaryMiddleware, c.streamingMiddleware
}

func (c *completedServerConfig) SetMiddleware(unaryInterceptors []grpc.UnaryServerInterceptor, streamingInterceptors []grpc.StreamServerInterceptor) RunnableServer {
	c.unaryMiddleware = unaryInterceptors
	c.streamingMiddleware = streamingInterceptors
	return c
}

func (c *completedServerConfig) Datastore() datastore.Datastore {
	return c.ds
}

func (c *completedServerConfig) Dispatcher() dispatch.Dispatcher {
	return c.dispatcher
}

func (c *completedServerConfig) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	stopOnCancel := func(stopFn func()) func() error {
		return func() error {
			<-ctx.Done()
			stopFn()
			return nil
		}
	}

	g.Go(c.gRPCServer.Listen)
	g.Go(stopOnCancel(c.gRPCServer.GracefulStop))

	g.Go(c.dispatchGRPCServer.Listen)
	g.Go(stopOnCancel(c.dispatchGRPCServer.GracefulStop))

	g.Go(c.gatewayServer.ListenAndServe)
	g.Go(stopOnCancel(c.gatewayServer.Close))

	g.Go(c.metricsServer.ListenAndServe)
	g.Go(stopOnCancel(c.metricsServer.Close))

	g.Go(c.dashboardServer.ListenAndServe)
	g.Go(stopOnCancel(c.dashboardServer.Close))

	if err := g.Wait(); err != nil {
		log.Warn().Err(err).Msg("error shutting down servers")
	}

	return nil
}
