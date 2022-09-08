package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/authzed/grpcutil"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/rs/cors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/auth"
	"github.com/authzed/spicedb/internal/dashboard"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	"github.com/authzed/spicedb/internal/dispatch"
	clusterdispatch "github.com/authzed/spicedb/internal/dispatch/cluster"
	combineddispatch "github.com/authzed/spicedb/internal/dispatch/combined"
	"github.com/authzed/spicedb/internal/gateway"
	"github.com/authzed/spicedb/internal/services"
	dispatchSvc "github.com/authzed/spicedb/internal/services/dispatch"
	"github.com/authzed/spicedb/internal/services/health"
	v1svc "github.com/authzed/spicedb/internal/services/v1"
	v1alpha1svc "github.com/authzed/spicedb/internal/services/v1alpha1"
	"github.com/authzed/spicedb/internal/telemetry"
	"github.com/authzed/spicedb/pkg/balancer"
	datastorecfg "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/authzed/spicedb/pkg/datastore"
)

//go:generate go run github.com/ecordell/optgen -output zz_generated.options.go . Config
type Config struct {
	// API config
	GRPCServer             util.GRPCServerConfig
	GRPCAuthFunc           grpc_auth.AuthFunc
	PresharedKey           []string
	ShutdownGracePeriod    time.Duration
	DisableVersionResponse bool

	// GRPC Gateway config
	HTTPGateway                    util.HTTPServerConfig
	HTTPGatewayUpstreamAddr        string
	HTTPGatewayUpstreamTLSCertPath string
	HTTPGatewayCorsEnabled         bool
	HTTPGatewayCorsAllowedOrigins  []string

	// Datastore
	DatastoreConfig datastorecfg.Config
	Datastore       datastore.Datastore

	// Namespace cache
	NamespaceCacheConfig CacheConfig

	// Schema options
	SchemaPrefixesRequired bool

	// Dispatch options
	DispatchServer               util.GRPCServerConfig
	DispatchMaxDepth             uint32
	DispatchConcurrencyLimit     uint16
	DispatchUpstreamAddr         string
	DispatchUpstreamCAPath       string
	DispatchClientMetricsPrefix  string
	DispatchClusterMetricsPrefix string
	Dispatcher                   dispatch.Dispatcher

	DispatchCacheConfig        CacheConfig
	ClusterDispatchCacheConfig CacheConfig

	// API Behavior
	DisableV1SchemaAPI       bool
	MaximumUpdatesPerWrite   uint16
	MaximumPreconditionCount uint16

	// Additional Services
	DashboardAPI util.HTTPServerConfig
	MetricsAPI   util.HTTPServerConfig

	// Middleware for grpc
	UnaryMiddleware     []grpc.UnaryServerInterceptor
	StreamingMiddleware []grpc.StreamServerInterceptor

	// Middleware for dispatch
	DispatchUnaryMiddleware     []grpc.UnaryServerInterceptor
	DispatchStreamingMiddleware []grpc.StreamServerInterceptor

	// Telemetry
	SilentlyDisableTelemetry bool
	TelemetryCAOverridePath  string
	TelemetryEndpoint        string
	TelemetryInterval        time.Duration
}

// Complete validates the config and fills out defaults.
// if there is no error, a completedServerConfig (with limited options for
// mutation) is returned.
func (c *Config) Complete() (RunnableServer, error) {
	if len(c.PresharedKey) < 1 && c.GRPCAuthFunc == nil {
		return nil, fmt.Errorf("a preshared key must be provided to authenticate API requests")
	}

	if c.GRPCAuthFunc == nil {
		log.Trace().Int("preshared-keys-count", len(c.PresharedKey)).Msg("using gRPC auth with preshared key(s)")
		for index, presharedKey := range c.PresharedKey {
			if len(presharedKey) == 0 {
				return nil, fmt.Errorf("preshared key #%d is empty", index+1)
			}

			log.Trace().Int(fmt.Sprintf("preshared-key-%d-length", index+1), len(presharedKey)).Msg("preshared key configured")
		}

		c.GRPCAuthFunc = auth.RequirePresharedKey(c.PresharedKey)
	} else {
		log.Trace().Msg("using preconfigured auth function")
	}

	ds := c.Datastore
	if ds == nil {
		var err error
		ds, err = datastorecfg.NewDatastore(c.DatastoreConfig.ToOption())
		if err != nil {
			return nil, fmt.Errorf("failed to create datastore: %w", err)
		}
	}

	nscc, err := c.NamespaceCacheConfig.Complete()
	if err != nil {
		return nil, fmt.Errorf("failed to create namespace cache: %w", err)
	}

	ds, err = proxy.NewCachingDatastoreProxy(ds, nscc)
	if err != nil {
		return nil, fmt.Errorf("failed to create namespace caching datastore proxy: %w", err)
	}

	enableGRPCHistogram()

	dispatcher := c.Dispatcher
	if dispatcher == nil {
		var err error
		cc, cerr := c.DispatchCacheConfig.Complete()
		if cerr != nil {
			return nil, fmt.Errorf("failed to create dispatcher: %w", cerr)
		}

		dispatchPresharedKey := ""
		if len(c.PresharedKey) > 0 {
			dispatchPresharedKey = c.PresharedKey[0]
		}

		dispatcher, err = combineddispatch.NewDispatcher(
			combineddispatch.UpstreamAddr(c.DispatchUpstreamAddr),
			combineddispatch.UpstreamCAPath(c.DispatchUpstreamCAPath),
			combineddispatch.GrpcPresharedKey(dispatchPresharedKey),
			combineddispatch.GrpcDialOpts(
				grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
				grpc.WithDefaultServiceConfig(balancer.BalancerServiceConfig),
			),
			combineddispatch.PrometheusSubsystem(c.DispatchClientMetricsPrefix),
			combineddispatch.CacheConfig(cc),
			combineddispatch.ConcurrencyLimit(c.DispatchConcurrencyLimit),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create dispatcher: %w", err)
		}
	}

	if len(c.DispatchUnaryMiddleware) == 0 && len(c.DispatchStreamingMiddleware) == 0 {
		if c.GRPCAuthFunc == nil {
			c.DispatchUnaryMiddleware, c.DispatchStreamingMiddleware = DefaultDispatchMiddleware(log.Logger, auth.RequirePresharedKey(c.PresharedKey), ds)
		} else {
			c.DispatchUnaryMiddleware, c.DispatchStreamingMiddleware = DefaultDispatchMiddleware(log.Logger, c.GRPCAuthFunc, ds)
		}
	}

	var cachingClusterDispatch dispatch.Dispatcher
	if c.DispatchServer.Enabled {
		cdcc, cerr := c.ClusterDispatchCacheConfig.Complete()
		if cerr != nil {
			return nil, fmt.Errorf("failed to configure cluster dispatch: %w", cerr)
		}

		var err error
		cachingClusterDispatch, err = clusterdispatch.NewClusterDispatcher(
			dispatcher,
			clusterdispatch.PrometheusSubsystem(c.DispatchClusterMetricsPrefix),
			clusterdispatch.CacheConfig(cdcc),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to configure cluster dispatch: %w", err)
		}
	}

	dispatchGrpcServer, err := c.DispatchServer.Complete(zerolog.InfoLevel,
		func(server *grpc.Server) {
			dispatchSvc.RegisterGrpcServices(server, cachingClusterDispatch)
		},
		grpc.ChainUnaryInterceptor(c.DispatchUnaryMiddleware...),
		grpc.ChainStreamInterceptor(c.DispatchStreamingMiddleware...),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create dispatch gRPC server: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	datastoreFeatures, err := ds.Features(ctx)
	if err != nil {
		return nil, fmt.Errorf("error determining datastore features: %w", err)
	}

	prefixRequiredOption := v1alpha1svc.PrefixRequired
	if !c.SchemaPrefixesRequired {
		prefixRequiredOption = v1alpha1svc.PrefixNotRequired
	}

	v1SchemaServiceOption := services.V1SchemaServiceEnabled
	if c.DisableV1SchemaAPI {
		v1SchemaServiceOption = services.V1SchemaServiceDisabled
	}

	watchServiceOption := services.WatchServiceEnabled
	if !datastoreFeatures.Watch.Enabled {
		log.Warn().Str("reason", datastoreFeatures.Watch.Reason).Msg("watch api disabled; underlying datastore does not support it")
		watchServiceOption = services.WatchServiceDisabled
	}

	if len(c.UnaryMiddleware) == 0 && len(c.StreamingMiddleware) == 0 {
		c.UnaryMiddleware, c.StreamingMiddleware = DefaultMiddleware(log.Logger, c.GRPCAuthFunc, !c.DisableVersionResponse, dispatcher, ds)
	}

	permSysConfig := v1svc.PermissionsServerConfig{
		MaxPreconditionsCount: c.MaximumPreconditionCount,
		MaxUpdatesPerWrite:    c.MaximumUpdatesPerWrite,
		MaximumAPIDepth:       c.DispatchMaxDepth,
	}

	healthManager := health.NewHealthManager(dispatcher, ds)
	grpcServer, err := c.GRPCServer.Complete(zerolog.InfoLevel,
		func(server *grpc.Server) {
			services.RegisterGrpcServices(
				server,
				healthManager,
				dispatcher,
				prefixRequiredOption,
				v1SchemaServiceOption,
				watchServiceOption,
				permSysConfig,
			)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC server: %w", err)
	}

	// Configure the gateway to serve HTTP
	if len(c.HTTPGatewayUpstreamAddr) == 0 {
		c.HTTPGatewayUpstreamAddr = c.GRPCServer.Address
	} else {
		log.Info().Str("upstream", c.HTTPGatewayUpstreamAddr).Msg("Overriding REST gateway upstream")
	}

	if len(c.HTTPGatewayUpstreamTLSCertPath) == 0 {
		c.HTTPGatewayUpstreamTLSCertPath = c.GRPCServer.TLSCertPath
	} else {
		log.Info().Str("cert-path", c.HTTPGatewayUpstreamTLSCertPath).Msg("Overriding REST gateway upstream TLS")
	}

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

	if c.HTTPGateway.Enabled {
		log.Info().Str("upstream", c.HTTPGatewayUpstreamAddr).Msg("starting REST gateway")
	}

	gatewayServer, err := c.HTTPGateway.Complete(zerolog.InfoLevel, gatewayHandler)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize rest gateway: %w", err)
	}

	dashboardServer, err := c.DashboardAPI.Complete(zerolog.InfoLevel, dashboard.NewHandler(
		c.GRPCServer.Address,
		c.GRPCServer.TLSKeyPath != "" || c.GRPCServer.TLSCertPath != "",
		c.DatastoreConfig.Engine,
		ds,
	))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize dashboard server: %w", err)
	}

	registry, err := telemetry.RegisterTelemetryCollector(c.DatastoreConfig.Engine, ds)
	if err != nil {
		log.Warn().Err(err).Msg("unable to initialize telemetry collector")
	}

	reporter := telemetry.DisabledReporter
	if c.SilentlyDisableTelemetry {
		reporter = telemetry.SilentlyDisabledReporter
	} else if c.TelemetryEndpoint != "" && c.DatastoreConfig.DisableStats {
		reporter = telemetry.DisabledReporter
	} else if c.TelemetryEndpoint != "" && registry != nil {
		var err error
		reporter, err = telemetry.RemoteReporter(
			registry, c.TelemetryEndpoint, c.TelemetryCAOverridePath, c.TelemetryInterval,
		)
		if err != nil {
			return nil, fmt.Errorf("unable to initialize metrics reporter: %w", err)
		}
	}

	metricsServer, err := c.MetricsAPI.Complete(zerolog.InfoLevel, MetricsHandler(registry))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metrics server: %w", err)
	}

	return &completedServerConfig{
		gRPCServer:          grpcServer,
		dispatchGRPCServer:  dispatchGrpcServer,
		gatewayServer:       gatewayServer,
		metricsServer:       metricsServer,
		dashboardServer:     dashboardServer,
		unaryMiddleware:     c.UnaryMiddleware,
		streamingMiddleware: c.StreamingMiddleware,
		presharedKeys:       c.PresharedKey,
		telemetryReporter:   reporter,
		healthManager:       healthManager,
		closeFunc: func() {
			if err := ds.Close(); err != nil {
				log.Warn().Err(err).Msg("couldn't close datastore")
			}
			if err := dispatcher.Close(); err != nil {
				log.Warn().Err(err).Msg("couldn't close dispatcher")
			}
			if cachingClusterDispatch == nil {
				return
			}
			if err := cachingClusterDispatch.Close(); err != nil {
				log.Warn().Err(err).Msg("couldn't close cluster dispatcher")
			}
		},
	}, nil
}

// RunnableServer is a spicedb service set ready to run
type RunnableServer interface {
	Run(ctx context.Context) error
	Middleware() ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor)
	SetMiddleware(unaryInterceptors []grpc.UnaryServerInterceptor, streamingInterceptors []grpc.StreamServerInterceptor) RunnableServer
	GRPCDialContext(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error)
	DispatchNetDialContext(ctx context.Context, s string) (net.Conn, error)
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
	telemetryReporter  telemetry.Reporter
	healthManager      health.Manager

	unaryMiddleware     []grpc.UnaryServerInterceptor
	streamingMiddleware []grpc.StreamServerInterceptor
	presharedKeys       []string
	closeFunc           func()
}

func (c *completedServerConfig) Middleware() ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
	return c.unaryMiddleware, c.streamingMiddleware
}

func (c *completedServerConfig) SetMiddleware(unaryInterceptors []grpc.UnaryServerInterceptor, streamingInterceptors []grpc.StreamServerInterceptor) RunnableServer {
	c.unaryMiddleware = unaryInterceptors
	c.streamingMiddleware = streamingInterceptors
	return c
}

func (c *completedServerConfig) GRPCDialContext(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if len(c.presharedKeys) == 0 {
		return c.gRPCServer.DialContext(ctx, opts...)
	}
	if c.gRPCServer.Insecure() {
		opts = append(opts, grpcutil.WithInsecureBearerToken(c.presharedKeys[0]))
	} else {
		opts = append(opts, grpcutil.WithBearerToken(c.presharedKeys[0]))
	}
	return c.gRPCServer.DialContext(ctx, opts...)
}

func (c *completedServerConfig) DispatchNetDialContext(ctx context.Context, s string) (net.Conn, error) {
	return c.dispatchGRPCServer.NetDialContext(ctx, s)
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

	grpcServer := c.gRPCServer.WithOpts(grpc.ChainUnaryInterceptor(c.unaryMiddleware...), grpc.ChainStreamInterceptor(c.streamingMiddleware...))
	g.Go(c.healthManager.Checker(ctx))
	g.Go(grpcServer.Listen(ctx))
	g.Go(stopOnCancel(grpcServer.GracefulStop))

	g.Go(c.dispatchGRPCServer.Listen(ctx))
	g.Go(stopOnCancel(c.dispatchGRPCServer.GracefulStop))

	g.Go(c.gatewayServer.ListenAndServe)
	g.Go(stopOnCancel(c.gatewayServer.Close))

	g.Go(c.metricsServer.ListenAndServe)
	g.Go(stopOnCancel(c.metricsServer.Close))

	g.Go(c.dashboardServer.ListenAndServe)
	g.Go(stopOnCancel(c.dashboardServer.Close))

	g.Go(func() error { return c.telemetryReporter(ctx) })

	g.Go(stopOnCancel(c.closeFunc))

	if err := g.Wait(); err != nil {
		log.Warn().Err(err).Msg("error shutting down servers")
	}

	return nil
}

var promOnce sync.Once

// enableGRPCHistogram enables the standard time history for gRPC requests,
// ensuring that it is only enabled once
func enableGRPCHistogram() {
	// EnableHandlingTimeHistogram is not thread safe and only needs to happen
	// once
	promOnce.Do(func() {
		grpcprom.EnableHandlingTimeHistogram(grpcprom.WithHistogramBuckets(
			[]float64{.006, .010, .018, .024, .032, .042, .056, .075, .100, .178, .316, .562, 1.000},
		))
	})
}
