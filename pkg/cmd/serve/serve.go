package serve

import (
	"context"
	"errors"
	"net/http"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/rs/cors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/dashboard"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	combineddispatch "github.com/authzed/spicedb/internal/dispatch/combined"
	"github.com/authzed/spicedb/internal/gateway"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services"
	v1alpha1svc "github.com/authzed/spicedb/internal/services/v1alpha1"
	cmdutil "github.com/authzed/spicedb/pkg/cmd"
	"github.com/authzed/spicedb/pkg/validationfile"
)

const PresharedKeyFlag = "grpc-preshared-key"

func RegisterServeFlags(cmd *cobra.Command, config *cmdutil.ServerConfig) {
	// Flags for the gRPC API server
	cmdutil.RegisterGRPCServerFlags(cmd.Flags(), &config.GRPCServer, "grpc", "gRPC", ":50051", true)
	cmd.Flags().StringVar(&config.PresharedKey, PresharedKeyFlag, "", "preshared key to require for authenticated requests")
	cmd.Flags().DurationVar(&config.ShutdownGracePeriod, "grpc-shutdown-grace-period", 0*time.Second, "amount of time after receiving sigint to continue serving")
	if err := cmd.MarkFlagRequired(PresharedKeyFlag); err != nil {
		panic("failed to mark flag as required: " + err.Error())
	}

	// Flags for the datastore
	cmdutil.RegisterDatastoreFlags(cmd, &config.Datastore)
	cmd.Flags().BoolVar(&config.ReadOnly, "datastore-readonly", false, "set the service to read-only mode")
	cmd.Flags().StringSliceVar(&config.BootstrapFiles, "datastore-bootstrap-files", []string{}, "bootstrap data yaml files to load")
	cmd.Flags().BoolVar(&config.BootstrapOverwrite, "datastore-bootstrap-overwrite", false, "overwrite any existing data with bootstrap data")

	cmd.Flags().BoolVar(&config.RequestHedgingEnabled, "datastore-request-hedging", true, "enable request hedging")
	cmd.Flags().DurationVar(&config.RequestHedgingInitialSlowValue, "datastore-request-hedging-initial-slow-value", 10*time.Millisecond, "initial value to use for slow datastore requests, before statistics have been collected")
	cmd.Flags().Uint64Var(&config.RequestHedgingMaxRequests, "datastore-request-hedging-max-requests", 1_000_000, "maximum number of historical requests to consider")
	cmd.Flags().Float64Var(&config.RequestHedgingQuantile, "datastore-request-hedging-quantile", 0.95, "quantile of historical datastore request time over which a request will be considered slow")

	// Flags for the namespace manager
	cmd.Flags().DurationVar(&config.NamespaceCacheExpiration, "ns-cache-expiration", 1*time.Minute, "amount of time a namespace entry should remain cached")

	// Flags for parsing and validating schemas.
	cmd.Flags().BoolVar(&config.SchemaPrefixesRequired, "schema-prefixes-required", false, "require prefixes on all object definitions in schemas")

	// Flags for HTTP gateway
	cmdutil.RegisterHTTPServerFlags(cmd.Flags(), &config.HTTPGateway, "http", "http", ":8443", false)
	cmd.Flags().StringVar(&config.HTTPGatewayUpstreamAddr, "http-upstream-override-addr", "", "Override the upstream to point to a different gRPC server")
	if err := cmd.Flags().MarkHidden("http-upstream-override-addr"); err != nil {
		panic("failed to mark flag as hidden: " + err.Error())
	}
	cmd.Flags().StringVar(&config.HTTPGatewayUpstreamTLSCertPath, "http-upstream-override-tls-cert-path", "", "Override the upstream TLS certificate")
	if err := cmd.Flags().MarkHidden("http-upstream-override-tls-cert-path"); err != nil {
		panic("failed to mark flag as hidden: " + err.Error())
	}
	cmd.Flags().BoolVar(&config.HTTPGatewayCorsEnabled, "http-cors-enabled", false, "DANGEROUS: Enable CORS on the http gateway")
	if err := cmd.Flags().MarkHidden("http-cors-enabled"); err != nil {
		panic("failed to mark flag as hidden: " + err.Error())
	}
	cmd.Flags().StringSliceVar(&config.HTTPGatewayCorsAllowedOrigins, "http-cors-allowed-origins", []string{"*"}, "Set CORS allowed origins for http gateway, defaults to all origins")
	if err := cmd.Flags().MarkHidden("http-cors-allowed-origins"); err != nil {
		panic("failed to mark flag as hidden: " + err.Error())
	}

	// Flags for configuring the dispatch server
	cmdutil.RegisterGRPCServerFlags(cmd.Flags(), &config.DispatchServer, "dispatch-cluster", "dispatch", ":50053", false)

	// Flags for configuring dispatch requests
	cmd.Flags().Uint32Var(&config.DispatchMaxDepth, "dispatch-max-depth", 50, "maximum recursion depth for nested calls")
	cmd.Flags().StringVar(&config.DispatchUpstreamAddr, "dispatch-upstream-addr", "", "upstream grpc address to dispatch to")
	cmd.Flags().StringVar(&config.DispatchUpstreamCAPath, "dispatch-upstream-ca-path", "", "local path to the TLS CA used when connecting to the dispatch cluster")

	// Flags for configuring API behavior
	cmd.Flags().BoolVar(&config.DisableV1SchemaAPI, "disable-v1-schema-api", false, "disables the V1 schema API")

	// Flags for misc services
	cmdutil.RegisterHTTPServerFlags(cmd.Flags(), &config.DashboardAPI, "dashboard", "dashboard", ":8080", true)
	cmdutil.RegisterHTTPServerFlags(cmd.Flags(), &config.MetricsAPI, "metrics", "metrics", ":9090", true)
}

func NewServeCommand(programName string, config *cmdutil.ServerConfig) *cobra.Command {
	return &cobra.Command{
		Use:     "serve",
		Short:   "serve the permissions database",
		Long:    "A database that stores, computes, and validates application permissions",
		PreRunE: cmdutil.DefaultPreRunE(programName),
		RunE: func(cmd *cobra.Command, args []string) error {
			signalctx := cmdutil.SignalContextWithGracePeriod(
				context.Background(),
				config.ShutdownGracePeriod,
			)
			return Serve(signalctx, config)
		},
		Example: cmdutil.ServeExample(programName),
	}
}

func Serve(ctx context.Context, config *cmdutil.ServerConfig) error {
	if len(config.PresharedKey) < 1 {
		return errors.New("a preshared key must be provided to authenticate API requests")
	}

	ds, err := cmdutil.NewDatastore(config.Datastore.ToOption())
	if err != nil {
		log.Fatal().Err(err).Msg("failed to init datastore")
	}

	if len(config.BootstrapFiles) > 0 {
		revision, err := ds.HeadRevision(context.Background())
		if err != nil {
			log.Fatal().Err(err).Msg("unable to determine datastore state before applying bootstrap data")
		}

		nsDefs, err := ds.ListNamespaces(context.Background(), revision)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to determine datastore state before applying bootstrap data")
		}
		if config.BootstrapOverwrite || len(nsDefs) == 0 {
			log.Info().Msg("initializing datastore from bootstrap files")
			_, _, err = validationfile.PopulateFromFiles(ds, config.BootstrapFiles)
			if err != nil {
				log.Fatal().Err(err).Msg("failed to load bootstrap files")
			}
		} else {
			return errors.New("cannot apply bootstrap data: schema or tuples already exist in the datastore. Delete existing data or set the flag --datastore-bootstrap-overwrite=true")
		}
	}

	if config.RequestHedgingEnabled {
		log.Info().
			Stringer("initialSlowRequest", config.RequestHedgingInitialSlowValue).
			Uint64("maxRequests", config.RequestHedgingMaxRequests).
			Float64("hedgingQuantile", config.RequestHedgingQuantile).
			Msg("request hedging enabled")

		ds = proxy.NewHedgingProxy(
			ds,
			config.RequestHedgingInitialSlowValue,
			config.RequestHedgingMaxRequests,
			config.RequestHedgingQuantile,
		)
	}

	if config.ReadOnly {
		log.Warn().Msg("setting the service to read-only")
		ds = proxy.NewReadonlyDatastore(ds)
	}

	nsm, err := namespace.NewCachingNamespaceManager(ds, config.NamespaceCacheExpiration, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize namespace manager")
	}

	grpcprom.EnableHandlingTimeHistogram(grpcprom.WithHistogramBuckets(
		[]float64{.006, .010, .018, .024, .032, .042, .056, .075, .100, .178, .316, .562, 1.000},
	))

	if len(config.UnaryMiddleware) == 0 || len(config.StreamingMiddleware) == 0 {
		config.UnaryMiddleware, config.StreamingMiddleware = cmdutil.DefaultMiddleware(log.Logger, config.PresharedKey)
	}

	grpcServer, err := config.GRPCServer.Server(grpc.ChainUnaryInterceptor(config.UnaryMiddleware...), grpc.ChainStreamInterceptor(config.StreamingMiddleware...))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create gRPC server")
	}
	dispatchGrpcServer, err := config.DispatchServer.Server(grpc.ChainUnaryInterceptor(config.UnaryMiddleware...), grpc.ChainStreamInterceptor(config.StreamingMiddleware...))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create redispatch gRPC server")
	}

	redispatch, err := combineddispatch.NewDispatcher(nsm, ds, dispatchGrpcServer,
		combineddispatch.UpstreamAddr(config.DispatchUpstreamAddr),
		combineddispatch.UpstreamCAPath(config.DispatchUpstreamCAPath),
		combineddispatch.GrpcPresharedKey(config.PresharedKey),
		combineddispatch.GrpcDialOpts(
			grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
			grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"consistent-hashring"}`),
		),
	)
	if err != nil {
		log.Fatal().Err(err).Msg("failed when configuring dispatch")
	}

	prefixRequiredOption := v1alpha1svc.PrefixRequired
	if !config.SchemaPrefixesRequired {
		prefixRequiredOption = v1alpha1svc.PrefixNotRequired
	}

	v1SchemaServiceOption := services.V1SchemaServiceEnabled
	if !config.DisableV1SchemaAPI {
		v1SchemaServiceOption = services.V1SchemaServiceDisabled
	}

	services.RegisterGrpcServices(
		grpcServer,
		ds,
		nsm,
		redispatch,
		config.DispatchMaxDepth,
		prefixRequiredOption,
		v1SchemaServiceOption,
	)
	go func() {
		if err := config.GRPCServer.Listen(grpcServer, zerolog.InfoLevel); err != nil {
			log.Fatal().Err(err).Msg("failed to start gRPC server")
		}
	}()

	go func() {
		if err := config.DispatchServer.Listen(dispatchGrpcServer, zerolog.InfoLevel); err != nil {
			log.Fatal().Err(err).Msg("failed to start gRPC server")
		}
	}()

	var (
		gatewaySrv   *http.Server
		metricsSrv   *http.Server
		dashboardSrv *http.Server
	)

	// Start the REST gateway to serve HTTP/JSON.
	if len(config.HTTPGatewayUpstreamAddr) == 0 {
		config.HTTPGatewayUpstreamAddr = config.GRPCServer.Address
	} else {
		log.Info().Str("upstream", config.HTTPGatewayUpstreamAddr).Msg("Overriding REST gateway upstream")
	}

	if len(config.HTTPGatewayUpstreamTLSCertPath) == 0 {
		config.HTTPGatewayUpstreamTLSCertPath = config.GRPCServer.TLSCertPath
	} else {
		log.Info().Str("cert-path", config.HTTPGatewayUpstreamTLSCertPath).Msg("Overriding REST gateway upstream TLS")
	}

	gatewayHandler, err := gateway.NewHandler(ctx, config.HTTPGatewayUpstreamAddr, config.HTTPGatewayUpstreamTLSCertPath)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize rest gateway")
	}

	if config.HTTPGatewayCorsEnabled {
		log.Info().Strs("origins", config.HTTPGatewayCorsAllowedOrigins).Msg("Setting REST gateway CORS policy")
		gatewayHandler = cors.New(cors.Options{
			AllowedOrigins:   config.HTTPGatewayCorsAllowedOrigins,
			AllowCredentials: true,
			AllowedHeaders:   []string{"Authorization", "Content-Type"},
			Debug:            log.Debug().Enabled(),
		}).Handler(gatewayHandler)
	}

	config.HTTPGateway.Handler = gatewayHandler

	go func() {
		gatewaySrv, err = config.HTTPGateway.ListenAndServe(zerolog.InfoLevel)
		if err != nil {
			log.Fatal().Err(err).Msg("failed while serving http")
		}
	}()

	// Start the metrics endpoint.
	config.MetricsAPI.Handler = cmdutil.MetricsHandler()
	go func() {
		metricsSrv, err = config.MetricsAPI.ListenAndServe(zerolog.InfoLevel)
		if err != nil {
			log.Fatal().Err(err).Msg("failed while serving metrics")
		}
	}()

	// Start a dashboard.
	config.DashboardAPI.Handler = dashboard.NewHandler(
		config.GRPCServer.Address,
		config.GRPCServer.TLSKeyPath != "" || config.GRPCServer.TLSCertPath != "",
		config.Datastore.Engine,
		ds,
	)
	go func() {
		dashboardSrv, err = config.DashboardAPI.ListenAndServe(zerolog.InfoLevel)
		if err != nil {
			log.Fatal().Err(err).Msg("failed while serving dashboard")
		}
	}()

	<-ctx.Done()
	grpcServer.GracefulStop()
	dispatchGrpcServer.GracefulStop()

	if err := gatewaySrv.Close(); err != nil {
		log.Fatal().Err(err).Msg("failed while shutting down rest gateway")
	}

	if err := nsm.Close(); err != nil {
		log.Fatal().Err(err).Msg("failed while shutting down namespace manager")
	}

	if err := redispatch.Close(); err != nil {
		log.Fatal().Err(err).Msg("failed while shutting down dispatcher")
	}

	if err := ds.Close(); err != nil {
		log.Fatal().Err(err).Msg("failed while shutting down datastore")
	}

	if err := metricsSrv.Close(); err != nil {
		log.Fatal().Err(err).Msg("failed while shutting down metrics server")
	}

	if err := dashboardSrv.Close(); err != nil {
		log.Fatal().Err(err).Msg("failed while shutting down dashboard")
	}

	return nil
}
