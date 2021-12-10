package serve

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpczerolog "github.com/grpc-ecosystem/go-grpc-middleware/providers/zerolog/v2"
	grpclog "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/auth"
	"github.com/authzed/spicedb/internal/dashboard"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	combineddispatch "github.com/authzed/spicedb/internal/dispatch/combined"
	"github.com/authzed/spicedb/internal/gateway"
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services"
	v1alpha1svc "github.com/authzed/spicedb/internal/services/v1alpha1"
	cmdutil "github.com/authzed/spicedb/pkg/cmd"
	logmw "github.com/authzed/spicedb/pkg/middleware/logging"
	"github.com/authzed/spicedb/pkg/middleware/requestid"
	"github.com/authzed/spicedb/pkg/validationfile"
)

func RegisterServeFlags(cmd *cobra.Command, dsConfig *cmdutil.DatastoreConfig) {
	// Flags for the gRPC API server
	cobrautil.RegisterGrpcServerFlags(cmd.Flags(), "grpc", "gRPC", ":50051", true)
	cmd.Flags().String("grpc-preshared-key", "", "preshared key to require for authenticated requests")
	cmd.Flags().Duration("grpc-shutdown-grace-period", 0*time.Second, "amount of time after receiving sigint to continue serving")
	if err := cmd.MarkFlagRequired("grpc-preshared-key"); err != nil {
		panic("failed to mark flag as required: " + err.Error())
	}

	// Flags for the datastore
	cmdutil.RegisterDatastoreFlags(cmd, dsConfig)
	cmd.Flags().Bool("datastore-readonly", false, "set the service to read-only mode")
	cmd.Flags().StringSlice("datastore-bootstrap-files", []string{}, "bootstrap data yaml files to load")
	cmd.Flags().Bool("datastore-bootstrap-overwrite", false, "overwrite any existing data with bootstrap data")

	cmd.Flags().Bool("datastore-request-hedging", true, "enable request hedging")
	cmd.Flags().Duration("datastore-request-hedging-initial-slow-value", 10*time.Millisecond, "initial value to use for slow datastore requests, before statistics have been collected")
	cmd.Flags().Uint64("datastore-request-hedging-max-requests", 1_000_000, "maximum number of historical requests to consider")
	cmd.Flags().Float64("datastore-request-hedging-quantile", 0.95, "quantile of historical datastore request time over which a request will be considered slow")

	// Flags for the namespace manager
	cmd.Flags().Duration("ns-cache-expiration", 1*time.Minute, "amount of time a namespace entry should remain cached")

	// Flags for parsing and validating schemas.
	cmd.Flags().Bool("schema-prefixes-required", false, "require prefixes on all object definitions in schemas")

	// Flags for HTTP gateway
	cobrautil.RegisterHttpServerFlags(cmd.Flags(), "http", "http", ":8443", false)

	// Flags for configuring the dispatch server
	cobrautil.RegisterGrpcServerFlags(cmd.Flags(), "dispatch-cluster", "dispatch", ":50053", false)

	// Flags for configuring dispatch requests
	cmd.Flags().Uint32("dispatch-max-depth", 50, "maximum recursion depth for nested calls")
	cmd.Flags().String("dispatch-upstream-addr", "", "upstream grpc address to dispatch to")
	cmd.Flags().String("dispatch-upstream-ca-path", "", "local path to the TLS CA used when connecting to the dispatch cluster")

	// Flags for configuring API behavior
	cmd.Flags().Bool("disable-v1-schema-api", false, "disables the V1 schema API")

	// Flags for misc services
	cobrautil.RegisterHttpServerFlags(cmd.Flags(), "dashboard", "dashboard", ":8080", true)
	cobrautil.RegisterHttpServerFlags(cmd.Flags(), "metrics", "metrics", ":9090", true)
}

func NewServeCommand(programName string, dsConfig *cmdutil.DatastoreConfig) *cobra.Command {
	return &cobra.Command{
		Use:     "serve",
		Short:   "serve the permissions database",
		Long:    "A database that stores, computes, and validates application permissions",
		PreRunE: cmdutil.DefaultPreRunE(programName),
		Run: func(cmd *cobra.Command, args []string) {
			serveRun(cmd, args, dsConfig)
		},
		Example: cmdutil.ServeExample(programName),
	}
}

func serveRun(cmd *cobra.Command, args []string, datastoreOpts *cmdutil.DatastoreConfig) {
	token := cobrautil.MustGetStringExpanded(cmd, "grpc-preshared-key")
	if len(token) < 1 {
		log.Fatal().Msg("a preshared key must be provided via --grpc-preshared-key to authenticate API requests")
	}

	ds, err := cmdutil.NewDatastore(datastoreOpts.ToOption())
	if err != nil {
		log.Fatal().Err(err).Msg("failed to init datastore")
	}

	bootstrapFilePaths := cobrautil.MustGetStringSlice(cmd, "datastore-bootstrap-files")
	if len(bootstrapFilePaths) > 0 {
		bootstrapOverwrite := cobrautil.MustGetBool(cmd, "datastore-bootstrap-overwrite")
		revision, err := ds.SyncRevision(context.Background())
		if err != nil {
			log.Fatal().Err(err).Msg("unable to determine datastore state before applying bootstrap data")
		}

		nsDefs, err := ds.ListNamespaces(context.Background(), revision)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to determine datastore state before applying bootstrap data")
		}
		if bootstrapOverwrite || len(nsDefs) == 0 {
			log.Info().Msg("initializing datastore from bootstrap files")
			_, _, err = validationfile.PopulateFromFiles(ds, bootstrapFilePaths)
			if err != nil {
				log.Fatal().Err(err).Msg("failed to load bootstrap files")
			}
		} else {
			log.Fatal().Err(err).Msg("cannot apply bootstrap data: schema or tuples already exist in the datastore. Delete existing data or set the flag --datastore-bootstrap-overwrite=true")
		}
	}

	if cobrautil.MustGetBool(cmd, "datastore-request-hedging") {
		initialSlowRequest := cobrautil.MustGetDuration(cmd, "datastore-request-hedging-initial-slow-value")
		maxRequests := cobrautil.MustGetUint64(cmd, "datastore-request-hedging-max-requests")
		hedgingQuantile := cobrautil.MustGetFloat64(cmd, "datastore-request-hedging-quantile")

		log.Info().
			Stringer("initialSlowRequest", initialSlowRequest).
			Uint64("maxRequests", maxRequests).
			Float64("hedgingQuantile", hedgingQuantile).
			Msg("request hedging enabled")

		ds = proxy.NewHedgingProxy(
			ds,
			initialSlowRequest,
			maxRequests,
			hedgingQuantile,
		)
	}

	if cobrautil.MustGetBool(cmd, "datastore-readonly") {
		log.Warn().Msg("setting the service to read-only")
		ds = proxy.NewReadonlyDatastore(ds)
	}

	nsCacheExpiration := cobrautil.MustGetDuration(cmd, "ns-cache-expiration")
	nsm, err := namespace.NewCachingNamespaceManager(ds, nsCacheExpiration, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize namespace manager")
	}

	grpcprom.EnableHandlingTimeHistogram(grpcprom.WithHistogramBuckets(
		[]float64{.006, .010, .018, .024, .032, .042, .056, .075, .100, .178, .316, .562, 1.000},
	))

	middleware := grpc.ChainUnaryInterceptor(
		requestid.UnaryServerInterceptor(requestid.GenerateIfMissing(true)),
		logmw.UnaryServerInterceptor(logmw.ExtractMetadataField("x-request-id", "requestID")),
		grpclog.UnaryServerInterceptor(grpczerolog.InterceptorLogger(log.Logger)),
		otelgrpc.UnaryServerInterceptor(),
		grpcauth.UnaryServerInterceptor(auth.RequirePresharedKey(token)),
		grpcprom.UnaryServerInterceptor,
		servicespecific.UnaryServerInterceptor,
	)

	streamMiddleware := grpc.ChainStreamInterceptor(
		requestid.StreamServerInterceptor(requestid.GenerateIfMissing(true)),
		logmw.StreamServerInterceptor(logmw.ExtractMetadataField("x-request-id", "requestID")),
		grpclog.StreamServerInterceptor(grpczerolog.InterceptorLogger(log.Logger)),
		otelgrpc.StreamServerInterceptor(),
		grpcauth.StreamServerInterceptor(auth.RequirePresharedKey(token)),
		grpcprom.StreamServerInterceptor,
		servicespecific.StreamServerInterceptor,
	)

	grpcServer, err := cobrautil.GrpcServerFromFlags(cmd, "grpc", middleware, streamMiddleware)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create gRPC server")
	}

	dispatchGrpcServer, err := cobrautil.GrpcServerFromFlags(cmd, "dispatch-cluster", middleware, streamMiddleware)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create redispatch gRPC server")
	}

	redispatch, err := combineddispatch.NewDispatcher(nsm, ds, dispatchGrpcServer,
		combineddispatch.UpstreamAddr(cobrautil.MustGetStringExpanded(cmd, "dispatch-upstream-addr")),
		combineddispatch.UpstreamCAPath(cobrautil.MustGetStringExpanded(cmd, "dispatch-upstream-ca-path")),
		combineddispatch.GrpcPresharedKey(cobrautil.MustGetStringExpanded(cmd, "grpc-preshared-key")),
		combineddispatch.GrpcDialOpts(
			grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
			grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"consistent-hashring"}`),
		),
	)
	if err != nil {
		log.Fatal().Err(err).Msg("failed when configuring dispatch")
	}

	prefixRequiredOption := v1alpha1svc.PrefixRequired
	if !cobrautil.MustGetBool(cmd, "schema-prefixes-required") {
		prefixRequiredOption = v1alpha1svc.PrefixNotRequired
	}

	v1SchemaServiceOption := services.V1SchemaServiceEnabled
	if cobrautil.MustGetBool(cmd, "disable-v1-schema-api") {
		v1SchemaServiceOption = services.V1SchemaServiceDisabled
	}

	services.RegisterGrpcServices(
		grpcServer,
		ds,
		nsm,
		redispatch,
		cobrautil.MustGetUint32(cmd, "dispatch-max-depth"),
		prefixRequiredOption,
		v1SchemaServiceOption,
	)
	go func() {
		if err := cobrautil.GrpcListenFromFlags(cmd, "grpc", grpcServer, zerolog.InfoLevel); err != nil {
			log.Fatal().Err(err).Msg("failed to start gRPC server")
		}
	}()

	go func() {
		if err := cobrautil.GrpcListenFromFlags(cmd, "dispatch-cluster", dispatchGrpcServer, zerolog.InfoLevel); err != nil {
			log.Fatal().Err(err).Msg("failed to start gRPC server")
		}
	}()

	// Start the REST gateway to serve HTTP/JSON.
	gatewayHandler, err := gateway.NewHandler(
		context.TODO(),
		cobrautil.MustGetStringExpanded(cmd, "grpc-addr"),
		cobrautil.MustGetStringExpanded(cmd, "grpc-tls-cert-path"),
	)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize rest gateway")
	}
	gatewaySrv := cobrautil.HttpServerFromFlags(cmd, "http")
	gatewaySrv.Handler = gatewayHandler
	go func() {
		if err := cobrautil.HttpListenFromFlags(cmd, "http", gatewaySrv, zerolog.InfoLevel); err != nil {
			log.Fatal().Err(err).Msg("failed while serving http")
		}
	}()

	// Start the metrics endpoint.
	metricsSrv := cobrautil.HttpServerFromFlags(cmd, "metrics")
	metricsSrv.Handler = cmdutil.MetricsHandler()
	go func() {
		if err := cobrautil.HttpListenFromFlags(cmd, "metrics", metricsSrv, zerolog.InfoLevel); err != nil {
			log.Fatal().Err(err).Msg("failed while serving metrics")
		}
	}()

	// Start a dashboard.
	dashboardSrv := cobrautil.HttpServerFromFlags(cmd, "dashboard")
	dashboardSrv.Handler = dashboard.NewHandler(
		cobrautil.MustGetStringExpanded(cmd, "grpc-addr"),
		cobrautil.MustGetStringExpanded(cmd, "grpc-tls-cert-path") != "" && cobrautil.MustGetStringExpanded(cmd, "grpc-tls-key-path") != "",
		datastoreOpts.Engine,
		ds,
	)
	go func() {
		if err := cobrautil.HttpListenFromFlags(cmd, "dashboard", dashboardSrv, zerolog.InfoLevel); err != nil {
			log.Fatal().Err(err).Msg("failed while serving dashboard")
		}
	}()

	signalctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	gracePeriod := cobrautil.MustGetDuration(cmd, "grpc-shutdown-grace-period")

	<-signalctx.Done()
	log.Info().Msg("received interrupt")

	if gracePeriod > 0 {
		interruptGrace, _ := signal.NotifyContext(context.Background(), os.Interrupt)
		graceTimer := time.NewTimer(gracePeriod)

		log.Info().Stringer("timeout", gracePeriod).Msg("starting shutdown grace period")

		select {
		case <-graceTimer.C:
		case <-interruptGrace.Done():
			log.Warn().Msg("interrupted shutdown grace period")
		}
	}

	log.Info().Msg("shutting down")
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
}
