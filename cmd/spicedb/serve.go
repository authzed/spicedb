package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fatih/color"
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
	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	combineddispatch "github.com/authzed/spicedb/internal/dispatch/combined"
	"github.com/authzed/spicedb/internal/gateway"
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services"
	v1alpha1svc "github.com/authzed/spicedb/internal/services/v1alpha1"
	"github.com/authzed/spicedb/pkg/cmd/serve"
	logmw "github.com/authzed/spicedb/pkg/middleware/logging"
	"github.com/authzed/spicedb/pkg/middleware/requestid"
	"github.com/authzed/spicedb/pkg/validationfile"
)

func registerServeCmd(rootCmd *cobra.Command) {
	serveCmd := &cobra.Command{
		Use:     "serve",
		Short:   "serve the permissions database",
		Long:    "A database that stores, computes, and validates application permissions",
		PreRunE: defaultPreRunE,
		Run:     serveRun,
		Example: fmt.Sprintf(`	%s:
		spicedb serve --grpc-preshared-key "somerandomkeyhere"

	%s:
		spicedb serve --grpc-preshared-key "realkeyhere" --grpc-tls-cert-path path/to/tls/cert --grpc-tls-key-path path/to/tls/key \
			--http-tls-cert-path path/to/tls/cert --http-tls-key-path path/to/tls/key \
			--datastore-engine postgres --datastore-conn-uri "postgres-connection-string-here"
`, color.YellowString("No TLS and in-memory"), color.GreenString("TLS and a real datastore")),
	}

	// Flags for the gRPC API server
	cobrautil.RegisterGrpcServerFlags(serveCmd.Flags(), "grpc", "gRPC", ":50051", true)
	serveCmd.Flags().String("grpc-preshared-key", "", "preshared key to require for authenticated requests")
	serveCmd.Flags().Duration("grpc-shutdown-grace-period", 0*time.Second, "amount of time after receiving sigint to continue serving")
	if err := serveCmd.MarkFlagRequired("grpc-preshared-key"); err != nil {
		panic("failed to mark flag as required: " + err.Error())
	}

	// Flags for the datastore
	var datastoreOptions serve.Options
	serve.RegisterDatastoreFlags(serveCmd, &datastoreOptions)
	serveCmd.Flags().Bool("datastore-readonly", false, "set the service to read-only mode")
	serveCmd.Flags().StringSlice("datastore-bootstrap-files", []string{}, "bootstrap data yaml files to load")
	serveCmd.Flags().Bool("datastore-bootstrap-overwrite", false, "overwrite any existing data with bootstrap data")

	serveCmd.Flags().Bool("datastore-request-hedging", true, "enable request hedging")
	serveCmd.Flags().Duration("datastore-request-hedging-initial-slow-value", 10*time.Millisecond, "initial value to use for slow datastore requests, before statistics have been collected")
	serveCmd.Flags().Uint64("datastore-request-hedging-max-requests", 1_000_000, "maximum number of historical requests to consider")
	serveCmd.Flags().Float64("datastore-request-hedging-quantile", 0.95, "quantile of historical datastore request time over which a request will be considered slow")

	// Flags for the namespace manager
	serveCmd.Flags().Duration("ns-cache-expiration", 1*time.Minute, "amount of time a namespace entry should remain cached")

	// Flags for parsing and validating schemas.
	serveCmd.Flags().Bool("schema-prefixes-required", false, "require prefixes on all object definitions in schemas")

	// Flags for HTTP gateway
	cobrautil.RegisterHttpServerFlags(serveCmd.Flags(), "http", "http", ":8443", false)

	// Flags for configuring the dispatch server
	cobrautil.RegisterGrpcServerFlags(serveCmd.Flags(), "dispatch-cluster", "dispatch", ":50053", false)

	// Flags for configuring dispatch requests
	serveCmd.Flags().Uint32("dispatch-max-depth", 50, "maximum recursion depth for nested calls")
	serveCmd.Flags().String("dispatch-upstream-addr", "", "upstream grpc address to dispatch to")
	serveCmd.Flags().String("dispatch-upstream-ca-path", "", "local path to the TLS CA used when connecting to the dispatch cluster")

	// Flags for configuring API behavior
	serveCmd.Flags().Bool("disable-v1-schema-api", false, "disables the V1 schema API")

	// Flags for misc services
	cobrautil.RegisterHttpServerFlags(serveCmd.Flags(), "dashboard", "dashboard", ":8080", true)
	cobrautil.RegisterHttpServerFlags(serveCmd.Flags(), "metrics", "metrics", ":9090", true)

	// Required flags.

	rootCmd.AddCommand(serveCmd)
}

func serveRun(cmd *cobra.Command, args []string) {
	token := cobrautil.MustGetStringExpanded(cmd, "grpc-preshared-key")
	if len(token) < 1 {
		log.Fatal().Msg("a preshared key must be provided via --grpc-preshared-key to authenticate API requests")
	}

	datastoreEngine := cobrautil.MustGetStringExpanded(cmd, "datastore-engine")
	ds, err := serve.NewDatastore(
		datastore.Engine(datastoreEngine),
		serve.WithRevisionQuantization(cobrautil.MustGetDuration(cmd, "datastore-revision-fuzzing-duration")),
		serve.WithGCWindow(cobrautil.MustGetDuration(cmd, "datastore-gc-window")),
		serve.WithURI(cobrautil.MustGetStringExpanded(cmd, "datastore-conn-uri")),
		serve.WithMaxIdleTime(cobrautil.MustGetDuration(cmd, "datastore-conn-max-idletime")),
		serve.WithMaxLifetime(cobrautil.MustGetDuration(cmd, "datastore-conn-max-lifetime")),
		serve.WithMaxOpenConns(cobrautil.MustGetInt(cmd, "datastore-conn-max-open")),
		serve.WithMinOpenConns(cobrautil.MustGetInt(cmd, "datastore-conn-min-open")),
		serve.WithSplitQuerySize(cobrautil.MustGetStringExpanded(cmd, "datastore-query-split-size")),
		serve.WithFollowerReadDelay(cobrautil.MustGetDuration(cmd, "datastore-follower-read-delay-duration")),
		serve.WithMaxRetries(cobrautil.MustGetInt(cmd, "datastore-max-tx-retries")),
		serve.WithOverlapKey(cobrautil.MustGetStringExpanded(cmd, "datastore-tx-overlap-key")),
		serve.WithOverlapStrategy(cobrautil.MustGetStringExpanded(cmd, "datastore-tx-overlap-strategy")),
		serve.WithHealthCheckPeriod(cobrautil.MustGetDuration(cmd, "datastore-conn-healthcheck-interval")),
		serve.WithGCInterval(cobrautil.MustGetDuration(cmd, "datastore-gc-interval")),
		serve.WithGCMaxOperationTime(cobrautil.MustGetDuration(cmd, "datastore-gc-max-operation-time")),
	)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to init datastore")
	}

	bootstrapFilePaths := cobrautil.MustGetStringSlice(cmd, "datastore-bootstrap-files")
	if len(bootstrapFilePaths) > 0 {
		bootstrapOverwrite := cobrautil.MustGetBool(cmd, "datastore-bootstrap-overwrite")
		nsDefs, err := ds.ListNamespaces(context.Background())
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
	metricsSrv.Handler = metricsHandler()
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
		datastoreEngine,
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
