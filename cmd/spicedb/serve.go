package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alecthomas/units"
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
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/crdb"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/client/consistentbackend"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/dispatch/remote"
	"github.com/authzed/spicedb/internal/gateway"
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services"
	clusterdispatch "github.com/authzed/spicedb/internal/services/dispatch"
	v1alpha1svc "github.com/authzed/spicedb/internal/services/v1alpha1"
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
	serveCmd.Flags().String("datastore-engine", "memory", `type of datastore to initialize ("memory", "postgres", "cockroachdb")`)
	serveCmd.Flags().String("datastore-conn-uri", "", `connection string used by remote datastores (e.g. "postgres://postgres:password@localhost:5432/spicedb")`)
	serveCmd.Flags().Bool("datastore-readonly", false, "set the service to read-only mode")
	serveCmd.Flags().Int("datastore-conn-max-open", 20, "number of concurrent connections open in a remote datastore's connection pool")
	serveCmd.Flags().Int("datastore-conn-min-open", 10, "number of minimum concurrent connections open in a remote datastore's connection pool")
	serveCmd.Flags().Duration("datastore-conn-max-lifetime", 30*time.Minute, "maximum amount of time a connection can live in a remote datastore's connection pool")
	serveCmd.Flags().Duration("datastore-conn-max-idletime", 30*time.Minute, "maximum amount of time a connection can idle in a remote datastore's connection pool")
	serveCmd.Flags().Duration("datastore-conn-healthcheck-interval", 30*time.Second, "time between a remote datastore's connection pool health checks")
	serveCmd.Flags().Duration("datastore-gc-window", 24*time.Hour, "amount of time before revisions are garbage collected")
	serveCmd.Flags().Duration("datastore-gc-interval", 3*time.Minute, "amount of time between passes of garbage collection (postgres driver only)")
	serveCmd.Flags().Duration("datastore-gc-max-operation-time", 1*time.Minute, "maximum amount of time a garbage collection pass can operate before timing out (postgres driver only)")
	serveCmd.Flags().Duration("datastore-revision-fuzzing-duration", 5*time.Second, "amount of time to advertize stale revisions (cockroach driver only)")
	serveCmd.Flags().Duration("datastore-follower-read-delay-duration", 5*time.Second, "amount of time to use as a delay to enable follower reads")
	serveCmd.Flags().String("datastore-query-split-size", common.DefaultSplitAtEstimatedQuerySize.String(), "estimated number of bytes at which a query is split when using a remote datastore")
	serveCmd.Flags().StringSlice("datastore-bootstrap-files", []string{}, "bootstrap data yaml files to load")
	serveCmd.Flags().Bool("datastore-bootstrap-overwrite", false, "overwrite any existing data with bootstrap data")
	serveCmd.Flags().Int("datastore-max-tx-retries", 50, "number of times a retriable transaction should be retried (cockroach driver only)")
	serveCmd.Flags().String("datastore-tx-overlap-strategy", "static", `strategy to generate transaction overlap keys ("prefix", "static", "insecure") (cockroach driver only)`)
	serveCmd.Flags().String("datastore-tx-overlap-key", "key", "static key to touch when writing to ensure transactions overlap (only used if --datastore-tx-overlap-strategy=static is set; cockroach driver only)")

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

	// Flags for configuring dispatch behavior
	serveCmd.Flags().Uint32("dispatch-max-depth", 50, "maximum recursion depth for nested calls")
	cobrautil.RegisterGrpcServerFlags(serveCmd.Flags(), "dispatch-cluster", "dispatch", ":50053", false)
	serveCmd.Flags().String("dispatch-cluster-dns-name", "", "DNS SRV record name to resolve for cluster dispatch")
	serveCmd.Flags().String("dispatch-cluster-service-name", "grpc", "DNS SRV record service name to resolve for cluster dispatch")
	serveCmd.Flags().String("dispatch-peer-resolver-addr", "", "address used to connect to the peer endpoint resolver")
	serveCmd.Flags().String("dispatch-peer-resolver-tls-cert-path", "", "local path to the TLS certificate for the peer endpoint resolver")

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
	datastoreURI := cobrautil.MustGetStringExpanded(cmd, "datastore-conn-uri")

	revisionFuzzingTimedelta := cobrautil.MustGetDuration(cmd, "datastore-revision-fuzzing-duration")
	gcWindow := cobrautil.MustGetDuration(cmd, "datastore-gc-window")
	maxRetries := cobrautil.MustGetInt(cmd, "datastore-max-tx-retries")
	overlapKey := cobrautil.MustGetStringExpanded(cmd, "datastore-tx-overlap-key")
	overlapStrategy := cobrautil.MustGetStringExpanded(cmd, "datastore-tx-overlap-strategy")

	splitQuerySize, err := units.ParseBase2Bytes(cobrautil.MustGetStringExpanded(cmd, "datastore-query-split-size"))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to parse datastore-query-split-size")
	}

	var ds datastore.Datastore
	if datastoreEngine == "memory" {
		log.Info().Msg("using in-memory datastore")
		ds, err = memdb.NewMemdbDatastore(0, revisionFuzzingTimedelta, gcWindow, 0)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to init datastore")
		}
	} else if datastoreEngine == "cockroachdb" {
		log.Info().Msg("using cockroachdb datastore")
		ds, err = crdb.NewCRDBDatastore(
			datastoreURI,
			crdb.ConnMaxIdleTime(cobrautil.MustGetDuration(cmd, "datastore-conn-max-idletime")),
			crdb.ConnMaxLifetime(cobrautil.MustGetDuration(cmd, "datastore-conn-max-lifetime")),
			crdb.MaxOpenConns(cobrautil.MustGetInt(cmd, "datastore-conn-max-open")),
			crdb.MinOpenConns(cobrautil.MustGetInt(cmd, "datastore-conn-min-open")),
			crdb.RevisionQuantization(revisionFuzzingTimedelta),
			crdb.FollowerReadDelay(cobrautil.MustGetDuration(cmd, "datastore-follower-read-delay-duration")),
			crdb.GCWindow(gcWindow),
			crdb.MaxRetries(maxRetries),
			crdb.SplitAtEstimatedQuerySize(splitQuerySize),
			crdb.OverlapKey(overlapKey),
			crdb.OverlapStrategy(overlapStrategy),
		)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to init datastore")
		}
	} else if datastoreEngine == "postgres" {
		log.Info().Msg("using postgres datastore")
		ds, err = postgres.NewPostgresDatastore(
			datastoreURI,
			postgres.ConnMaxIdleTime(cobrautil.MustGetDuration(cmd, "datastore-conn-max-idletime")),
			postgres.ConnMaxLifetime(cobrautil.MustGetDuration(cmd, "datastore-conn-max-lifetime")),
			postgres.HealthCheckPeriod(cobrautil.MustGetDuration(cmd, "datastore-conn-healthcheck-interval")),
			postgres.MaxOpenConns(cobrautil.MustGetInt(cmd, "datastore-conn-max-open")),
			postgres.MinOpenConns(cobrautil.MustGetInt(cmd, "datastore-conn-min-open")),
			postgres.RevisionFuzzingTimedelta(revisionFuzzingTimedelta),
			postgres.GCInterval(cobrautil.MustGetDuration(cmd, "datastore-gc-interval")),
			postgres.GCMaxOperationTime(cobrautil.MustGetDuration(cmd, "datastore-gc-max-operation-time")),
			postgres.GCWindow(gcWindow),
			postgres.EnablePrometheusStats(),
			postgres.EnableTracing(),
			postgres.SplitAtEstimatedQuerySize(splitQuerySize),
		)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to init datastore")
		}
	} else {
		log.Fatal().Str("datastore-engine", datastoreEngine).Msg("unknown datastore engine type")
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

	redispatch := graph.NewLocalOnlyDispatcher(nsm, ds)
	redispatchClientCtx, redispatchClientCancel := context.WithCancel(context.Background())

	redispatchTarget := cobrautil.MustGetStringExpanded(cmd, "dispatch-cluster-dns-name")
	redispatchServiceName := cobrautil.MustGetStringExpanded(cmd, "dispatch-cluster-service-name")
	if redispatchTarget != "" {
		log.Info().Str("target", redispatchTarget).Msg("initializing remote redispatcher")

		resolverAddr := cobrautil.MustGetStringExpanded(cmd, "dispatch-peer-resolver-addr")
		resolverCertPath := cobrautil.MustGetStringExpanded(cmd, "dispatch-peer-resolver-tls-cert-path")
		var resolverConfig *consistentbackend.EndpointResolverConfig
		if resolverCertPath != "" {
			log.Debug().Str("addr", resolverAddr).Str("cacert", resolverCertPath).Msg("using TLS protected peer resolver")
			resolverConfig = consistentbackend.NewEndpointResolver(resolverAddr, resolverCertPath)
		} else {
			log.Debug().Str("addr", resolverAddr).Msg("using insecure peer resolver")
			resolverConfig = consistentbackend.NewEndpointResolverNoTLS(resolverAddr)
		}

		peerPSK := cobrautil.MustGetStringExpanded(cmd, "grpc-preshared-key")
		peerCertPath := cobrautil.MustGetStringExpanded(cmd, "dispatch-cluster-tls-cert-path")
		selfEndpoint := cobrautil.MustGetStringExpanded(cmd, "dispatch-cluster-addr")

		var endpointConfig *consistentbackend.EndpointConfig
		var fallbackConfig *consistentbackend.FallbackEndpointConfig
		if peerCertPath == "" {
			log.Debug().Str("endpoint", redispatchTarget).Msg("using insecure peers")
			endpointConfig = consistentbackend.NewEndpointConfigNoTLS(redispatchServiceName, redispatchTarget, peerPSK)
			fallbackConfig = consistentbackend.NewFallbackEndpointNoTLS(selfEndpoint, peerPSK)
		} else {
			log.Debug().Str("endpoint", redispatchTarget).Str("cacert", resolverCertPath).Msg("using TLS protected peers")
			endpointConfig = consistentbackend.NewEndpointConfig(redispatchServiceName, redispatchTarget, peerPSK, peerCertPath)
			fallbackConfig = consistentbackend.NewFallbackEndpoint(selfEndpoint, peerPSK, peerCertPath)
		}

		client, err := consistentbackend.NewConsistentBackendClient(resolverConfig, endpointConfig, fallbackConfig)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to initialize smart client")
		}

		go func() {
			client.Start(redispatchClientCtx)
			log.Info().Msg("started redispatch redispatch client")
		}()

		redispatch = remote.NewClusterDispatcher(client)
	}

	cachingRedispatch, err := caching.NewCachingDispatcher(redispatch, nil, "dispatch_client")
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize redispatcher cache")
	}

	clusterDispatch := graph.NewDispatcher(cachingRedispatch, nsm, ds)
	cachingClusterDispatch, err := caching.NewCachingDispatcher(clusterDispatch, nil, "dispatch")
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize cluster dispatcher cache")
	}

	clusterdispatch.RegisterGrpcServices(dispatchGrpcServer, cachingClusterDispatch)

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
		cachingRedispatch,
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
	redispatchClientCancel()

	if err := gatewaySrv.Close(); err != nil {
		log.Fatal().Err(err).Msg("failed while shutting down rest gateway")
	}

	if err := nsm.Close(); err != nil {
		log.Fatal().Err(err).Msg("failed while shutting down namespace manager")
	}

	if err := cachingRedispatch.Close(); err != nil {
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
