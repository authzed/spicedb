package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
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
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services"
	internaldispatch "github.com/authzed/spicedb/internal/services/dispatch"
	v1alpha1svc "github.com/authzed/spicedb/internal/services/v1alpha1"
	"github.com/authzed/spicedb/pkg/validationfile"
)

func registerServeCmd(rootCmd *cobra.Command) {
	serveCmd := &cobra.Command{
		Use:               "serve",
		Short:             "serve the permissions database",
		Long:              "A database that stores, computes, and validates application permissions",
		PersistentPreRunE: persistentPreRunE,
		Run:               serveRun,
		Example: fmt.Sprintf(`	%s:
		spicedb serve --grpc-preshared-key "somerandomkeyhere" --grpc-no-tls

	%s:
		spicedb serve --grpc-preshared-key "realkeyhere" --grpc-cert-path path/to/tls/cert
	        		  --grpc-key-path path/to/tls/key --datastore-engine postgres
	        		  --datastore-conn-uri "postgres-connection-string-here"
`, color.YellowString("No TLS and in-memory"), color.GreenString("TLS and a real datastore")),
	}

	cobrautil.RegisterGrpcServerFlags(serveCmd.Flags())
	cobrautil.RegisterMetricsServerFlags(serveCmd.Flags())

	// Flags for the gRPC server beyond those provided from cobrautil
	serveCmd.Flags().String("grpc-preshared-key", "", "preshared key to require for authenticated requests")
	serveCmd.Flags().Duration("grpc-shutdown-grace-period", 0*time.Second, "amount of time after receiving sigint to continue serving")

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
	serveCmd.Flags().Duration("datastore-revision-fuzzing-duration", 5*time.Second, "amount of time to advertize stale revisions")
	serveCmd.Flags().String("datastore-query-split-size", common.DefaultSplitAtEstimatedQuerySize.String(), "estimated number of bytes at which a query is split when using a remote datastore")
	serveCmd.Flags().StringSlice("datastore-bootstrap-files", []string{}, "bootstrap data yaml files to load")
	serveCmd.Flags().Bool("datastore-bootstrap-overwrite", false, "overwrite any existing data with bootstrap data")
	serveCmd.Flags().Int("datastore-max-tx-retries", 50, "number of times a retriable transaction should be retried (cockroach driver only)")
	serveCmd.Flags().String("datastore-tx-overlap-strategy", "static", `strategy to generate transaction overlap keys ("prefix", "static", "insecure") (cockroach driver only)`)
	serveCmd.Flags().String("datastore-tx-overlap-key", "key", "static key to touch when writing to ensure transactions overlap (only used if --datastore-tx-overlap-strategy=static is set; cockroach driver only)")

	// Flags for the namespace manager
	serveCmd.Flags().Duration("ns-cache-expiration", 1*time.Minute, "amount of time a namespace entry should remain cached")

	// Flags for parsing and validating schemas.
	serveCmd.Flags().Bool("schema-prefixes-required", false, "require prefixes on all object definitions in schemas")

	// Flags for internal dispatch API
	serveCmd.Flags().String("internal-grpc-addr", ":50053", "address to listen for internal requests")

	// Flags for configuring dispatch behavior
	serveCmd.Flags().Uint32("dispatch-max-depth", 50, "maximum recursion depth for nested calls")
	serveCmd.Flags().String("dispatch-redispatch-dns-name", "", "dns SRV record name to resolve for remote redispatch, empty string disables redispatch")
	serveCmd.Flags().String("dispatch-redispatch-service-name", "grpc", "dns SRV record service name to resolve for remote redispatch")
	serveCmd.Flags().String("dispatch-peer-resolver-addr", "", "address used to connect to the peer endpoint resolver")
	serveCmd.Flags().String("dispatch-peer-resolver-cert-path", "", "local path to the TLS certificate for the peer endpoint resolver")

	// Flags for configuring API behavior
	serveCmd.Flags().Bool("disable-v1-schema-api", false, "disables the V1 schema API")

	// Flags for local dev dashboard
	serveCmd.Flags().String("dashboard-addr", ":8080", "address to listen for the dashboard")

	// Required flags.
	if err := serveCmd.MarkFlagRequired("grpc-preshared-key"); err != nil {
		panic("failed to mark flag as required: " + err.Error())
	}

	rootCmd.AddCommand(serveCmd)
}

func serveRun(cmd *cobra.Command, args []string) {
	token := cobrautil.MustGetString(cmd, "grpc-preshared-key")
	if len(token) < 1 {
		log.Fatal().Msg("a preshared key must be provided via --grpc-preshared-key to authenticate API requests")
	}

	datastoreEngine := cobrautil.MustGetString(cmd, "datastore-engine")
	datastoreURI := cobrautil.MustGetString(cmd, "datastore-conn-uri")

	revisionFuzzingTimedelta := cobrautil.MustGetDuration(cmd, "datastore-revision-fuzzing-duration")
	gcWindow := cobrautil.MustGetDuration(cmd, "datastore-gc-window")
	maxRetries := cobrautil.MustGetInt(cmd, "datastore-max-tx-retries")
	overlapKey := cobrautil.MustGetString(cmd, "datastore-tx-overlap-key")
	overlapStrategy := cobrautil.MustGetString(cmd, "datastore-tx-overlap-strategy")

	splitQuerySize, err := units.ParseBase2Bytes(cobrautil.MustGetString(cmd, "datastore-query-split-size"))
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
		grpclog.UnaryServerInterceptor(grpczerolog.InterceptorLogger(log.Logger)),
		otelgrpc.UnaryServerInterceptor(),
		grpcauth.UnaryServerInterceptor(auth.RequirePresharedKey(token)),
		grpcprom.UnaryServerInterceptor,
		servicespecific.UnaryServerInterceptor,
	)

	streamMiddleware := grpc.ChainStreamInterceptor(
		grpclog.StreamServerInterceptor(grpczerolog.InterceptorLogger(log.Logger)),
		otelgrpc.StreamServerInterceptor(),
		grpcauth.StreamServerInterceptor(auth.RequirePresharedKey(token)),
		grpcprom.StreamServerInterceptor,
		servicespecific.StreamServerInterceptor,
	)

	grpcServer, err := cobrautil.GrpcServerFromFlags(cmd, middleware, streamMiddleware)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create gRPC server")
	}

	internalGrpcServer, err := cobrautil.GrpcServerFromFlags(cmd, middleware, streamMiddleware)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create internal gRPC server")
	}

	redispatch := graph.NewLocalOnlyDispatcher(nsm, ds)
	redispatchClientCtx, redispatchClientCancel := context.WithCancel(context.Background())

	redispatchTarget := cobrautil.MustGetString(cmd, "dispatch-redispatch-dns-name")
	redispatchServiceName := cobrautil.MustGetString(cmd, "dispatch-redispatch-service-name")
	if redispatchTarget != "" {
		log.Info().Str("target", redispatchTarget).Msg("initializing remote redispatcher")

		resolverAddr := cobrautil.MustGetString(cmd, "dispatch-peer-resolver-addr")
		resolverCertPath := cobrautil.MustGetString(cmd, "dispatch-peer-resolver-cert-path")
		var resolverConfig *consistentbackend.EndpointResolverConfig
		if resolverCertPath != "" {
			log.Debug().Str("addr", resolverAddr).Str("cacert", resolverCertPath).Msg("using TLS protected peer resolver")
			resolverConfig = consistentbackend.NewEndpointResolver(resolverAddr, resolverCertPath)
		} else {
			log.Debug().Str("addr", resolverAddr).Msg("using insecure peer resolver")
			resolverConfig = consistentbackend.NewEndpointResolverNoTLS(resolverAddr)
		}

		peerCertPath := cobrautil.MustGetStringExpanded(cmd, "grpc-cert-path")
		peerPSK := cobrautil.MustGetString(cmd, "grpc-preshared-key")
		selfEndpoint := cobrautil.MustGetString(cmd, "internal-grpc-addr")

		var endpointConfig *consistentbackend.EndpointConfig
		var fallbackConfig *consistentbackend.FallbackEndpointConfig
		if !cobrautil.MustGetBool(cmd, "grpc-no-tls") {
			log.Debug().Str("endpoint", redispatchTarget).Str("cacert", resolverCertPath).Msg("using TLS protected peers")
			endpointConfig = consistentbackend.NewEndpointConfig(redispatchServiceName, redispatchTarget, peerPSK, peerCertPath)
			fallbackConfig = consistentbackend.NewFallbackEndpoint(selfEndpoint, peerPSK, peerCertPath)
		} else {
			log.Debug().Str("endpoint", redispatchTarget).Msg("using insecure peers")
			endpointConfig = consistentbackend.NewEndpointConfigNoTLS(redispatchServiceName, redispatchTarget, peerPSK)
			fallbackConfig = consistentbackend.NewFallbackEndpointNoTLS(selfEndpoint, peerPSK)
		}

		client, err := consistentbackend.NewConsistentBackendClient(resolverConfig, endpointConfig, fallbackConfig)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to initialize smart client")
		}

		go func() {
			client.Start(redispatchClientCtx)
			log.Info().Msg("started internal redispatch client")
		}()

		redispatch = remote.NewClusterDispatcher(client)
	}

	cachingRedispatch, err := caching.NewCachingDispatcher(redispatch, nil, "dispatch_client")
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize redispatcher cache")
	}

	prefixRequiredOption := v1alpha1svc.PrefixRequired
	if !cobrautil.MustGetBool(cmd, "schema-prefixes-required") {
		prefixRequiredOption = v1alpha1svc.PrefixNotRequired
	}

	v1SchemaServiceOption := services.V1SchemaServiceEnabled
	if cobrautil.MustGetBool(cmd, "disable-v1-schema-api") {
		v1SchemaServiceOption = services.V1SchemaServiceDisabled
	}

	maxDepth := cobrautil.MustGetUint32(cmd, "dispatch-max-depth")
	services.RegisterGrpcServices(grpcServer, ds, nsm, cachingRedispatch, maxDepth, prefixRequiredOption, v1SchemaServiceOption)

	internalDispatch := graph.NewDispatcher(cachingRedispatch, nsm, ds)
	cachingInternalDispatch, err := caching.NewCachingDispatcher(internalDispatch, nil, "dispatch")
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize internal dispatcher cache")
	}

	internaldispatch.RegisterGrpcServices(internalGrpcServer, cachingInternalDispatch)

	go func() {
		addr := cobrautil.MustGetString(cmd, "grpc-addr")
		l, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatal().Str("addr", addr).Msg("failed to listen on addr for gRPC server")
		}

		log.Info().Str("addr", addr).Msg("gRPC server started listening")
		err = grpcServer.Serve(l)
		if err != nil {
			log.Fatal().Msg("failed to start gRPC server")
		}
	}()

	go func() {
		addr := cobrautil.MustGetString(cmd, "internal-grpc-addr")
		l, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatal().Str("addr", addr).Msg("failed to listen on addr for internal gRPC server")
		}

		log.Info().Str("addr", addr).Msg("internal gRPC server started listening")
		err = internalGrpcServer.Serve(l)
		if err != nil {
			log.Fatal().Msg("failed to start internal gRPC server")
		}
	}()

	metricsrv := cobrautil.MetricsServerFromFlags(cmd)
	go func() {
		if err := metricsrv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("failed while serving metrics")
		}
	}()

	dashboardAddr := cobrautil.MustGetString(cmd, "dashboard-addr")
	dashboard := dashboard.NewDashboard(dashboardAddr, dashboard.Args{
		GrpcNoTLS:       cobrautil.MustGetBool(cmd, "grpc-no-tls"),
		GrpcAddr:        cobrautil.MustGetString(cmd, "grpc-addr"),
		DatastoreEngine: datastoreEngine,
	}, ds)
	if dashboardAddr != "" {
		go func() {
			if err := dashboard.ListenAndServe(); err != http.ErrServerClosed {
				log.Fatal().Err(err).Msg("failed while serving dashboard")
			}
		}()

		url := fmt.Sprintf("http://localhost%s", dashboardAddr)
		log.Info().Str("url", url).Msg("dashboard running")
	}

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
	internalGrpcServer.GracefulStop()
	redispatchClientCancel()

	if err := metricsrv.Close(); err != nil {
		log.Fatal().Err(err).Msg("failed while shutting down metrics server")
	}

	if err := nsm.Close(); err != nil {
		log.Fatal().Err(err).Msg("failed while shutting down namespace manager")
	}

	if err := ds.Close(); err != nil {
		log.Fatal().Err(err).Msg("failed while shutting down datastore")
	}

	if dashboardAddr != "" {
		if err := dashboard.Close(); err != nil {
			log.Fatal().Err(err).Msg("failed while shutting down dashboard")
		}
	}
}
