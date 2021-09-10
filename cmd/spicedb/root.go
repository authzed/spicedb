package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alecthomas/units"
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
	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/crdb"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/dispatch/remote"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services"
	internaldispatch "github.com/authzed/spicedb/internal/services/dispatch"
	v1alpha1svc "github.com/authzed/spicedb/internal/services/v1alpha1"
	"github.com/authzed/spicedb/pkg/smartclient/consistentbackend"
	"github.com/authzed/spicedb/pkg/validationfile"
)

func newRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:               "spicedb",
		Short:             "A modern permissions database",
		Long:              "A database that stores, computes, and validates application permissions",
		PersistentPreRunE: persistentPreRunE,
		Run:               rootRun,
	}

	cobrautil.RegisterZeroLogFlags(rootCmd.PersistentFlags())
	cobrautil.RegisterOpenTelemetryFlags(rootCmd.PersistentFlags(), rootCmd.Use)
	cobrautil.RegisterGrpcServerFlags(rootCmd.Flags())
	cobrautil.RegisterMetricsServerFlags(rootCmd.Flags())

	// Flags for the gRPC server beyond those provided from cobrautil
	rootCmd.Flags().String("grpc-preshared-key", "", "preshared key to require for authenticated requests")
	rootCmd.Flags().Duration("grpc-shutdown-grace-period", 0*time.Second, "amount of time after receiving sigint to continue serving")

	// Flags for the datastore
	rootCmd.Flags().String("datastore-engine", "memory", `type of datastore to initialize ("memory", "postgres", "cockroachdb")`)
	rootCmd.Flags().String("datastore-conn-uri", "", `connection string used by remote datastores (e.g. "postgres://postgres:password@localhost:5432/spicedb")`)
	rootCmd.Flags().Bool("datastore-readonly", false, "set the service to read-only mode")
	rootCmd.Flags().Int("datastore-conn-max-open", 20, "number of concurrent connections open in a remote datastore's connection pool")
	rootCmd.Flags().Int("datastore-conn-min-open", 10, "number of minimum concurrent connections open in a remote datastore's connection pool")
	rootCmd.Flags().Duration("datastore-conn-max-lifetime", 30*time.Minute, "maximum amount of time a connection can live in a remote datastore's connection pool")
	rootCmd.Flags().Duration("datastore-conn-max-idletime", 30*time.Minute, "maximum amount of time a connection can idle in a remote datastore's connection pool")
	rootCmd.Flags().Duration("datastore-conn-healthcheck-interval", 30*time.Second, "time between a remote datastore's connection pool health checks")
	rootCmd.Flags().Duration("datastore-gc-window", 24*time.Hour, "amount of time before revisions are garbage collected")
	rootCmd.Flags().Duration("datastore-revision-fuzzing-duration", 5*time.Second, "amount of time to advertize stale revisions")
	rootCmd.Flags().String("datastore-query-split-size", common.DefaultSplitAtEstimatedQuerySize.String(), "estimated number of bytes at which a query is split when using a remote datastore")
	rootCmd.Flags().StringSlice("datastore-bootstrap-files", []string{}, "bootstrap data yaml files to load")
	rootCmd.Flags().Bool("datastore-bootstrap-overwrite", false, "overwrite any existing data with bootstrap data")

	// Flags for the namespace manager
	rootCmd.Flags().Duration("ns-cache-expiration", 1*time.Minute, "amount of time a namespace entry should remain cached")

	// Flags for parsing and validating schemas.
	rootCmd.Flags().Bool("schema-prefixes-required", false, "require prefixes on all object definitions in schemas")

	// Flags for internal dispatch API
	rootCmd.Flags().String("internal-grpc-addr", ":50053", "address to listen for internal requests")

	// Flags for configuring dispatch behavior
	rootCmd.Flags().Uint32("dispatch-max-depth", 50, "maximum recursion depth for nested calls")
	rootCmd.Flags().String("dispatch-redispatch-dns-name", "", "dns SRV record name to resolve for remote redispatch, empty string disables redispatch")
	rootCmd.Flags().String("dispatch-redispatch-service-name", "grpc", "dns SRV record service name to resolve for remote redispatch")
	rootCmd.Flags().String("dispatch-peer-resolver-addr", "", "address used to connect to the peer endpoint resolver")
	rootCmd.Flags().String("dispatch-peer-resolver-cert-path", "", "local path to the TLS certificate for the peer endpoint resolver")

	return rootCmd
}

func rootRun(cmd *cobra.Command, args []string) {
	token := cobrautil.MustGetString(cmd, "grpc-preshared-key")
	if len(token) < 1 {
		log.Fatal().Msg("must provide flag: --grpc-preshared-key")
	}

	grpcprom.EnableHandlingTimeHistogram(grpcprom.WithHistogramBuckets(
		[]float64{.006, .010, .018, .024, .032, .042, .056, .075, .100, .178, .316, .562, 1.000},
	))

	middleware := grpc.ChainUnaryInterceptor(
		grpclog.UnaryServerInterceptor(grpczerolog.InterceptorLogger(log.Logger)),
		otelgrpc.UnaryServerInterceptor(),
		grpcauth.UnaryServerInterceptor(auth.RequirePresharedKey(token)),
		grpcprom.UnaryServerInterceptor,
	)

	grpcServer, err := cobrautil.GrpcServerFromFlags(cmd, middleware)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create gRPC server")
	}

	internalGrpcServer, err := cobrautil.GrpcServerFromFlags(cmd, middleware)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create internal gRPC server")
	}

	datastoreEngine := cobrautil.MustGetString(cmd, "datastore-engine")
	datastoreUri := cobrautil.MustGetString(cmd, "datastore-conn-uri")

	revisionFuzzingTimedelta := cobrautil.MustGetDuration(cmd, "datastore-revision-fuzzing-duration")
	gcWindow := cobrautil.MustGetDuration(cmd, "datastore-gc-window")

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
			datastoreUri,
			crdb.ConnMaxIdleTime(cobrautil.MustGetDuration(cmd, "datastore-conn-max-idletime")),
			crdb.ConnMaxLifetime(cobrautil.MustGetDuration(cmd, "datastore-conn-max-lifetime")),
			crdb.MaxOpenConns(cobrautil.MustGetInt(cmd, "datastore-conn-max-open")),
			crdb.MinOpenConns(cobrautil.MustGetInt(cmd, "datastore-conn-min-open")),
			crdb.RevisionQuantization(revisionFuzzingTimedelta),
			crdb.GCWindow(gcWindow),
			crdb.SplitAtEstimatedQuerySize(splitQuerySize),
		)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to init datastore")
		}
	} else if datastoreEngine == "postgres" {
		log.Info().Msg("using postgres datastore")
		ds, err = postgres.NewPostgresDatastore(
			datastoreUri,
			postgres.ConnMaxIdleTime(cobrautil.MustGetDuration(cmd, "datastore-conn-max-idletime")),
			postgres.ConnMaxLifetime(cobrautil.MustGetDuration(cmd, "datastore-conn-max-lifetime")),
			postgres.HealthCheckPeriod(cobrautil.MustGetDuration(cmd, "datastore-conn-healthcheck-interval")),
			postgres.MaxOpenConns(cobrautil.MustGetInt(cmd, "datastore-conn-max-open")),
			postgres.MinOpenConns(cobrautil.MustGetInt(cmd, "datastore-conn-min-open")),
			postgres.RevisionFuzzingTimedelta(revisionFuzzingTimedelta),
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
		isEmpty, err := ds.IsEmpty(context.Background())
		if err != nil {
			log.Fatal().Err(err).Msg("unable to determine datastore state before applying bootstrap data")
		}
		if bootstrapOverwrite || isEmpty {
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

	maxDepth := cobrautil.MustGetUint32(cmd, "dispatch-max-depth")
	services.RegisterGrpcServices(grpcServer, ds, nsm, cachingRedispatch, maxDepth, prefixRequiredOption)

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
}
