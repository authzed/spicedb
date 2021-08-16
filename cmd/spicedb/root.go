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
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpczerolog "github.com/grpc-ecosystem/go-grpc-middleware/providers/zerolog/v2"
	grpclog "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/authzed/spicedb/internal/auth"
	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/crdb"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/datastore/readonly"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/namespace"
	v0svc "github.com/authzed/spicedb/internal/services/v0"
	v1alpha1svc "github.com/authzed/spicedb/internal/services/v1alpha1"
	"github.com/authzed/spicedb/pkg/grpcutil"
	"github.com/authzed/spicedb/pkg/smartclient"
)

func newRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:               "spicedb",
		Short:             "A tuple store for ACLs.",
		PersistentPreRunE: persistentPreRunE,
		Run:               rootRun,
	}

	rootCmd.Flags().String("grpc-addr", ":50051", "address to listen on for serving gRPC services")
	rootCmd.Flags().String("grpc-cert-path", "", "local path to the TLS certificate used to serve gRPC services")
	rootCmd.Flags().String("grpc-key-path", "", "local path to the TLS key used to serve gRPC services")
	rootCmd.Flags().Bool("grpc-no-tls", false, "serve unencrypted gRPC services")
	rootCmd.Flags().Duration("grpc-max-conn-age", 30*time.Second, "how long a connection should be able to live")

	rootCmd.Flags().String("metrics-addr", ":9090", "address to listen on for serving metrics and profiles")

	rootCmd.Flags().Bool("read-only", false, "set the service to read-only mode")
	rootCmd.Flags().Uint16("max-depth", 50, "maximum recursion depth for nested calls")
	rootCmd.Flags().String("preshared-key", "", "preshared key to require on authenticated requests")
	rootCmd.Flags().String("datastore-engine", "memory", "type of datastore to initialize (e.g. postgres, cockroachdb, memory")
	rootCmd.Flags().String("datastore-url", "", "connection url (e.g. postgres://postgres:password@localhost:5432/spicedb) of storage layer for those engines that support it (postgres, crdb)")
	rootCmd.Flags().String("datastore-query-split-size", common.DefaultSplitAtEstimatedQuerySize.String(), "the estimated number of bytes at which a query is split, for those engines that support it (postgres, crdb)")

	rootCmd.Flags().Duration("revision-fuzzing-duration", 5*time.Second, "amount of time to advertize stale revisions")
	rootCmd.Flags().Duration("gc-window", 24*time.Hour, "amount of time before a revision is garbage collected")
	rootCmd.Flags().Duration("ns-cache-expiration", 1*time.Minute, "amount of time a namespace entry should remain cached")

	rootCmd.Flags().Int("pg-max-conn-open", 20, "number of concurrent connections open in a the postgres connection pool")
	rootCmd.Flags().Int("pg-min-conn-open", 10, "number of minimum concurrent connections open in a the postgres connection pool")
	rootCmd.Flags().Duration("pg-max-conn-lifetime", 30*time.Minute, "maximum amount of time a connection can live in the postgres connection pool")
	rootCmd.Flags().Duration("pg-max-conn-idletime", 30*time.Minute, "maximum amount of time a connection can idle in the postgres connection pool")
	rootCmd.Flags().Duration("pg-health-check-period", 30*time.Second, "duration between checks of the health of idle connections")

	rootCmd.Flags().Int("crdb-max-conn-open", 20, "number of concurrent connections open in the cockroachdb connection pool")
	rootCmd.Flags().Int("crdb-min-conn-open", 20, "number of idle connections to keep open in the cockroachdb connection pool")
	rootCmd.Flags().Duration("crdb-max-conn-lifetime", 30*time.Minute, "maximum amount of time a connection can live in the cockroachdb connection pool")
	rootCmd.Flags().Duration("crdb-max-conn-idletime", 30*time.Minute, "maximum amount of time a connection can idle in the cockroachdb connection pool")
	rootCmd.Flags().Duration("shutdown-grace-period", 0*time.Second, "amount of time after receiving sigint to continue serving")

	rootCmd.Flags().String("redispatch-dns-name", "", "dns service name to resolve for remote redispatch, empty string disables redispatch")
	rootCmd.Flags().String("peer-resolver-addr", "", "address used to connect to the peer endpoint resolver")
	rootCmd.Flags().String("peer-resolver-cert-path", "", "local path to the TLS certificate for the peer endpoint resolver")

	return rootCmd
}

func rootRun(cmd *cobra.Command, args []string) {
	token := cobrautil.MustGetString(cmd, "preshared-key")
	if len(token) < 1 {
		log.Fatal().Msg("must provide a preshared-key")
	}

	var sharedOptions []grpc.ServerOption
	sharedOptions = append(sharedOptions, grpcmw.WithUnaryServerChain(
		otelgrpc.UnaryServerInterceptor(),
		grpcauth.UnaryServerInterceptor(auth.RequirePresharedKey(token)),
		grpcprom.UnaryServerInterceptor,
		grpclog.UnaryServerInterceptor(grpczerolog.InterceptorLogger(log.Logger)),
	))

	sharedOptions = append(sharedOptions, grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionAge: cobrautil.MustGetDuration(cmd, "grpc-max-conn-age"),
	}))

	grpcprom.EnableHandlingTimeHistogram(grpcprom.WithHistogramBuckets(
		[]float64{.006, .010, .018, .024, .032, .042, .056, .075, .100, .178, .316, .562, 1.000},
	))

	var grpcServer *grpc.Server
	if cobrautil.MustGetBool(cmd, "grpc-no-tls") {
		grpcServer = grpc.NewServer(sharedOptions...)
	} else {
		var err error
		grpcServer, err = NewTlsGrpcServer(
			cobrautil.MustGetStringExpanded(cmd, "grpc-cert-path"),
			cobrautil.MustGetStringExpanded(cmd, "grpc-key-path"),
			sharedOptions...,
		)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to create TLS gRPC server")
		}
	}

	datastoreEngine := cobrautil.MustGetString(cmd, "datastore-engine")
	datastoreUrl := cobrautil.MustGetString(cmd, "datastore-url")

	revisionFuzzingTimedelta := cobrautil.MustGetDuration(cmd, "revision-fuzzing-duration")
	gcWindow := cobrautil.MustGetDuration(cmd, "gc-window")

	var err error
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
			datastoreUrl,
			crdb.ConnMaxIdleTime(cobrautil.MustGetDuration(cmd, "crdb-max-conn-idletime")),
			crdb.ConnMaxLifetime(cobrautil.MustGetDuration(cmd, "crdb-max-conn-lifetime")),
			crdb.MaxOpenConns(cobrautil.MustGetInt(cmd, "crdb-max-conn-open")),
			crdb.MinOpenConns(cobrautil.MustGetInt(cmd, "crdb-min-conn-open")),
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
			datastoreUrl,
			postgres.ConnMaxIdleTime(cobrautil.MustGetDuration(cmd, "pg-max-conn-idletime")),
			postgres.ConnMaxLifetime(cobrautil.MustGetDuration(cmd, "pg-max-conn-lifetime")),
			postgres.HealthCheckPeriod(cobrautil.MustGetDuration(cmd, "pg-health-check-period")),
			postgres.MaxOpenConns(cobrautil.MustGetInt(cmd, "pg-max-conn-open")),
			postgres.MinOpenConns(cobrautil.MustGetInt(cmd, "pg-min-conn-open")),
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

	if cobrautil.MustGetBool(cmd, "read-only") {
		log.Warn().Msg("setting the service to read-only")
		ds = readonly.NewReadonlyDatastore(ds)
	}

	nsCacheExpiration := cobrautil.MustGetDuration(cmd, "ns-cache-expiration")
	nsm, err := namespace.NewCachingNamespaceManager(ds, nsCacheExpiration, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize namespace manager")
	}

	dispatch, err := graph.NewLocalDispatcher(nsm, ds)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize dispatcher")
	}

	redispatchTarget := cobrautil.MustGetString(cmd, "redispatch-dns-name")
	if redispatchTarget != "" {
		log.Info().Str("target", redispatchTarget).Msg("initializing remote redispatcher")

		resolverAddr := cobrautil.MustGetString(cmd, "peer-resolver-addr")
		resolverCertPath := cobrautil.MustGetString(cmd, "peer-resolver-cert-path")
		var resolverConfig *smartclient.EndpointResolverConfig
		if resolverCertPath != "" {
			log.Debug().Str("addr", resolverAddr).Str("cacert", resolverCertPath).Msg("using TLS protected peer resolver")
			resolverConfig = smartclient.NewEndpointResolver(resolverAddr, resolverCertPath)
		} else {
			log.Debug().Str("addr", resolverAddr).Msg("using insecure peer resolver")
			resolverConfig = smartclient.NewEndpointResolverNoTLS(resolverAddr)
		}

		peerCertPath := cobrautil.MustGetStringExpanded(cmd, "grpc-cert-path")
		peerPSK := cobrautil.MustGetString(cmd, "preshared-key")
		selfEndpoint := cobrautil.MustGetString(cmd, "grpc-addr")

		var endpointConfig *smartclient.EndpointConfig
		var fallbackConfig *smartclient.FallbackEndpointConfig
		if !cobrautil.MustGetBool(cmd, "grpc-no-tls") {
			log.Debug().Str("endpoint", redispatchTarget).Str("cacert", resolverCertPath).Msg("using TLS protected peers")
			endpointConfig = smartclient.NewEndpointConfig(redispatchTarget, peerPSK, peerCertPath)
			fallbackConfig = smartclient.NewFallbackEndpoint(selfEndpoint, peerPSK, peerCertPath)
		} else {
			log.Debug().Str("endpoint", redispatchTarget).Msg("using insecure peers")
			endpointConfig = smartclient.NewEndpointConfigNoTLS(redispatchTarget, peerPSK)
			fallbackConfig = smartclient.NewFallbackEndpointNoTLS(selfEndpoint, peerPSK)
		}

		client, err := smartclient.NewSmartClient(resolverConfig, endpointConfig, fallbackConfig)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to initialize smart client")
		}

		redispatcher := graph.NewClusterDispatcher(client, v0svc.DepthRemainingHeader)
		dispatch, err = graph.NewLocalDispatcherWithRedispatch(nsm, ds, redispatcher)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to initialize redispatcher")
		}
	}

	cachingDispatch, err := graph.NewCachingDispatcher(dispatch, nil, graph.RegisterPromMetrics)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize dispatcher cache")
	}

	registerGrpcServices(grpcServer, ds, nsm, cachingDispatch, cobrautil.MustGetUint16(cmd, "max-depth"))

	go func() {
		addr := cobrautil.MustGetString(cmd, "grpc-addr")
		l, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatal().Str("addr", addr).Msg("failed to listen on addr for gRPC server")
		}

		log.Info().Str("addr", addr).Msg("gRPC server started listening")
		grpcServer.Serve(l)
	}()

	metricsrv := NewMetricsServer(cobrautil.MustGetString(cmd, "metrics-addr"))
	go func() {
		if err := metricsrv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("failed while serving metrics")
		}
	}()

	signalctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	gracePeriod := cobrautil.MustGetDuration(cmd, "shutdown-grace-period")

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

	if err := metricsrv.Close(); err != nil {
		log.Fatal().Err(err).Msg("failed while shutting down metrics server")
	}
}

func registerGrpcServices(
	srv *grpc.Server,
	ds datastore.Datastore,
	nsm namespace.Manager,
	dispatch graph.Dispatcher,
	maxDepth uint16,
) {
	healthSrv := grpcutil.NewAuthlessHealthServer()

	v0.RegisterACLServiceServer(srv, v0svc.NewACLServer(ds, nsm, dispatch, maxDepth))
	healthSrv.SetServingStatus("ACLService", healthpb.HealthCheckResponse_SERVING)

	v0.RegisterNamespaceServiceServer(srv, v0svc.NewNamespaceServer(ds))
	healthSrv.SetServingStatus("NamespaceService", healthpb.HealthCheckResponse_SERVING)

	v0.RegisterWatchServiceServer(srv, v0svc.NewWatchServer(ds, nsm))
	healthSrv.SetServingStatus("WatchService", healthpb.HealthCheckResponse_SERVING)

	v1alpha1.RegisterSchemaServiceServer(srv, v1alpha1svc.NewSchemaServer(ds))
	healthSrv.SetServingStatus("SchemaService", healthpb.HealthCheckResponse_SERVING)

	healthpb.RegisterHealthServer(srv, healthSrv)
	reflection.Register(srv)
}
