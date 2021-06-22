package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"time"

	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpczerolog "github.com/grpc-ecosystem/go-grpc-middleware/providers/zerolog/v2"
	grpclog "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jzelinskie/cobrautil"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/authzed/spicedb/internal/auth"
	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/crdb"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services"
	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/cmdutil"
	"github.com/authzed/spicedb/pkg/grpcutil"
)

func main() {
	var rootCmd = &cobra.Command{
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
	rootCmd.Flags().String("preshared-key", "", "preshared key to require on authenticated requests")
	rootCmd.Flags().Uint16("max-depth", 50, "maximum recursion depth for nested calls")
	rootCmd.Flags().String("datastore-engine", "memory", "type of datastore to initialize (e.g. postgres, cockroachdb, memory")
	rootCmd.Flags().String("datastore-url", "", "connection url (e.g. postgres://postgres:password@localhost:5432/spicedb) of storage layer for those engines that support it (postgres, crdb)")
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

	cmdutil.RegisterLoggingPersistentFlags(rootCmd)
	cmdutil.RegisterTracingPersistentFlags(rootCmd)

	var migrateCmd = &cobra.Command{
		Use:               "migrate [revision]",
		Short:             "execute schema migrations against database",
		PersistentPreRunE: persistentPreRunE,
		Run:               migrateRun,
		Args:              cobra.ExactArgs(1),
	}

	migrateCmd.Flags().String("datastore-engine", "postgres", "type of datastore to initialize (e.g. postgres, cockroachdb, memory")
	migrateCmd.Flags().String("datastore-url", "", "connection url (e.g. postgres://postgres:password@localhost:5432/spicedb) of storage layer for those engines that support it (postgres, crdb)")
	rootCmd.AddCommand(migrateCmd)

	var headCmd = &cobra.Command{
		Use:   "head",
		Short: "compute the head database migration revision",
		Run:   headRevisionRun,
		Args:  cobra.ExactArgs(0),
	}
	headCmd.Flags().String("datastore-engine", "postgres", "type of datastore to initialize (e.g. postgres, cockroachdb, memory")
	rootCmd.AddCommand(headCmd)

	var developerServiceCmd = &cobra.Command{
		Use:   "developer-service",
		Short: "runs the developer service",
		Run:   developerServiceRun,
		Args:  cobra.ExactArgs(0),
	}
	developerServiceCmd.Flags().String("grpc-addr", ":50053", "address to listen on for serving gRPC services")
	developerServiceCmd.Flags().String("grpc-cert-path", "", "local path to the TLS certificate used to serve gRPC services")
	developerServiceCmd.Flags().String("grpc-key-path", "", "local path to the TLS key used to serve gRPC services")
	developerServiceCmd.Flags().Bool("grpc-no-tls", false, "serve unencrypted gRPC services")
	developerServiceCmd.Flags().Duration("grpc-max-conn-age", 60*time.Second, "how long a connection should be able to live")
	developerServiceCmd.Flags().String("share-store", "inmemory", "kind of share store to use")
	developerServiceCmd.Flags().String("share-store-salt", "", "salt for share store hashing")
	developerServiceCmd.Flags().String("s3-access-key", "", "s3 access key for s3 share store")
	developerServiceCmd.Flags().String("s3-secret-key", "", "s3 secret key for s3 share store")
	developerServiceCmd.Flags().String("s3-bucket", "", "s3 bucket name for s3 share store")
	developerServiceCmd.Flags().String("s3-endpoint", "", "s3 endpoint for s3 share store")
	rootCmd.AddCommand(developerServiceCmd)

	rootCmd.Execute()
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

	var ds datastore.Datastore
	var err error
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
		)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to init datastore")
		}
	} else {
		log.Fatal().Str("datastore-engine", datastoreEngine).Msg("unknown datastore engine type")
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

	cachingDispatch, err := graph.NewCachingDispatcher(dispatch, nil, graph.RegisterPromMetrics)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize dispatcher cache")
	}

	RegisterGrpcServices(grpcServer, ds, nsm, cachingDispatch, cobrautil.MustGetUint16(cmd, "max-depth"))

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

	signalctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)
	for {
		select {
		case <-signalctx.Done():
			log.Info().Msg("received interrupt")
			grpcServer.GracefulStop()

			if err := metricsrv.Close(); err != nil {
				log.Fatal().Err(err).Msg("failed while shutting down metrics server")
			}
			return
		}
	}
}

func NewMetricsServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return &http.Server{
		Addr:    addr,
		Handler: mux,
	}
}

func RegisterGrpcServices(
	srv *grpc.Server,
	ds datastore.Datastore,
	nsm namespace.Manager,
	dispatch graph.Dispatcher,
	maxDepth uint16,
) {
	healthSrv := grpcutil.NewAuthlessHealthServer()

	api.RegisterACLServiceServer(srv, services.NewACLServer(ds, nsm, dispatch, maxDepth))
	healthSrv.SetServingStatus("ACLService", healthpb.HealthCheckResponse_SERVING)

	api.RegisterNamespaceServiceServer(srv, services.NewNamespaceServer(ds))
	healthSrv.SetServingStatus("NamespaceService", healthpb.HealthCheckResponse_SERVING)

	api.RegisterWatchServiceServer(srv, services.NewWatchServer(ds, nsm))
	healthSrv.SetServingStatus("WatchService", healthpb.HealthCheckResponse_SERVING)

	healthpb.RegisterHealthServer(srv, healthSrv)
	reflection.Register(srv)
}

func NewTlsGrpcServer(certPath, keyPath string, opts ...grpc.ServerOption) (*grpc.Server, error) {
	if certPath == "" || keyPath == "" {
		return nil, errors.New("missing one of required values: cert path, key path")
	}

	creds, err := credentials.NewServerTLSFromFile(certPath, keyPath)
	if err != nil {
		return nil, err
	}

	opts = append(opts, grpc.Creds(creds))
	return grpc.NewServer(opts...), nil
}

func persistentPreRunE(cmd *cobra.Command, args []string) error {
	if err := cobrautil.SyncViperPreRunE("spicedb")(cmd, args); err != nil {
		return err
	}

	cmdutil.LoggingPreRun(cmd, args)
	cmdutil.TracingPreRun(cmd, args)

	return nil
}
