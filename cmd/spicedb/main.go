package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"time"

	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jzelinskie/cobrautil"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"

	"github.com/authzed/spicedb/internal/auth"
	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services"
	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	health "github.com/authzed/spicedb/pkg/REDACTEDapi/healthcheck"
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
	rootCmd.Flags().String("metrics-addr", ":9090", "address to listen on for serving metrics and profiles")
	rootCmd.Flags().String("preshared-key", "", "preshared key to require on authenticated requests")
	rootCmd.Flags().Uint16("max-depth", 50, "maximum recursion depth for nested calls")
	rootCmd.Flags().String("datastore-url", "memory:///", "connection url of storage layer")
	rootCmd.Flags().Duration("revision-fuzzing-duration", 5*time.Second, "amount of time to advertize stale revisions")
	rootCmd.Flags().Duration("gc-window", 24*time.Hour, "amount of time before a revision is garbage collected")
	rootCmd.Flags().Duration("ns-cache-expiration", 1*time.Minute, "amount of time a namespace entry should remain cached")
	rootCmd.Flags().Int("pg-max-conn-open", 20, "number of concurrent connections open in a the postgres connection pool")
	rootCmd.Flags().Int("pg-max-conn-idle", 20, "number of idle connections open in a the postgres connection pool")
	rootCmd.Flags().Duration("pg-max-conn-lifetime", 30*time.Minute, "maximum amount of time a connection can live in the postgres connection pool")
	rootCmd.Flags().Duration("pg-max-conn-idletime", 30*time.Minute, "maximum amount of time a connection can idle in the postgres connection pool")

	rootCmd.PersistentFlags().String("log-level", "info", "verbosity of logging (trace, debug, info, warn, error, fatal, panic)")

	rootCmd.Execute()
}

func rootRun(cmd *cobra.Command, args []string) {
	token := cobrautil.MustGetString(cmd, "preshared-key")
	if len(token) < 1 {
		log.Fatal().Msg("must provide a preshared-key")
	}

	grpcMiddleware := grpcmw.WithUnaryServerChain(
		grpcauth.UnaryServerInterceptor(auth.RequirePresharedKey(token)),
		grpcprom.UnaryServerInterceptor,
	)

	var grpcServer *grpc.Server
	if cobrautil.MustGetBool(cmd, "grpc-no-tls") {
		grpcServer = grpc.NewServer(grpcMiddleware)
	} else {
		var err error
		grpcServer, err = NewTlsGrpcServer(
			cobrautil.MustGetStringExpanded(cmd, "grpc-cert-path"),
			cobrautil.MustGetStringExpanded(cmd, "grpc-key-path"),
			grpcMiddleware,
		)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to create TLS gRPC server")
		}
	}

	datastoreUrl := cobrautil.MustGetString(cmd, "datastore-url")

	revisionFuzzingTimedelta := cobrautil.MustGetDuration(cmd, "revision-fuzzing-duration")
	gcWindow := cobrautil.MustGetDuration(cmd, "gc-window")

	var ds datastore.Datastore
	var err error
	if datastoreUrl == "memory:///" {
		log.Info().Msg("using in-memory datastore")
		ds, err = memdb.NewMemdbDatastore(0, revisionFuzzingTimedelta, gcWindow, 0)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to init datastore")
		}
	} else {
		log.Info().Msg("using postgres datastore")
		ds, err = postgres.NewPostgresDatastore(
			datastoreUrl,
			&postgres.ConnectionProperties{
				MaxOpenConns:    cobrautil.MustGetInt(cmd, "pg-max-conn-open"),
				MaxIdleConns:    cobrautil.MustGetInt(cmd, "pg-max-conn-idle"),
				ConnMaxLifetime: cobrautil.MustGetDuration(cmd, "pg-max-conn-lifetime"),
				ConnMaxIdleTime: cobrautil.MustGetDuration(cmd, "pg-max-conn-idletime"),
			},
			0,
			revisionFuzzingTimedelta,
			gcWindow,
		)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to init datastore")
		}
	}

	nsCacheExpiration := cobrautil.MustGetDuration(cmd, "ns-cache-expiration")
	nsm, err := namespace.NewCachingNamespaceManager(ds, nsCacheExpiration, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize namespace manager")
	}

	dispatch, err := graph.NewLocalDispatcher(nsm, ds)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize check dispatcher")
	}

	RegisterGrpcServices(grpcServer, ds, nsm, dispatch, cobrautil.MustGetUint16(cmd, "max-depth"))

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
	api.RegisterACLServiceServer(srv, services.NewACLServer(ds, nsm, dispatch, maxDepth))
	api.RegisterNamespaceServiceServer(srv, services.NewNamespaceServer(ds))
	api.RegisterWatchServiceServer(srv, services.NewWatchServer(ds, nsm))
	health.RegisterHealthServer(srv, services.NewHealthServer())
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

	level := strings.ToLower(cobrautil.MustGetString(cmd, "log-level"))
	switch level {
	case "trace":
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	case "fatal":
		zerolog.SetGlobalLevel(zerolog.FatalLevel)
	case "panic":
		zerolog.SetGlobalLevel(zerolog.PanicLevel)
	default:
		return errors.New("unknown log level")
	}
	log.Info().Str("new level", level).Msg("set log level")

	return nil
}
