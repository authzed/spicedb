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
	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jzelinskie/cobrautil"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"

	"github.com/authzed/spicedb/internal/auth"
	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/services"
	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	health "github.com/authzed/spicedb/pkg/REDACTEDapi/healthcheck"
)

func main() {
	var rootCmd = &cobra.Command{
		Use:               "spicedb",
		Short:             "A tuple store for ACLs.",
		PersistentPreRunE: cobrautil.SyncViperPreRunE("CALADAN"),
		Run:               rootRun,
	}

	rootCmd.Flags().String("grpc-addr", ":50051", "address to listen on for serving gRPC services")
	rootCmd.Flags().String("grpc-cert-path", "", "local path to the TLS certificate used to serve gRPC services")
	rootCmd.Flags().String("grpc-key-path", "", "local path to the TLS key used to serve gRPC services")
	rootCmd.Flags().Bool("grpc-no-tls", false, "serve unencrypted gRPC services")
	rootCmd.Flags().String("metrics-addr", ":9090", "address to listen on for serving metrics and profiles")
	rootCmd.Flags().String("preshared-key", "", "preshared key to require on authenticated requests")
	rootCmd.Flags().Bool("log-debug", false, "enable logging debug events")
	rootCmd.Flags().Uint16("max-depth", 50, "maximum recursion depth for nested calls")
	rootCmd.Flags().String("datastore-url", "memory:///", "connection url of storage layer")
	rootCmd.Flags().Duration("revision-fuzzing-duration", 5*time.Second, "amount of time to advertize stale revisions")
	rootCmd.Flags().Duration("gc-window", 24*time.Hour, "amount of time before a revision is garbage collected")

	rootCmd.Execute()
}

func rootRun(cmd *cobra.Command, args []string) {
	logger, _ := zap.NewProduction()
	if cobrautil.MustGetBool(cmd, "log-debug") {
		logger, _ = zap.NewDevelopment()
	}
	defer logger.Sync()

	token := cobrautil.MustGetString(cmd, "preshared-key")
	if len(token) < 1 {
		logger.Fatal("must provide a preshared-key")
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
			logger.Fatal("failed to create TLS gRPC server", zap.Error(err))
		}
	}

	datastoreUrl := cobrautil.MustGetString(cmd, "datastore-url")

	revisionFuzzingTimedelta := cobrautil.MustGetDuration(cmd, "revision-fuzzing-duration")
	gcWindow := cobrautil.MustGetDuration(cmd, "gc-window")

	var ds datastore.Datastore
	var err error
	if datastoreUrl == "memory:///" {
		logger.Info("using in-memory datastore")
		ds, err = memdb.NewMemdbDatastore(0, revisionFuzzingTimedelta, gcWindow)
		if err != nil {
			logger.Fatal("failed to init datastore", zap.Error(err))
		}
	} else {
		logger.Info("using postgres datastore")
		ds, err = postgres.NewPostgresDatastore(datastoreUrl, 0, revisionFuzzingTimedelta, gcWindow)
		if err != nil {
			logger.Fatal("failed to init datastore", zap.Error(err))
		}
	}

	dispatch, err := graph.NewLocalDispatcher(ds)
	if err != nil {
		logger.Fatal("failed to initialize check dispatcher", zap.Error(err))
	}

	RegisterGrpcServices(grpcServer, ds, dispatch, cobrautil.MustGetUint16(cmd, "max-depth"))

	go func() {
		addr := cobrautil.MustGetString(cmd, "grpc-addr")
		l, err := net.Listen("tcp", addr)
		if err != nil {
			logger.Fatal("failed to listen on addr for gRPC server", zap.Error(err), zap.String("addr", addr))
		}

		logger.Info("gRPC server started listening", zap.String("addr", addr))
		grpcServer.Serve(l)
	}()

	metricsrv := NewMetricsServer(cobrautil.MustGetString(cmd, "metrics-addr"))
	go func() {
		if err := metricsrv.ListenAndServe(); err != http.ErrServerClosed {
			logger.Fatal("failed while serving metrics", zap.Error(err))
		}
	}()

	signalctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)
	for {
		select {
		case <-signalctx.Done():
			logger.Info("received interrupt")
			grpcServer.GracefulStop()

			if err := metricsrv.Close(); err != nil {
				logger.Fatal("failed while shutting down metrics server", zap.Error(err))
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
	dispatch graph.Dispatcher,
	maxDepth uint16,
) {
	api.RegisterACLServiceServer(srv, services.NewACLServer(ds, dispatch, maxDepth))
	api.RegisterNamespaceServiceServer(srv, services.NewNamespaceServer(ds))
	api.RegisterWatchServiceServer(srv, services.NewWatchServer(ds))
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
