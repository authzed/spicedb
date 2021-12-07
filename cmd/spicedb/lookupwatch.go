package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	v1alpha1 "github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/grpcutil"
	"github.com/authzed/spicedb/internal/auth"
	"github.com/authzed/spicedb/internal/gateway"
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
	v1alpha1svc "github.com/authzed/spicedb/internal/services/v1alpha1"
	logmw "github.com/authzed/spicedb/pkg/middleware/logging"
	"github.com/authzed/spicedb/pkg/middleware/requestid"
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
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func registerServeLookupWatchCmd(rootCmd *cobra.Command) {
	serveCmd := &cobra.Command{
		Use:               "serve-lookupwatch",
		Short:             "serve the Loookup Watch API",
		Long:              "A database that stores, computes, and validates application permissions", // todo: fill out a better long description
		PersistentPreRunE: defaultPreRunE,
		Run:               serveLookupWatchRun,
		Example: fmt.Sprintf(`	%s:
		spicedb serve-lookupwatch --grpc-preshared-key "somerandomkeyhere"

	%s:
		spicedb serve --grpc-preshared-key "realkeyhere" --grpc-tls-cert-path path/to/tls/cert --grpc-tls-key-path path/to/tls/key \
			--http-tls-cert-path path/to/tls/cert --http-tls-key-path path/to/tls/key
`, color.YellowString("No TLS and in-memory"), color.GreenString("TLS and a real datastore")),
	}

	// Flags for the gRPC API server
	cobrautil.RegisterGrpcServerFlags(serveCmd.Flags(), "grpc", "gRPC", ":50051", true)
	serveCmd.Flags().String("grpc-preshared-key", "", "preshared key to require for authenticated requests")
	serveCmd.Flags().Duration("grpc-shutdown-grace-period", 0*time.Second, "amount of time after receiving sigint to continue serving")
	if err := serveCmd.MarkFlagRequired("grpc-preshared-key"); err != nil {
		panic("failed to mark flag as required: " + err.Error())
	}

	// Flags for HTTP gateway
	cobrautil.RegisterHttpServerFlags(serveCmd.Flags(), "http", "http", ":8443", false)

	// Flags for misc services
	cobrautil.RegisterHttpServerFlags(serveCmd.Flags(), "metrics", "metrics", ":9090", true)

	// Flags for local dev dashboard
	serveCmd.Flags().String("dashboard-addr", ":8080", "address to listen for the dashboard")

	// Required flags.

	rootCmd.AddCommand(serveCmd)
}

func serveLookupWatchRun(cmd *cobra.Command, args []string) {
	token := cobrautil.MustGetStringExpanded(cmd, "grpc-preshared-key")
	if len(token) < 1 {
		log.Fatal().Msg("a preshared key must be provided via --grpc-preshared-key to authenticate API requests")
	}

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

	healthSrv := grpcutil.NewAuthlessHealthServer()

	v1alpha1.RegisterLookupWatchServiceServer(grpcServer, v1alpha1svc.NewLookupWatchServer(nil, nil))
	healthSrv.SetServicesHealthy(&v1alpha1.LookupWatchService_ServiceDesc)

	healthpb.RegisterHealthServer(grpcServer, healthSrv)

	reflection.Register(grpcutil.NewAuthlessReflectionInterceptor(grpcServer))

	go func() {
		addr := cobrautil.MustGetStringExpanded(cmd, "grpc-addr")
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

	if err := gatewaySrv.Close(); err != nil {
		log.Fatal().Err(err).Msg("failed while shutting down rest gateway")
	}

	if err := metricsSrv.Close(); err != nil {
		log.Fatal().Err(err).Msg("failed while shutting down metrics server")
	}
}
