package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
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
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func registerServeLookupWatchCmd(rootCmd *cobra.Command) {
	serveCmd := &cobra.Command{
		Use:               "serve-lookupwatch",
		Short:             "serve the Loookup Watch API",
		Long:              "A database that stores, computes, and validates application permissions", // todo: fill out a better long description
		PersistentPreRunE: persistentPreRunE,
		Run:               serveLookupWatchRun,
		Example: fmt.Sprintf(`	%s:
		spicedb serve-lookupwatch --grpc-preshared-key "somerandomkeyhere" --grpc-no-tls --http-no-tls

	%s:
		spicedb serve --grpc-preshared-key "realkeyhere" --grpc-cert-path path/to/tls/cert --grpc-key-path path/to/tls/key \
			--http-cert-path path/to/tls/cert --http-key-path path/to/tls/key
`, color.YellowString("No TLS and in-memory"), color.GreenString("TLS and a real datastore")),
	}

	cobrautil.RegisterGrpcServerFlags(serveCmd.Flags())
	cobrautil.RegisterMetricsServerFlags(serveCmd.Flags())

	// Flags for the gRPC server beyond those provided from cobrautil
	serveCmd.Flags().String("grpc-preshared-key", "", "preshared key to require for authenticated requests")
	serveCmd.Flags().Duration("grpc-shutdown-grace-period", 0*time.Second, "amount of time after receiving sigint to continue serving")

	// Flags for the namespace manager
	serveCmd.Flags().Duration("ns-cache-expiration", 1*time.Minute, "amount of time a namespace entry should remain cached")

	// Flags for parsing and validating schemas.
	serveCmd.Flags().Bool("schema-prefixes-required", false, "require prefixes on all object definitions in schemas")

	// Flags for internal dispatch API
	serveCmd.Flags().String("internal-grpc-addr", ":50053", "address to listen for internal requests")

	// Flags for HTTP gateway
	serveCmd.Flags().String("http-addr", ":8443", "address to listen for HTTP API requests")
	serveCmd.Flags().Bool("http-no-tls", false, "serve HTTP API requests unencrypted")
	serveCmd.Flags().String("http-cert-path", "", "local path to the TLS certificate used to serve HTTP API requests")
	serveCmd.Flags().String("http-key-path", "", "local path to the TLS key used to serve HTTP API requests")

	// Flags for local dev dashboard
	serveCmd.Flags().String("dashboard-addr", ":8080", "address to listen for the dashboard")

	// Required flags.
	if err := serveCmd.MarkFlagRequired("grpc-preshared-key"); err != nil {
		panic("failed to mark flag as required: " + err.Error())
	}

	rootCmd.AddCommand(serveCmd)
}

func serveLookupWatchRun(cmd *cobra.Command, args []string) {

	token := cobrautil.MustGetStringExpanded(cmd, "grpc-preshared-key")
	if len(token) < 1 {
		log.Fatal().Msg("a preshared key must be provided via --grpc-preshared-key to authenticate API requests")
	}

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

	healthSrv := grpcutil.NewAuthlessHealthServer()

	v1alpha1.RegisterLookupWatchServiceServer(grpcServer, v1alpha1svc.NewLookupWatchServer())
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
	gatewaySrv, err := gateway.NewHTTPServer(context.TODO(), gateway.Config{
		Addr:                cobrautil.MustGetStringExpanded(cmd, "http-addr"),
		UpstreamAddr:        cobrautil.MustGetStringExpanded(cmd, "grpc-addr"),
		UpstreamTLSDisabled: cobrautil.MustGetBool(cmd, "grpc-no-tls"),
		UpstreamTLSCertPath: cobrautil.MustGetStringExpanded(cmd, "grpc-cert-path"),
	})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize rest gateway")
	}
	go func() {
		log.Info().Str("addr", gatewaySrv.Addr).Msg("rest gateway server started listening")
		if cobrautil.MustGetBool(cmd, "http-no-tls") {
			if err := gatewaySrv.ListenAndServe(); err != http.ErrServerClosed {
				log.Fatal().Err(err).Msg("failed while serving rest gateway")
			}
		} else {
			certPath := cobrautil.MustGetStringExpanded(cmd, "http-cert-path")
			keyPath := cobrautil.MustGetStringExpanded(cmd, "http-key-path")
			if certPath == "" || keyPath == "" {
				errStr := "failed to start http server: must provide either --http-no-tls or --http-cert-path and --http-key-path"
				log.Fatal().Err(errors.New(errStr)).Msg("failed to create http server")
			}

			if err := gatewaySrv.ListenAndServeTLS(certPath, keyPath); err != http.ErrServerClosed {
				log.Fatal().Err(err).Msg("failed while serving rest gateway")
			}
		}
	}()

	// Start the metrics endpoint.
	metricsrv := cobrautil.MetricsServerFromFlags(cmd)
	go func() {
		addr := cobrautil.MustGetStringExpanded(cmd, "metrics-addr")
		log.Info().Str("addr", addr).Msg("metrics server started listening")
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

	if err := gatewaySrv.Close(); err != nil {
		log.Fatal().Err(err).Msg("failed while shutting down rest gateway")
	}

	if err := metricsrv.Close(); err != nil {
		log.Fatal().Err(err).Msg("failed while shutting down metrics server")
	}
}
