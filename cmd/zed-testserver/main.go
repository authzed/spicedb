package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	v1alpha1 "github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	gcWindow                = 1 * time.Hour
	nsCacheExpiration       = 0 * time.Minute // No caching
	maxDepth                = 50
	revisionFuzzingDuration = 10 * time.Millisecond
)

func main() {
	persistentPreRunE := cobrautil.CommandStack(
		cobrautil.SyncViperPreRunE("zed-testserver"),
		cobrautil.ZeroLogPreRunE,
		cobrautil.OpenTelemetryPreRunE,
	)

	rootCmd := &cobra.Command{
		Use:               "zed-testserver",
		Short:             "Authzed local testing server",
		PersistentPreRunE: persistentPreRunE,
	}

	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Runs the Authzed local testing server",
		Run:   runTestServer,
	}

	runCmd.Flags().String("grpc-addr", ":50051", "address to listen on for serving gRPC services")
	runCmd.Flags().String("readonly-grpc-addr", ":50052", "address to listen on for serving read-only gRPC services")
	runCmd.Flags().StringSlice("load-configs", []string{}, "configuration yaml files to load")

	rootCmd.AddCommand(runCmd)
	cobrautil.RegisterZeroLogFlags(rootCmd.PersistentFlags())
	cobrautil.RegisterOpenTelemetryFlags(rootCmd.PersistentFlags(), rootCmd.Use)

	rootCmd.Execute()
}

func runTestServer(cmd *cobra.Command, args []string) {
	configFilePaths := cobrautil.MustGetStringSlice(cmd, "load-configs")

	readWriteMiddleware := &perTokenBackendMiddleware{
		&sync.Map{},
		configFilePaths,
		true,
	}
	readOnlyMiddleware := &perTokenBackendMiddleware{
		&sync.Map{},
		configFilePaths,
		true,
	}

	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(
		readWriteMiddleware.UnaryServerInterceptor(),
	), grpc.ChainStreamInterceptor(
		readWriteMiddleware.StreamServerInterceptor(),
	))
	readonlyServer := grpc.NewServer(grpc.ChainUnaryInterceptor(
		readOnlyMiddleware.UnaryServerInterceptor(),
	), grpc.ChainStreamInterceptor(
		readOnlyMiddleware.StreamServerInterceptor(),
	))

	for _, srv := range []*grpc.Server{grpcServer, readonlyServer} {
		v0.RegisterACLServiceServer(srv, &dummyBackend{})
		v0.RegisterNamespaceServiceServer(srv, &dummyBackend{})
		v1alpha1.RegisterSchemaServiceServer(srv, &dummyBackend{})
		v1.RegisterPermissionsServiceServer(srv, &dummyBackend{})
		reflection.Register(srv)
	}

	go func() {
		addr := cobrautil.MustGetString(cmd, "grpc-addr")
		l, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatal().Str("addr", addr).Msg("failed to listen on addr for gRPC server")
		}

		log.Info().Str("addr", addr).Msg("gRPC server started listening")
		grpcServer.Serve(l)
	}()

	go func() {
		addr := cobrautil.MustGetString(cmd, "readonly-grpc-addr")
		l, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatal().Str("addr", addr).Msg("failed to listen on readonly addr for gRPC server")
		}

		log.Info().Str("addr", addr).Msg("readonly gRPC server started listening")
		readonlyServer.Serve(l)
	}()

	signalctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

	<-signalctx.Done()

	log.Info().Msg("received interrupt")
	grpcServer.GracefulStop()
	readonlyServer.GracefulStop()
}

type dummyBackend struct {
	v0.UnimplementedACLServiceServer
	v0.UnimplementedNamespaceServiceServer
	v1alpha1.UnimplementedSchemaServiceServer
	v1.UnimplementedPermissionsServiceServer
}
