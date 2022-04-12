package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/authzed/grpcutil"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	grpczerolog "github.com/grpc-ecosystem/go-grpc-middleware/providers/zerolog/v2"
	grpclog "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jzelinskie/cobrautil"
	"github.com/jzelinskie/stringz"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	v0svc "github.com/authzed/spicedb/internal/services/v0"
	"github.com/authzed/spicedb/pkg/cmd/server"
)

func RegisterDevtoolsFlags(cmd *cobra.Command) {
	cobrautil.RegisterGrpcServerFlags(cmd.Flags(), "grpc", "gRPC", ":50051", true)
	cobrautil.RegisterHTTPServerFlags(cmd.Flags(), "metrics", "metrics", ":9090", true)
	cobrautil.RegisterHTTPServerFlags(cmd.Flags(), "http", "download", ":8443", false)

	cmd.Flags().String("share-store", "inmemory", "kind of share store to use")
	cmd.Flags().String("share-store-salt", "", "salt for share store hashing")
	cmd.Flags().String("s3-access-key", "", "s3 access key for s3 share store")
	cmd.Flags().String("s3-secret-key", "", "s3 secret key for s3 share store")
	cmd.Flags().String("s3-bucket", "", "s3 bucket name for s3 share store")
	cmd.Flags().String("s3-endpoint", "", "s3 endpoint for s3 share store")
	cmd.Flags().String("s3-region", "auto", "s3 region for s3 share store")
}

func NewDevtoolsCommand(programName string) *cobra.Command {
	return &cobra.Command{
		Use:     "serve-devtools",
		Short:   "runs the developer tools service",
		Long:    "Serves the authzed.api.v0.DeveloperService which is used for development tooling such as the Authzed Playground",
		PreRunE: server.DefaultPreRunE(programName),
		RunE:    runfunc,
		Args:    cobra.ExactArgs(0),
	}
}

func runfunc(cmd *cobra.Command, args []string) error {
	grpcServer, err := cobrautil.GrpcServerFromFlags(cmd, "grpc", grpc.ChainUnaryInterceptor(
		grpclog.UnaryServerInterceptor(grpczerolog.InterceptorLogger(log.Logger)),
		otelgrpc.UnaryServerInterceptor(),
		grpcprom.UnaryServerInterceptor,
	))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create gRPC server")
	}

	shareStore, err := shareStoreFromCmd(cmd)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to configure share store")
	}

	registerDeveloperGrpcServices(grpcServer, shareStore)

	go func() {
		if err := cobrautil.GrpcListenFromFlags(cmd, "grpc", grpcServer, zerolog.InfoLevel); err != nil {
			log.Warn().Err(err).Msg("gRPC service did not shutdown cleanly")
		}
	}()

	// Start the metrics endpoint.
	metricsSrv := cobrautil.HTTPServerFromFlags(cmd, "metrics")
	metricsSrv.Handler = server.MetricsHandler(server.DisableTelemetryHandler)
	go func() {
		if err := cobrautil.HTTPListenFromFlags(cmd, "metrics", metricsSrv, zerolog.InfoLevel); err != nil {
			log.Fatal().Err(err).Msg("failed while serving metrics")
		}
	}()

	// start the http download api
	downloadSrv := cobrautil.HTTPServerFromFlags(cmd, "http")
	downloadSrv.Handler = v0svc.NewHTTPDownloadServer(cobrautil.MustGetString(cmd, "http-addr"), shareStore).Handler
	go func() {
		if err := cobrautil.HTTPListenFromFlags(cmd, "http", downloadSrv, zerolog.InfoLevel); err != nil {
			log.Fatal().Err(err).Msg("failed while serving download http api")
		}
	}()
	signalctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)
	<-signalctx.Done()
	log.Info().Msg("received interrupt")
	grpcServer.GracefulStop()
	if err := metricsSrv.Close(); err != nil {
		log.Fatal().Err(err).Msg("failed while shutting down metrics server")
	}

	return nil
}

func shareStoreFromCmd(cmd *cobra.Command) (v0svc.ShareStore, error) {
	shareStoreSalt := cobrautil.MustGetStringExpanded(cmd, "share-store-salt")
	shareStoreKind := cobrautil.MustGetStringExpanded(cmd, "share-store")
	event := log.Info()

	var shareStore v0svc.ShareStore
	switch shareStoreKind {
	case "inmemory":
		shareStore = v0svc.NewInMemoryShareStore(shareStoreSalt)

	case "s3":
		bucketName := cobrautil.MustGetStringExpanded(cmd, "s3-bucket")
		accessKey := cobrautil.MustGetStringExpanded(cmd, "s3-access-key")
		secretKey := cobrautil.MustGetStringExpanded(cmd, "s3-secret-key")
		endpoint := cobrautil.MustGetStringExpanded(cmd, "s3-endpoint")
		region := stringz.DefaultEmpty(cobrautil.MustGetStringExpanded(cmd, "s3-region"), "auto")

		optsNames := []string{"s3-bucket", "s3-access-key", "s3-secret-key", "s3-endpoint"}
		opts := []string{bucketName, accessKey, secretKey, endpoint}
		if i := stringz.SliceIndex(opts, ""); i >= 0 {
			return nil, fmt.Errorf("missing required field: %s", optsNames[i])
		}

		config := &aws.Config{
			Credentials: credentials.NewStaticCredentials(
				accessKey,
				secretKey,
				"",
			),
			Endpoint: aws.String(endpoint),
			Region:   aws.String(region),
		}

		var err error
		shareStore, err = v0svc.NewS3ShareStore(bucketName, shareStoreSalt, config)
		if err != nil {
			return nil, fmt.Errorf("failed to create S3 share store: %w", err)
		}

		event = event.Str("endpoint", endpoint).Str("region", region).Str("bucketName", bucketName).Str("accessKey", accessKey)

	default:
		return nil, errors.New("unknown share store")
	}

	event.Str("kind", shareStoreKind).Msg("configured share store")
	return shareStore, nil
}

func registerDeveloperGrpcServices(srv *grpc.Server, shareStore v0svc.ShareStore) {
	healthSrv := grpcutil.NewAuthlessHealthServer()

	v0.RegisterDeveloperServiceServer(srv, v0svc.NewDeveloperServer(shareStore))
	healthSrv.SetServingStatus("DeveloperService", healthpb.HealthCheckResponse_SERVING)

	healthpb.RegisterHealthServer(srv, healthSrv)
	reflection.Register(srv)
}
