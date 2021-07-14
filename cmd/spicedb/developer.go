package main

import (
	"context"
	"net"
	"os"
	"os/signal"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	grpczerolog "github.com/grpc-ecosystem/go-grpc-middleware/providers/zerolog/v2"
	grpclog "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jzelinskie/cobrautil"
	"github.com/jzelinskie/stringz"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	v0svc "github.com/authzed/spicedb/internal/services/v0"
	"github.com/authzed/spicedb/pkg/grpcutil"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
)

func developerServiceRun(cmd *cobra.Command, args []string) {
	var sharedOptions []grpc.ServerOption
	sharedOptions = append(sharedOptions, grpcmw.WithUnaryServerChain(
		otelgrpc.UnaryServerInterceptor(),
		grpcprom.UnaryServerInterceptor,
		grpclog.UnaryServerInterceptor(grpczerolog.InterceptorLogger(log.Logger)),
	))

	sharedOptions = append(sharedOptions, grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionAge: cobrautil.MustGetDuration(cmd, "grpc-max-conn-age"),
	}))

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

	shareStoreKind := cobrautil.MustGetString(cmd, "share-store")
	shareStoreSalt := cobrautil.MustGetString(cmd, "share-store-salt")

	var shareStore v0svc.ShareStore
	if shareStoreKind == "inmemory" {
		log.Info().Msg("using in-memory sharestore")
		shareStore = v0svc.NewInMemoryShareStore(shareStoreSalt)
	} else if shareStoreKind == "s3" {
		bucketName := cobrautil.MustGetString(cmd, "s3-bucket")
		if len(bucketName) == 0 {
			log.Fatal().Msg("missing --s3-bucket for s3 share store")
		}

		accessKey := cobrautil.MustGetString(cmd, "s3-access-key")
		if len(accessKey) == 0 {
			log.Fatal().Msg("missing --s3-access-key for s3 share store")
		}

		secretKey := cobrautil.MustGetString(cmd, "s3-secret-key")
		if len(secretKey) == 0 {
			log.Fatal().Msg("missing --s3-secret-key for s3 share store")
		}

		endpoint := cobrautil.MustGetString(cmd, "s3-endpoint")
		if len(endpoint) == 0 {
			log.Fatal().Msg("missing --s3-endpoint for s3 share store")
		}

		region := stringz.DefaultEmpty(cobrautil.MustGetString(cmd, "s3-region"), "auto")

		config := &aws.Config{
			Credentials: credentials.NewStaticCredentials(
				accessKey,
				secretKey,
				"",
			),
			Endpoint: aws.String(endpoint),
			Region:   aws.String(region),
		}

		s3store, err := v0svc.NewS3ShareStore(bucketName, shareStoreSalt, config)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to create S3 share store")
		}
		shareStore = s3store

		log.Info().Str("endpoint", endpoint).Str("region", region).Str("bucket-name", bucketName).Str("access-key", accessKey).Msg("using S3 sharestore")
	} else {
		log.Fatal().Str("share-store", shareStoreKind).Msg("unknown share store type")
	}

	healthSrv := grpcutil.NewAuthlessHealthServer()

	v0.RegisterDeveloperServiceServer(grpcServer, v0svc.NewDeveloperServer(shareStore))
	healthSrv.SetServingStatus("DeveloperService", healthpb.HealthCheckResponse_SERVING)

	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	reflection.Register(grpcServer)

	go func() {
		addr := cobrautil.MustGetString(cmd, "grpc-addr")
		l, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatal().Str("addr", addr).Msg("failed to listen on addr for gRPC server")
		}

		log.Info().Str("addr", addr).Msg("gRPC server started listening")
		grpcServer.Serve(l)
	}()

	signalctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)
	for {
		select {
		case <-signalctx.Done():
			log.Info().Msg("received interrupt")
			grpcServer.GracefulStop()
			return
		}
	}
}
