package testserver

import (
	"context"
	"fmt"
	"time"

	helpers "github.com/ecordell/optgen/helpers"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/gateway"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/middleware/pertoken"
	"github.com/authzed/spicedb/internal/middleware/readonly"
	"github.com/authzed/spicedb/internal/services"
	"github.com/authzed/spicedb/internal/services/health"
	v1svc "github.com/authzed/spicedb/internal/services/v1"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/authzed/spicedb/pkg/datastore"
)

const (
	maxDepth                = 50
	defaultConcurrencyLimit = 10
	defaultMaxChunkSize     = 100
)

//go:generate go run github.com/ecordell/optgen -output zz_generated.options.go . Config
type Config struct {
	GRPCServer                      util.GRPCServerConfig `debugmap:"visible"`
	ReadOnlyGRPCServer              util.GRPCServerConfig `debugmap:"visible"`
	HTTPGateway                     util.HTTPServerConfig `debugmap:"visible"`
	ReadOnlyHTTPGateway             util.HTTPServerConfig `debugmap:"visible"`
	LoadConfigs                     []string              `debugmap:"visible"`
	MaximumUpdatesPerWrite          uint16                `debugmap:"visible"`
	MaximumPreconditionCount        uint16                `debugmap:"visible"`
	MaxCaveatContextSize            int                   `debugmap:"visible"`
	MaxRelationshipContextSize      int                   `debugmap:"visible"`
	MaxReadRelationshipsLimit       uint32                `debugmap:"visible"`
	MaxDeleteRelationshipsLimit     uint32                `debugmap:"visible"`
	MaxLookupResourcesLimit         uint32                `debugmap:"visible"`
	MaxBulkExportRelationshipsLimit uint32                `debugmap:"visible"`
}

type RunnableTestServer interface {
	Run(ctx context.Context) error
	GRPCDialContext(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error)
	ReadOnlyGRPCDialContext(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error)
}

type datastoreReady struct{}

func (dr datastoreReady) ReadyState(_ context.Context) (datastore.ReadyState, error) {
	return datastore.ReadyState{IsReady: true}, nil
}

func (c *Config) Complete() (RunnableTestServer, error) {
	log.Ctx(context.Background()).Info().Fields(helpers.Flatten(c.DebugMap())).Msg("configuration")

	dispatcher := graph.NewLocalOnlyDispatcher(defaultConcurrencyLimit, defaultMaxChunkSize)

	datastoreMiddleware := pertoken.NewMiddleware(c.LoadConfigs)

	healthManager := health.NewHealthManager(dispatcher, &datastoreReady{})

	registerServices := func(srv *grpc.Server) {
		services.RegisterGrpcServices(
			srv,
			healthManager,
			dispatcher,
			services.V1SchemaServiceEnabled,
			services.WatchServiceEnabled,
			v1svc.PermissionsServerConfig{
				MaxPreconditionsCount:           c.MaximumPreconditionCount,
				MaxUpdatesPerWrite:              c.MaximumUpdatesPerWrite,
				MaximumAPIDepth:                 maxDepth,
				MaxCaveatContextSize:            c.MaxCaveatContextSize,
				MaxReadRelationshipsLimit:       c.MaxReadRelationshipsLimit,
				MaxDeleteRelationshipsLimit:     c.MaxDeleteRelationshipsLimit,
				MaxLookupResourcesLimit:         c.MaxLookupResourcesLimit,
				MaxBulkExportRelationshipsLimit: c.MaxBulkExportRelationshipsLimit,
				DispatchChunkSize:               defaultMaxChunkSize,
				ExpiringRelationshipsEnabled:    true,
			},
			1*time.Second,
		)
	}

	opts := *server.NewMiddlewareOptionWithOptions(server.WithAuthFunc(func(ctx context.Context) (context.Context, error) {
		// Turn off the default auth system.
		return ctx, nil
	}))
	opts = opts.WithDatastoreMiddleware(datastoreMiddleware)

	unaryMiddleware, err := server.DefaultUnaryMiddleware(opts)
	if err != nil {
		return nil, err
	}

	streamMiddleware, err := server.DefaultStreamingMiddleware(opts)
	if err != nil {
		return nil, err
	}

	gRPCSrv, err := c.GRPCServer.Complete(zerolog.InfoLevel, registerServices,
		grpc.ChainUnaryInterceptor(unaryMiddleware.ToGRPCInterceptors()...),
		grpc.ChainStreamInterceptor(streamMiddleware.ToGRPCInterceptors()...),
	)
	if err != nil {
		return nil, err
	}

	readOnlyGRPCSrv, err := c.ReadOnlyGRPCServer.Complete(zerolog.InfoLevel, registerServices,
		grpc.ChainUnaryInterceptor(
			append(unaryMiddleware.ToGRPCInterceptors(), readonly.UnaryServerInterceptor())...,
		),
		grpc.ChainStreamInterceptor(
			append(streamMiddleware.ToGRPCInterceptors(), readonly.StreamServerInterceptor())...,
		),
	)
	if err != nil {
		return nil, err
	}

	gatewayHandler, err := gateway.NewHandler(context.TODO(), c.GRPCServer.Address, c.GRPCServer.TLSCertPath)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize rest gateway")
	}

	if c.HTTPGateway.HTTPEnabled {
		log.Info().Msg("starting REST gateway")
	}

	gatewayServer, err := c.HTTPGateway.Complete(zerolog.InfoLevel, gatewayHandler)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize rest gateway: %w", err)
	}

	readOnlyGatewayHandler, err := gateway.NewHandler(context.TODO(), c.ReadOnlyGRPCServer.Address, c.ReadOnlyGRPCServer.TLSCertPath)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize rest gateway")
	}

	if c.ReadOnlyHTTPGateway.HTTPEnabled {
		log.Info().Msg("starting REST gateway")
	}

	readOnlyGatewayServer, err := c.ReadOnlyHTTPGateway.Complete(zerolog.InfoLevel, readOnlyGatewayHandler)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize rest gateway: %w", err)
	}

	return &completedTestServer{
		gRPCServer:            gRPCSrv,
		readOnlyGRPCServer:    readOnlyGRPCSrv,
		gatewayServer:         gatewayServer,
		readOnlyGatewayServer: readOnlyGatewayServer,
		healthManager:         healthManager,
	}, nil
}

type completedTestServer struct {
	gRPCServer         util.RunnableGRPCServer
	readOnlyGRPCServer util.RunnableGRPCServer

	gatewayServer         util.RunnableHTTPServer
	readOnlyGatewayServer util.RunnableHTTPServer

	healthManager health.Manager
}

func (c *completedTestServer) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	stopOnCancel := func(stopFn func()) func() error {
		return func() error {
			<-ctx.Done()
			stopFn()
			return nil
		}
	}

	g.Go(c.healthManager.Checker(ctx))

	g.Go(c.gRPCServer.Listen(ctx))
	g.Go(stopOnCancel(c.gRPCServer.GracefulStop))

	g.Go(c.readOnlyGRPCServer.Listen(ctx))
	g.Go(stopOnCancel(c.readOnlyGRPCServer.GracefulStop))

	g.Go(c.gatewayServer.ListenAndServe)
	g.Go(stopOnCancel(c.gatewayServer.Close))

	g.Go(c.readOnlyGatewayServer.ListenAndServe)
	g.Go(stopOnCancel(c.readOnlyGatewayServer.Close))

	if err := g.Wait(); err != nil {
		log.Ctx(ctx).Warn().Err(err).Msg("error shutting down servers")
	}

	return nil
}

func (c *completedTestServer) GRPCDialContext(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return c.gRPCServer.DialContext(ctx, opts...)
}

func (c *completedTestServer) ReadOnlyGRPCDialContext(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return c.readOnlyGRPCServer.DialContext(ctx, opts...)
}
