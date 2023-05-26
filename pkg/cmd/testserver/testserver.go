package testserver

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/gateway"
	log "github.com/authzed/spicedb/internal/logging"
	consistencymw "github.com/authzed/spicedb/internal/middleware/consistency"
	dispatchmw "github.com/authzed/spicedb/internal/middleware/dispatcher"
	"github.com/authzed/spicedb/internal/middleware/pertoken"
	"github.com/authzed/spicedb/internal/middleware/readonly"
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
	"github.com/authzed/spicedb/internal/services"
	"github.com/authzed/spicedb/internal/services/health"
	v1svc "github.com/authzed/spicedb/internal/services/v1"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/authzed/spicedb/pkg/datastore"
)

const maxDepth = 50

//go:generate go run github.com/ecordell/optgen -output zz_generated.options.go . Config
type Config struct {
	GRPCServer                 util.GRPCServerConfig `debugmap:"visible"`
	ReadOnlyGRPCServer         util.GRPCServerConfig `debugmap:"visible"`
	HTTPGateway                util.HTTPServerConfig `debugmap:"visible"`
	ReadOnlyHTTPGateway        util.HTTPServerConfig `debugmap:"visible"`
	LoadConfigs                []string              `debugmap:"visible"`
	MaximumUpdatesPerWrite     uint16                `debugmap:"visible"`
	MaximumPreconditionCount   uint16                `debugmap:"visible"`
	MaxCaveatContextSize       int                   `debugmap:"visible"`
	MaxRelationshipContextSize int                   `debugmap:"visible"`
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
	dispatcher := graph.NewLocalOnlyDispatcher(10)

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
				MaxPreconditionsCount: c.MaximumPreconditionCount,
				MaxUpdatesPerWrite:    c.MaximumUpdatesPerWrite,
				MaximumAPIDepth:       maxDepth,
				MaxCaveatContextSize:  c.MaxCaveatContextSize,
			},
		)
	}
	gRPCSrv, err := c.GRPCServer.Complete(zerolog.InfoLevel, registerServices,
		grpc.ChainUnaryInterceptor(
			datastoreMiddleware.UnaryServerInterceptor(),
			dispatchmw.UnaryServerInterceptor(dispatcher),
			consistencymw.UnaryServerInterceptor(),
			servicespecific.UnaryServerInterceptor,
		),
		grpc.ChainStreamInterceptor(
			datastoreMiddleware.StreamServerInterceptor(),
			dispatchmw.StreamServerInterceptor(dispatcher),
			consistencymw.StreamServerInterceptor(),
			servicespecific.StreamServerInterceptor,
		),
	)
	if err != nil {
		return nil, err
	}

	readOnlyGRPCSrv, err := c.ReadOnlyGRPCServer.Complete(zerolog.InfoLevel, registerServices,
		grpc.ChainUnaryInterceptor(
			datastoreMiddleware.UnaryServerInterceptor(),
			readonly.UnaryServerInterceptor(),
			dispatchmw.UnaryServerInterceptor(dispatcher),
			consistencymw.UnaryServerInterceptor(),
			servicespecific.UnaryServerInterceptor,
		),
		grpc.ChainStreamInterceptor(
			datastoreMiddleware.StreamServerInterceptor(),
			readonly.StreamServerInterceptor(),
			dispatchmw.StreamServerInterceptor(dispatcher),
			consistencymw.StreamServerInterceptor(),
			servicespecific.StreamServerInterceptor,
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
