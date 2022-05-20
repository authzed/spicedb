package testserver

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/dispatch/graph"
	consistencymw "github.com/authzed/spicedb/internal/middleware/consistency"
	dispatchmw "github.com/authzed/spicedb/internal/middleware/dispatcher"
	"github.com/authzed/spicedb/internal/middleware/pertoken"
	"github.com/authzed/spicedb/internal/middleware/readonly"
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
	"github.com/authzed/spicedb/internal/services"
	v1alpha1svc "github.com/authzed/spicedb/internal/services/v1alpha1"
	"github.com/authzed/spicedb/pkg/cmd/util"
)

const maxDepth = 50

//go:generate go run github.com/ecordell/optgen -output zz_generated.options.go . Config
type Config struct {
	GRPCServer         util.GRPCServerConfig
	ReadOnlyGRPCServer util.GRPCServerConfig
	LoadConfigs        []string
}

type RunnableTestServer interface {
	Run(ctx context.Context) error
	GRPCDialContext(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error)
	ReadOnlyGRPCDialContext(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error)
}

func (c *Config) Complete() (RunnableTestServer, error) {
	dispatcher := graph.NewLocalOnlyDispatcher()

	datastoreMiddleware := pertoken.NewMiddleware(c.LoadConfigs)

	registerServices := func(srv *grpc.Server) {
		services.RegisterGrpcServices(
			srv,
			dispatcher,
			maxDepth,
			v1alpha1svc.PrefixNotRequired,
			services.V1SchemaServiceEnabled,
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

	return &completedTestServer{
		gRPCServer:         gRPCSrv,
		readOnlyGRPCServer: readOnlyGRPCSrv,
	}, nil
}

type completedTestServer struct {
	gRPCServer         util.RunnableGRPCServer
	readOnlyGRPCServer util.RunnableGRPCServer
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

	g.Go(c.gRPCServer.Listen)
	g.Go(stopOnCancel(c.gRPCServer.GracefulStop))
	g.Go(c.readOnlyGRPCServer.Listen)
	g.Go(stopOnCancel(c.readOnlyGRPCServer.GracefulStop))

	if err := g.Wait(); err != nil {
		log.Warn().Err(err).Msg("error shutting down servers")
	}

	return nil
}

func (c *completedTestServer) GRPCDialContext(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return c.gRPCServer.DialContext(ctx, opts...)
}

func (c *completedTestServer) ReadOnlyGRPCDialContext(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return c.readOnlyGRPCServer.DialContext(ctx, opts...)
}
