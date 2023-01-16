package testserver

import (
	"context"
	"time"

	"github.com/authzed/spicedb/internal/middleware/servicespecific"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/middleware/consistency"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/middleware/logging"
)

// ServerConfig is configuration for the test server.
type ServerConfig struct {
	MaxUpdatesPerWrite    uint16
	MaxPreconditionsCount uint16
}

// NewTestServer creates a new test server, using defaults for the config.
func NewTestServer(require *require.Assertions,
	revisionQuantization time.Duration,
	gcWindow time.Duration,
	schemaPrefixRequired bool,
	dsInitFunc func(datastore.Datastore, *require.Assertions) (datastore.Datastore, datastore.Revision),
) (*grpc.ClientConn, func(), datastore.Datastore, datastore.Revision) {
	return NewTestServerWithConfig(require, revisionQuantization, gcWindow, schemaPrefixRequired,
		ServerConfig{
			MaxUpdatesPerWrite:    1000,
			MaxPreconditionsCount: 1000,
		},
		dsInitFunc)
}

// NewTestServerWithConfig creates as new test server with the specified config.
func NewTestServerWithConfig(require *require.Assertions,
	revisionQuantization time.Duration,
	gcWindow time.Duration,
	schemaPrefixRequired bool,
	config ServerConfig,
	dsInitFunc func(datastore.Datastore, *require.Assertions) (datastore.Datastore, datastore.Revision),
) (*grpc.ClientConn, func(), datastore.Datastore, datastore.Revision) {
	emptyDS, err := memdb.NewMemdbDatastore(0, revisionQuantization, gcWindow)
	require.NoError(err)
	ds, revision := dsInitFunc(emptyDS, require)
	ctx, cancel := context.WithCancel(context.Background())
	srv, err := server.NewConfigWithOptions(
		server.WithDatastore(ds),
		server.WithDispatcher(graph.NewLocalOnlyDispatcher(10)),
		server.WithDispatchMaxDepth(50),
		server.WithMaximumPreconditionCount(config.MaxPreconditionsCount),
		server.WithMaximumUpdatesPerWrite(config.MaxUpdatesPerWrite),
		server.WithGRPCServer(util.GRPCServerConfig{
			Network: util.BufferedNetwork,
			Enabled: true,
		}),
		server.WithSchemaPrefixesRequired(schemaPrefixRequired),
		server.WithGRPCAuthFunc(func(ctx context.Context) (context.Context, error) {
			return ctx, nil
		}),
		server.WithHTTPGateway(util.HTTPServerConfig{Enabled: false}),
		server.WithDashboardAPI(util.HTTPServerConfig{Enabled: false}),
		server.WithMetricsAPI(util.HTTPServerConfig{Enabled: false}),
		server.WithDispatchServer(util.GRPCServerConfig{Enabled: false}),
		server.WithExperimentalCaveatsEnabled(true),
		server.SetMiddlewareModification([]server.MiddlewareModification{
			{
				Operation: server.OperationReplaceAllUnsafe,
				Middlewares: []server.ReferenceableMiddleware{
					{
						Name:                "logging",
						UnaryMiddleware:     logging.UnaryServerInterceptor(),
						StreamingMiddleware: logging.StreamServerInterceptor(),
					},
					{
						Name:                "datastore",
						UnaryMiddleware:     datastoremw.UnaryServerInterceptor(ds),
						StreamingMiddleware: datastoremw.StreamServerInterceptor(ds),
					},
					{
						Name:                "consistency",
						UnaryMiddleware:     consistency.UnaryServerInterceptor(),
						StreamingMiddleware: consistency.StreamServerInterceptor(),
					},
					{
						Name:                "servicespecific",
						UnaryMiddleware:     servicespecific.UnaryServerInterceptor,
						StreamingMiddleware: servicespecific.StreamServerInterceptor,
					},
				},
			},
		}),
	).Complete(ctx)
	require.NoError(err)

	go func() {
		require.NoError(srv.Run(ctx))
	}()

	conn, err := srv.GRPCDialContext(ctx, grpc.WithBlock())
	require.NoError(err)

	return conn, func() {
		if conn != nil {
			require.NoError(conn.Close())
		}
		cancel()
	}, ds, revision
}
