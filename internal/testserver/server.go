package testserver

import (
	"context"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/middleware/consistency"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/util"
)

func NewTestServer(require *require.Assertions,
	revisionFuzzingTimedelta time.Duration,
	gcWindow time.Duration,
	simulatedLatency time.Duration,
	schemaPrefixRequired bool,
	dsInitFunc func(datastore.Datastore, *require.Assertions) (datastore.Datastore, datastore.Revision),
) (*grpc.ClientConn, func(), decimal.Decimal) {
	emptyDS, err := memdb.NewMemdbDatastore(0, revisionFuzzingTimedelta, gcWindow, simulatedLatency)
	require.NoError(err)
	ds, revision := dsInitFunc(emptyDS, require)
	ns, err := namespace.NewCachingNamespaceManager(1*time.Second, nil)
	require.NoError(err)
	srv, err := server.NewConfigWithOptions(
		server.WithDatastore(ds),
		server.WithNamespaceCacheExpiration(1*time.Second),
		server.WithDispatcher(graph.NewLocalOnlyDispatcher(ns)),
		server.WithDispatchMaxDepth(50),
		server.WithGRPCServer(util.GRPCServerConfig{
			Network: util.BufferedNetwork,
			Enabled: true,
		}),
		server.WithSchemaPrefixesRequired(schemaPrefixRequired),
		server.WithNamespaceCacheExpiration(1*time.Second),
		server.WithGRPCAuthFunc(func(ctx context.Context) (context.Context, error) {
			return ctx, nil
		}),
		server.WithHTTPGateway(util.HTTPServerConfig{Enabled: false}),
		server.WithDashboardAPI(util.HTTPServerConfig{Enabled: false}),
		server.WithMetricsAPI(util.HTTPServerConfig{Enabled: false}),
		server.WithDispatchServer(util.GRPCServerConfig{Enabled: false}),
	).Complete()
	require.NoError(err)
	srv.SetMiddleware([]grpc.UnaryServerInterceptor{
		datastoremw.UnaryServerInterceptor(ds),
		consistency.UnaryServerInterceptor(),
		servicespecific.UnaryServerInterceptor,
	}, []grpc.StreamServerInterceptor{
		datastoremw.StreamServerInterceptor(ds),
		consistency.StreamServerInterceptor(),
		servicespecific.StreamServerInterceptor,
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		require.NoError(srv.Run(ctx))
	}()

	conn, err := srv.GRPCDialContext(ctx, grpc.WithBlock())
	require.NoError(err)

	return conn, func() {
		require.NoError(conn.Close())
		cancel()
	}, revision
}
