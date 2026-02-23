package testserver

import (
	"context"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	"github.com/authzed/spicedb/pkg/middleware/logging"
)

// ServerConfig is configuration for the test server.
type ServerConfig struct {
	MaxUpdatesPerWrite                 uint16
	MaxPreconditionsCount              uint16
	MaxRelationshipContextSize         int
	StreamingAPITimeout                time.Duration
	CaveatTypeSet                      *caveattypes.TypeSet
	EnableExperimentalLookupResources3 bool
}

var DefaultTestServerConfig = ServerConfig{
	MaxUpdatesPerWrite:                 1000,
	MaxPreconditionsCount:              1000,
	StreamingAPITimeout:                30 * time.Second,
	MaxRelationshipContextSize:         25000,
	EnableExperimentalLookupResources3: true,
}

type DatastoreInitFunc func(datastore.Datastore, *require.Assertions) (datastore.Datastore, datastore.Revision)

// NewTestServer creates a new test server, using defaults for the config.
func NewTestServer(require *require.Assertions,
	revisionQuantization time.Duration,
	gcWindow time.Duration,
	schemaPrefixRequired bool,
	dsInitFunc DatastoreInitFunc,
) (*grpc.ClientConn, func(), datastore.Datastore, datastore.Revision) {
	return NewTestServerWithConfig(require, revisionQuantization, gcWindow, schemaPrefixRequired,
		DefaultTestServerConfig,
		dsInitFunc)
}

// NewTestServerWithConfig creates as new test server with the specified config.
func NewTestServerWithConfig(require *require.Assertions,
	revisionQuantization time.Duration,
	gcWindow time.Duration,
	schemaPrefixRequired bool,
	config ServerConfig,
	dsInitFunc DatastoreInitFunc,
) (*grpc.ClientConn, func(), datastore.Datastore, datastore.Revision) {
	emptyDS, err := memdb.NewMemdbDatastore(0, revisionQuantization, gcWindow)
	require.NoError(err)

	return NewTestServerWithConfigAndDatastore(require, revisionQuantization, gcWindow, schemaPrefixRequired, config, emptyDS, dsInitFunc)
}

func NewTestServerWithConfigAndDatastore(require *require.Assertions,
	revisionQuantization time.Duration,
	gcWindow time.Duration,
	schemaPrefixRequired bool,
	config ServerConfig,
	emptyDS datastore.Datastore,
	dsInitFunc DatastoreInitFunc,
) (*grpc.ClientConn, func(), datastore.Datastore, datastore.Revision) {
	ds, revision := dsInitFunc(emptyDS, require)
	ctx, cancel := context.WithCancel(context.Background())
	cts := caveattypes.TypeSetOrDefault(config.CaveatTypeSet)

	lrver := ""
	if config.EnableExperimentalLookupResources3 {
		lrver = "lr3"
	}

	params, err := graph.NewDefaultDispatcherParametersForTesting()
	require.NoError(err)

	params.TypeSet = cts

	dispatcher, err := graph.NewLocalOnlyDispatcher(params)
	require.NoError(err)

	srv, err := server.NewConfigWithOptionsAndDefaults(
		server.WithDatastore(ds),
		server.WithDispatcher(dispatcher),
		server.WithDispatchMaxDepth(50),
		server.WithMaximumPreconditionCount(config.MaxPreconditionsCount),
		server.WithMaximumUpdatesPerWrite(config.MaxUpdatesPerWrite),
		server.WithStreamingAPITimeout(config.StreamingAPITimeout),
		server.WithMaxCaveatContextSize(4096),
		server.WithMaxRelationshipContextSize(config.MaxRelationshipContextSize),
		server.WithExperimentalLookupResourcesVersion(lrver),
		server.WithGRPCServer(util.GRPCServerConfig{
			Network: util.BufferedNetwork,
			Enabled: true,
		}),
		server.WithSchemaPrefixesRequired(schemaPrefixRequired),
		server.WithGRPCAuthFunc(func(ctx context.Context) (context.Context, error) {
			return ctx, nil
		}),
		server.WithHTTPGateway(util.HTTPServerConfig{HTTPEnabled: false}),
		server.WithMetricsAPI(util.HTTPServerConfig{HTTPEnabled: false}),
		server.WithDispatchServer(util.GRPCServerConfig{Enabled: false}),
		server.SetUnaryMiddlewareModification([]server.MiddlewareModification[grpc.UnaryServerInterceptor]{
			{
				Operation: server.OperationReplaceAllUnsafe,
				Middlewares: []server.ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
					{
						Name:       "logging",
						Middleware: logging.UnaryServerInterceptor(),
					},
					{
						Name:       "datastore",
						Middleware: datalayer.UnaryServerInterceptor(datalayer.NewDataLayer(ds)),
					},
					{
						Name:       "consistency",
						Middleware: consistency.UnaryServerInterceptor("testserver", consistency.TreatMismatchingTokensAsError),
					},
					{
						Name:       "servicespecific",
						Middleware: servicespecific.UnaryServerInterceptor,
					},
				},
			},
		}),
		server.SetStreamingMiddlewareModification([]server.MiddlewareModification[grpc.StreamServerInterceptor]{
			{
				Operation: server.OperationReplaceAllUnsafe,
				Middlewares: []server.ReferenceableMiddleware[grpc.StreamServerInterceptor]{
					{
						Name:       "logging",
						Middleware: logging.StreamServerInterceptor(),
					},
					{
						Name:       "datastore",
						Middleware: datalayer.StreamServerInterceptor(datalayer.NewDataLayer(ds)),
					},
					{
						Name:       "consistency",
						Middleware: consistency.StreamServerInterceptor("testserver", consistency.TreatMismatchingTokensAsError),
					},
					{
						Name:       "servicespecific",
						Middleware: servicespecific.StreamServerInterceptor,
					},
				},
			},
		}),
	).Complete(ctx)
	require.NoError(err)

	go func() {
		require.NoError(srv.Run(ctx))
	}()

	// TODO: move off of WithBlock
	conn, err := srv.GRPCDialContext(ctx, grpc.WithBlock()) // nolint: staticcheck
	require.NoError(err)

	return conn, func() {
		if conn != nil {
			require.NoError(conn.Close())
		}
		cancel()
	}, ds, revision
}
