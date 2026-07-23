package testserver

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
	"github.com/authzed/spicedb/pkg/query"
)

// ServerConfig is configuration for the test server.
type ServerConfig struct {
	MaxUpdatesPerWrite                 uint16
	MaxPreconditionsCount              uint16
	MaxRelationshipContextSize         int
	StreamingAPITimeout                time.Duration
	CaveatTypeSet                      *caveattypes.TypeSet
	EnableExperimentalLookupResources3 bool
	DataLayerOpts                      []datalayer.DataLayerOption

	// MetricsRegistry, when non-nil, is the Prometheus registry the server's
	// metrics are registered with, allowing tests to make assertions on them.
	MetricsRegistry prometheus.Registerer
}

var DefaultTestServerConfig = ServerConfig{
	EnableExperimentalLookupResources3: true,
}

type DatastoreInitFunc func(testing.TB, datastore.Datastore) (datastore.Datastore, datastore.Revision)

// NewTestServerWithConfig creates as new test server with the specified config.
func NewTestServerWithConfig(t testing.TB, revisionQuantization time.Duration, gcWindow time.Duration, schemaPrefixRequired bool, config ServerConfig, dsInitFunc DatastoreInitFunc) (*grpc.ClientConn, datastore.Datastore, datastore.Revision) {
	emptyDS, err := memdb.NewMemdbDatastore(0, revisionQuantization, gcWindow)
	require.NoError(t, err)

	return NewTestServerWithConfigAndDatastore(t, schemaPrefixRequired, config, emptyDS, dsInitFunc)
}

func NewTestServerWithConfigAndDatastore(t testing.TB, schemaPrefixRequired bool, config ServerConfig, emptyDS datastore.Datastore, dsInitFunc DatastoreInitFunc) (*grpc.ClientConn, datastore.Datastore, datastore.Revision) {
	ds, revision := dsInitFunc(t, emptyDS)
	cts := caveattypes.TypeSetOrDefault(config.CaveatTypeSet)

	lrver := ""
	if config.EnableExperimentalLookupResources3 {
		lrver = "lr3"
	}

	params, err := graph.NewDefaultDispatcherParametersForTesting()
	require.NoError(t, err)

	params.TypeSet = cts
	queryPlanMetadata := query.NewQueryPlanMetadata()
	params.QueryPlanMetadata = queryPlanMetadata

	dispatcher, err := graph.NewLocalOnlyDispatcher(params)
	require.NoError(t, err)

	metricsRegistry := config.MetricsRegistry
	if metricsRegistry == nil {
		metricsRegistry = prometheus.NewRegistry()
	}

	cfg := server.NewConfigWithOptionsAndDefaults(
		server.WithDatastore(ds),
		server.WithDispatcher(dispatcher),
		server.WithOTel(*server.NewOTelConfigWithOptionsAndDefaults(
			server.WithPrometheusRegistry(metricsRegistry),
		)),
		server.WithTelemetryEndpoint(""),
		server.WithSilentlyDisableTelemetry(true),
		server.WithQueryPlanMetadata(queryPlanMetadata),
		server.WithMaximumPreconditionCount(config.MaxPreconditionsCount),
		server.WithMaximumUpdatesPerWrite(config.MaxUpdatesPerWrite),
		server.WithStreamingAPITimeout(config.StreamingAPITimeout),
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
						Middleware: datalayer.UnaryServerInterceptor(datalayer.NewDataLayer(ds, config.DataLayerOpts...)),
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
						Middleware: datalayer.StreamServerInterceptor(datalayer.NewDataLayer(ds, config.DataLayerOpts...)),
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
	)
	srv, err := cfg.Complete(t.Context())
	require.NoError(t, err)

	done := make(chan bool, 1)
	go func() {
		_ = srv.Run(t.Context())
		done <- true
	}()

	conn, err := srv.NewClient()
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
		<-done
	})

	return conn, ds, revision
}
