package consistencytestutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/internal/testserver"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/validationfile"
)

// ConsistencyClusterAndData holds a connection to a SpiceDB "cluster" (size 1)
// running the V1 API for the given data.
type ConsistencyClusterAndData struct {
	Conn      *grpc.ClientConn
	DataStore datastore.Datastore
	Ctx       context.Context
	Populated *validationfile.PopulatedValidationFile
}

// LoadDataAndCreateClusterForTesting loads the data found in a consistency test file,
// builds a cluster for it, and returns both the data and cluster.
func LoadDataAndCreateClusterForTesting(t *testing.T, consistencyTestFilePath string, revisionDelta time.Duration, additionalServerOptions ...server.ConfigOption) ConsistencyClusterAndData {
	require := require.New(t)

	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, revisionDelta, memdb.DisableGC)
	require.NoError(err)

	return BuildDataAndCreateClusterForTesting(t, consistencyTestFilePath, ds, additionalServerOptions...)
}

// BuildDataAndCreateClusterForTesting loads the data found in a consistency test file,
// builds a cluster for it, and returns both the data and cluster.
func BuildDataAndCreateClusterForTesting(t *testing.T, consistencyTestFilePath string, ds datastore.Datastore, additionalServerOptions ...server.ConfigOption) ConsistencyClusterAndData {
	require := require.New(t)

	populated, _, err := validationfile.PopulateFromFiles(t.Context(), datalayer.NewDataLayer(ds), caveattypes.Default.TypeSet, []string{consistencyTestFilePath})
	require.NoError(err)

	connections, cleanup := testserver.TestClusterWithDispatch(t, 1, ds, additionalServerOptions...)
	t.Cleanup(cleanup)

	dl := datalayer.NewDataLayer(ds)

	dsCtx := datalayer.ContextWithHandle(t.Context())
	require.NoError(datalayer.SetInContext(dsCtx, dl))

	return ConsistencyClusterAndData{
		Conn:      connections[0],
		DataStore: ds,
		Ctx:       dsCtx,
		Populated: populated,
	}
}

// CreateDispatcherForTesting creates a dispatcher for consistency testing, with or without
// caching enabled.
func CreateDispatcherForTesting(t *testing.T, withCaching bool) dispatch.Dispatcher {
	require := require.New(t)
	params, err := graph.NewDefaultDispatcherParametersForTesting()
	require.NoError(err)
	dispatcher, err := graph.NewLocalOnlyDispatcher(params)
	require.NoError(err)
	if withCaching {
		cachingDispatcher, err := caching.NewCachingDispatcher(nil, false, "", &keys.CanonicalKeyHandler{})
		require.NoError(err)

		params2, err := graph.NewDefaultDispatcherParametersForTesting()
		require.NoError(err)
		params := params2
		params.ConcurrencyLimits = graph.SharedConcurrencyLimits(10)
		localDispatcher, err := graph.NewDispatcher(cachingDispatcher, params)
		require.NoError(err)
		t.Cleanup(func() {
			err := localDispatcher.Close()
			require.NoError(err)
		})

		cachingDispatcher.SetDelegate(localDispatcher)
		dispatcher = cachingDispatcher
	}
	t.Cleanup(func() {
		err := dispatcher.Close()
		require.NoError(err)
	})
	return dispatcher
}
