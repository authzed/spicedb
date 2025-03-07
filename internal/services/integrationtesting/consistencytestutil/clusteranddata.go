package consistencytestutil

import (
	"context"
	"testing"
	"time"

	"github.com/authzed/spicedb/internal/dispatch"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/validationfile"
)

const defaultConcurrencyLimit = 10

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

	ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, revisionDelta, memdb.DisableGC)
	require.NoError(err)

	return BuildDataAndCreateClusterForTesting(t, consistencyTestFilePath, ds, additionalServerOptions...)
}

// BuildDataAndCreateClusterForTesting loads the data found in a consistency test file,
// builds a cluster for it, and returns both the data and cluster.
func BuildDataAndCreateClusterForTesting(t *testing.T, consistencyTestFilePath string, ds datastore.Datastore, additionalServerOptions ...server.ConfigOption) ConsistencyClusterAndData {
	require := require.New(t)

	populated, revision, err := validationfile.PopulateFromFiles(context.Background(), ds, []string{consistencyTestFilePath})
	require.NoError(err)

	connections, cleanup := testserver.TestClusterWithDispatch(t, 1, ds, additionalServerOptions...)
	t.Cleanup(cleanup)

	dsCtx := datastoremw.ContextWithHandle(context.Background())
	require.NoError(datastoremw.SetInContext(dsCtx, ds))
	res := schema.ResolverForDatastoreReader(ds.SnapshotReader(revision))
	ts := schema.NewTypeSystem(res)

	// Validate the type system for each namespace.
	for _, nsDef := range populated.NamespaceDefinitions {
		_, err = ts.GetValidatedDefinition(dsCtx, nsDef.GetName())
		require.NoError(err)
	}

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
	dispatcher := graph.NewLocalOnlyDispatcher(defaultConcurrencyLimit, 100)
	if withCaching {
		cachingDispatcher, err := caching.NewCachingDispatcher(nil, false, "", &keys.CanonicalKeyHandler{})
		require.NoError(err)

		localDispatcher := graph.NewDispatcher(cachingDispatcher, graph.SharedConcurrencyLimits(10), 100)
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
