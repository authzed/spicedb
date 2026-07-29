package consistencytestutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/dispatch/keys"
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

// CreateDispatcherForTesting creates a dispatcher for consistency testing, with or without
// caching enabled.
func CreateDispatcherForTesting(t *testing.T, withCaching bool) dispatch.Dispatcher {
	require := require.New(t)
	params, err := graph.NewDefaultDispatcherParametersForTesting()
	require.NoError(err)
	dispatcher, err := graph.NewLocalOnlyDispatcher(params)
	require.NoError(err)
	if withCaching {
		cachingDispatcher, err := caching.NewCachingDispatcher(nil, dispatch.MetricsOptions{}, &keys.CanonicalKeyHandler{})
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
