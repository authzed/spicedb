package proxy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

// unwrapCapability stands in for the optional datastore extensions that callers
// reach for with datastore.UnwrapAs: GarbageCollectableDatastore,
// RepairableDatastore, BulkExportPartitioner, SQLDatastore and friends. None of
// the proxies implement them, so finding one depends entirely on UnwrapAs being
// able to walk the whole chain.
type unwrapCapability interface {
	datastore.Datastore
	Capability() string
}

type capableDatastore struct {
	fakeDatastore
}

func (capableDatastore) Capability() string { return "yes" }

func newCapableDatastore() capableDatastore {
	return capableDatastore{fakeDatastore{"capable", mustParseRevisionForTest("2"), nil}}
}

// TestProxiesPreserveUnwrapChain asserts that every datastore proxy in this
// package can be traversed by datastore.UnwrapAs. A proxy that embeds
// datastore.Datastore does not get an Unwrap method promoted, so forgetting to
// declare one silently hides every optional capability underneath it.
func TestProxiesPreserveUnwrapChain(t *testing.T) {
	inner := newCapableDatastore()
	require.NotNil(t, datastore.UnwrapAs[unwrapCapability](inner), "the bare datastore should satisfy the capability")

	for _, tc := range []struct {
		name string
		wrap func(t *testing.T, ds datastore.Datastore) datastore.Datastore
	}{
		{
			name: "readonly",
			wrap: func(_ *testing.T, ds datastore.Datastore) datastore.Datastore {
				return NewReadonlyDatastore(ds)
			},
		},
		{
			name: "observable",
			wrap: func(_ *testing.T, ds datastore.Datastore) datastore.Datastore {
				return NewObservableDatastoreProxy(ds)
			},
		},
		{
			name: "singleflight",
			wrap: func(_ *testing.T, ds datastore.Datastore) datastore.Datastore {
				return NewSingleflightDatastoreProxy(ds)
			},
		},
		{
			name: "optimized revision",
			wrap: func(_ *testing.T, ds datastore.Datastore) datastore.Datastore {
				return NewOptimizedRevisionProxy(ds, time.Second)
			},
		},
		{
			name: "strict replicated",
			wrap: func(t *testing.T, ds datastore.Datastore) datastore.Datastore {
				replicated, err := NewStrictReplicatedDatastore(ds, newCapableDatastore())
				require.NoError(t, err)
				return replicated
			},
		},
		{
			name: "checking replicated",
			wrap: func(t *testing.T, ds datastore.Datastore) datastore.Datastore {
				replicated, err := NewCheckingReplicatedDatastore(ds, newCapableDatastore())
				require.NoError(t, err)
				return replicated
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := tc.wrap(t, inner)
			require.NotNil(t, datastore.UnwrapAs[unwrapCapability](wrapped),
				"%s does not preserve the unwrap chain; declare an Unwrap() method on it", tc.name)
		})
	}
}

// TestReplicatedUnwrapReturnsPrimary pins the choice of what the replicated
// proxies unwrap to: the primary, never a replica. Garbage collection, repair
// and bulk-export partitioning all belong to the primary.
func TestReplicatedUnwrapReturnsPrimary(t *testing.T) {
	primary := fakeDatastore{"primary", mustParseRevisionForTest("2"), nil}
	replica := fakeDatastore{"replica", mustParseRevisionForTest("2"), nil}

	strict, err := NewStrictReplicatedDatastore(primary, replica)
	require.NoError(t, err)
	require.Equal(t, primary, strict.(datastore.UnwrappableDatastore).Unwrap())

	checking, err := NewCheckingReplicatedDatastore(primary, replica)
	require.NoError(t, err)
	require.Equal(t, primary, checking.(datastore.UnwrappableDatastore).Unwrap())
}
