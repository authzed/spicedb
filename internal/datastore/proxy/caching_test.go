package proxy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revision"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/testutil"
)

var (
	old  = revision.NewFromDecimal(decimal.NewFromInt(-100))
	zero = revision.NewFromDecimal(decimal.NewFromInt(0))
	one  = revision.NewFromDecimal(decimal.NewFromInt(1))
	two  = revision.NewFromDecimal(decimal.NewFromInt(2))
)

const (
	nsA = "namespace_a"
	nsB = "namespace_b"
)

// TestNilUnmarshal asserts that if we get a nil NamespaceDefinition from a
// datastore implementation, the process of inserting it into the cache and
// back does not break anything.
func TestNilUnmarshal(t *testing.T) {
	nsDef := (*core.NamespaceDefinition)(nil)
	marshalled, err := nsDef.MarshalVT()
	require.Nil(t, err)

	var newDef *core.NamespaceDefinition
	err = nsDef.UnmarshalVT(marshalled)
	require.Nil(t, err)
	require.Equal(t, nsDef, newDef)
}

func TestSnapshotNamespaceCaching(t *testing.T) {
	dsMock := &proxy_test.MockDatastore{}

	oneReader := &proxy_test.MockReader{}
	dsMock.On("SnapshotReader", one).Return(oneReader)
	oneReader.On("ReadNamespace", nsA).Return(nil, old, nil).Once()
	oneReader.On("ReadNamespace", nsB).Return(nil, zero, nil).Once()

	twoReader := &proxy_test.MockReader{}
	dsMock.On("SnapshotReader", two).Return(twoReader)
	twoReader.On("ReadNamespace", nsA).Return(nil, zero, nil).Once()
	twoReader.On("ReadNamespace", nsB).Return(nil, one, nil).Once()

	require := require.New(t)
	ctx := context.Background()

	ds := NewCachingDatastoreProxy(dsMock, DatastoreProxyTestCache(t))

	_, updatedOneA, err := ds.SnapshotReader(one).ReadNamespace(ctx, nsA)
	require.NoError(err)
	require.True(old.Equal(updatedOneA))

	_, updatedOneAAgain, err := ds.SnapshotReader(one).ReadNamespace(ctx, nsA)
	require.NoError(err)
	require.True(old.Equal(updatedOneAAgain))

	_, updatedOneB, err := ds.SnapshotReader(one).ReadNamespace(ctx, nsB)
	require.NoError(err)
	require.True(zero.Equal(updatedOneB))

	_, updatedOneBAgain, err := ds.SnapshotReader(one).ReadNamespace(ctx, nsB)
	require.NoError(err)
	require.True(zero.Equal(updatedOneBAgain))

	_, updatedTwoA, err := ds.SnapshotReader(two).ReadNamespace(ctx, nsA)
	require.NoError(err)
	require.True(zero.Equal(updatedTwoA))

	_, updatedTwoAAgain, err := ds.SnapshotReader(two).ReadNamespace(ctx, nsA)
	require.NoError(err)
	require.True(zero.Equal(updatedTwoAAgain))

	_, updatedTwoB, err := ds.SnapshotReader(two).ReadNamespace(ctx, nsB)
	require.NoError(err)
	require.True(one.Equal(updatedTwoB))

	_, updatedTwoBAgain, err := ds.SnapshotReader(two).ReadNamespace(ctx, nsB)
	require.NoError(err)
	require.True(one.Equal(updatedTwoBAgain))

	dsMock.AssertExpectations(t)
	oneReader.AssertExpectations(t)
	twoReader.AssertExpectations(t)
}

func TestRWTNamespaceCaching(t *testing.T) {
	dsMock := &proxy_test.MockDatastore{}
	rwtMock := &proxy_test.MockReadWriteTransaction{}

	require := require.New(t)

	dsMock.On("ReadWriteTx").Return(rwtMock, one, nil).Once()
	rwtMock.On("ReadNamespace", nsA).Return(nil, zero, nil).Once()

	ctx := context.Background()

	ds := NewCachingDatastoreProxy(dsMock, nil)

	rev, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		_, updatedA, err := rwt.ReadNamespace(ctx, nsA)
		require.NoError(err)
		require.True(zero.Equal(updatedA))

		// This will not call out the mock RWT again, the mock will panic if it does.
		_, updatedA, err = rwt.ReadNamespace(ctx, nsA)
		require.NoError(err)
		require.True(zero.Equal(updatedA))

		return nil
	})
	require.True(one.Equal(rev))
	require.NoError(err)

	dsMock.AssertExpectations(t)
	rwtMock.AssertExpectations(t)
}

func TestRWTNamespaceCacheWithWrites(t *testing.T) {
	dsMock := &proxy_test.MockDatastore{}
	rwtMock := &proxy_test.MockReadWriteTransaction{}

	require := require.New(t)

	dsMock.On("ReadWriteTx").Return(rwtMock, one, nil).Once()
	notFoundErr := datastore.NewNamespaceNotFoundErr(nsA)
	rwtMock.On("ReadNamespace", nsA).Return(nil, zero, notFoundErr).Once()

	ctx := context.Background()

	ds := NewCachingDatastoreProxy(dsMock, nil)

	rev, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		// Cache the 404
		_, _, err := rwt.ReadNamespace(ctx, nsA)
		require.ErrorIs(err, notFoundErr)

		// This will not call out the mock RWT again, the mock will panic if it does.
		_, _, err = rwt.ReadNamespace(ctx, nsA)
		require.Error(err, notFoundErr)

		// Write nsA
		nsADef := &core.NamespaceDefinition{Name: nsA}
		rwtMock.On("WriteNamespaces", []*core.NamespaceDefinition{nsADef}).Return(nil).Once()
		require.NoError(rwt.WriteNamespaces(ctx, nsADef))

		// Call ReadNamespace on nsA and we should flow through to the mock
		rwtMock.On("ReadNamespace", nsA).Return(nsADef, zero, nil).Once()
		def, updatedA, err := rwt.ReadNamespace(ctx, nsA)

		require.True(updatedA.Equal(zero))
		require.NotNil(def)
		require.NoError(err)

		return nil
	})
	require.True(one.Equal(rev))
	require.NoError(err)

	dsMock.AssertExpectations(t)
	rwtMock.AssertExpectations(t)
}

func TestSingleFlight(t *testing.T) {
	dsMock := &proxy_test.MockDatastore{}

	ctx := context.Background()
	oneReader := &proxy_test.MockReader{}
	dsMock.On("SnapshotReader", one).Return(oneReader)
	oneReader.
		On("ReadNamespace", nsA).
		WaitUntil(time.After(10*time.Millisecond)).
		Return(nil, old, nil).
		Once()

	require := require.New(t)

	ds := NewCachingDatastoreProxy(dsMock, nil)

	readNamespace := func() error {
		_, updatedAt, err := ds.SnapshotReader(one).ReadNamespace(ctx, nsA)
		require.NoError(err)
		require.True(old.Equal(updatedAt))
		return err
	}

	g := errgroup.Group{}
	g.Go(readNamespace)
	g.Go(readNamespace)

	require.NoError(g.Wait())

	dsMock.AssertExpectations(t)
	oneReader.AssertExpectations(t)
}

func TestSnapshotNamespaceCachingRealDatastore(t *testing.T) {
	tcs := []struct {
		name          string
		nsDef         *core.NamespaceDefinition
		namespaceName string
	}{
		{
			"missing namespace",
			nil,
			"somenamespace",
		},
		{
			"defined namespace",
			ns.Namespace(
				"document",
				ns.Relation("owner",
					nil,
					ns.AllowedRelation("user", "..."),
				),
				ns.Relation("editor",
					nil,
					ns.AllowedRelation("user", "..."),
				),
			),
			"document",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
			require.NoError(t, err)

			ctx := context.Background()
			ds := NewCachingDatastoreProxy(rawDS, nil)

			if tc.nsDef != nil {
				_, err = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
					return rwt.WriteNamespaces(ctx, tc.nsDef)
				})
				require.NoError(t, err)
			}

			headRev, err := ds.HeadRevision(ctx)
			require.NoError(t, err)

			reader := ds.SnapshotReader(headRev)
			ns, _, _ := reader.ReadNamespace(ctx, tc.namespaceName)
			testutil.RequireProtoEqual(t, tc.nsDef, ns, "found different namespaces")

			ns2, _, _ := reader.ReadNamespace(ctx, tc.namespaceName)
			testutil.RequireProtoEqual(t, tc.nsDef, ns2, "found different namespaces")
		})
	}
}

type reader struct {
	proxy_test.MockReader
}

func (r *reader) ReadNamespace(ctx context.Context, namespace string) (ns *core.NamespaceDefinition, lastWritten datastore.Revision, err error) {
	time.Sleep(10 * time.Millisecond)
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil, old, fmt.Errorf("error")
	}
	return &core.NamespaceDefinition{Name: namespace}, old, nil
}

func TestSingleFlightCancelled(t *testing.T) {
	dsMock := &proxy_test.MockDatastore{}
	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	defer cancel1()

	dsMock.On("SnapshotReader", one).Return(&reader{MockReader: proxy_test.MockReader{}})

	ds := NewCachingDatastoreProxy(dsMock, nil)

	g := sync.WaitGroup{}
	var ns2 *core.NamespaceDefinition
	g.Add(2)
	go func() {
		_, _, _ = ds.SnapshotReader(one).ReadNamespace(ctx1, nsA)
		g.Done()
	}()
	go func() {
		time.Sleep(5 * time.Millisecond)
		ns2, _, _ = ds.SnapshotReader(one).ReadNamespace(ctx2, nsA)
		g.Done()
	}()
	cancel1()

	g.Wait()
	require.NotNil(t, ns2)
	require.Equal(t, nsA, ns2.Name)

	dsMock.AssertExpectations(t)
}
