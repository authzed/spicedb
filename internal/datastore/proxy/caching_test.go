package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	"github.com/authzed/spicedb/pkg/datastore"
)

var (
	old  = decimal.NewFromInt(-100)
	zero = decimal.NewFromInt(0)
	one  = decimal.NewFromInt(1)
	two  = decimal.NewFromInt(2)
)

const (
	nsA = "namespace_a"
	nsB = "namespace_b"
)

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

	ds, err := NewCachingDatastoreProxy(dsMock, nil)
	require.NoError(err)

	_, updatedOneA, err := ds.SnapshotReader(one).ReadNamespace(ctx, nsA)
	require.NoError(err)
	require.Equal(old.IntPart(), updatedOneA.IntPart())

	_, updatedOneAAgain, err := ds.SnapshotReader(one).ReadNamespace(ctx, nsA)
	require.NoError(err)
	require.Equal(old.IntPart(), updatedOneAAgain.IntPart())

	_, updatedOneB, err := ds.SnapshotReader(one).ReadNamespace(ctx, nsB)
	require.NoError(err)
	require.Equal(zero.IntPart(), updatedOneB.IntPart())

	_, updatedOneBAgain, err := ds.SnapshotReader(one).ReadNamespace(ctx, nsB)
	require.NoError(err)
	require.Equal(zero.IntPart(), updatedOneBAgain.IntPart())

	_, updatedTwoA, err := ds.SnapshotReader(two).ReadNamespace(ctx, nsA)
	require.NoError(err)
	require.Equal(zero.IntPart(), updatedTwoA.IntPart())

	_, updatedTwoAAgain, err := ds.SnapshotReader(two).ReadNamespace(ctx, nsA)
	require.NoError(err)
	require.Equal(zero.IntPart(), updatedTwoAAgain.IntPart())

	_, updatedTwoB, err := ds.SnapshotReader(two).ReadNamespace(ctx, nsB)
	require.NoError(err)
	require.Equal(one.IntPart(), updatedTwoB.IntPart())

	_, updatedTwoBAgain, err := ds.SnapshotReader(two).ReadNamespace(ctx, nsB)
	require.NoError(err)
	require.Equal(one.IntPart(), updatedTwoBAgain.IntPart())

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

	ds, err := NewCachingDatastoreProxy(dsMock, nil)
	require.NoError(err)

	rev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		_, updatedA, err := rwt.ReadNamespace(ctx, nsA)
		require.NoError(err)
		require.Equal(zero.IntPart(), updatedA.IntPart())

		// This will not call out the mock RWT again, the mock will panic if it does.
		_, updatedA, err = rwt.ReadNamespace(ctx, nsA)
		require.NoError(err)
		require.Equal(zero.IntPart(), updatedA.IntPart())

		return nil
	})
	require.Equal(one.IntPart(), rev.IntPart())
	require.NoError(err)

	dsMock.AssertExpectations(t)
	rwtMock.AssertExpectations(t)
}

func TestSingleFlight(t *testing.T) {
	dsMock := &proxy_test.MockDatastore{}

	oneReader := &proxy_test.MockReader{}
	dsMock.On("SnapshotReader", one).Return(oneReader)
	oneReader.
		On("ReadNamespace", nsA).
		WaitUntil(time.After(10*time.Millisecond)).
		Return(nil, old, nil).
		Once()

	require := require.New(t)
	ctx := context.Background()

	ds, err := NewCachingDatastoreProxy(dsMock, nil)
	require.NoError(err)

	readNamespace := func() error {
		_, updatedAt, err := ds.SnapshotReader(one).ReadNamespace(ctx, nsA)
		require.NoError(err)
		require.Equal(old.IntPart(), updatedAt.IntPart())
		return err
	}

	g := errgroup.Group{}
	g.Go(readNamespace)
	g.Go(readNamespace)

	require.NoError(g.Wait())

	dsMock.AssertExpectations(t)
	oneReader.AssertExpectations(t)
}
