package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func newReadOnlyMock() (*proxy_test.MockDatastore, *proxy_test.MockReader) {
	dsMock := &proxy_test.MockDatastore{}
	readerMock := &proxy_test.MockReader{}

	dsMock.On("ReadWriteTx").Panic("read-only proxy should never open a read-write transaction").Maybe()
	dsMock.On("SnapshotReader", mock.Anything).Return(readerMock).Maybe()

	return dsMock, readerMock
}

func TestRWOperationErrors(t *testing.T) {
	require := require.New(t)

	delegate, _ := newReadOnlyMock()

	ds := NewReadonlyDatastore(delegate)
	ctx := context.Background()

	rev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.DeleteNamespaces(ctx, "fake")
	})
	require.ErrorAs(err, &datastore.ReadOnlyError{})
	require.Equal(datastore.NoRevision, rev)

	rev, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, &core.NamespaceDefinition{Name: "user"})
	})
	require.ErrorAs(err, &datastore.ReadOnlyError{})
	require.Equal(datastore.NoRevision, rev)

	rev, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, tuple.MustParse("user:test#boss@user:boss"))
	require.ErrorAs(err, &datastore.ReadOnlyError{})
	require.Equal(datastore.NoRevision, rev)
}

var expectedRevision = revisions.NewForTransactionID(123)

func TestReadyStatePassthrough(t *testing.T) {
	require := require.New(t)

	delegate, _ := newReadOnlyMock()
	ds := NewReadonlyDatastore(delegate)
	ctx := context.Background()

	delegate.On("ReadyState").Return(datastore.ReadyState{IsReady: true}, nil).Times(1)

	resp, err := ds.ReadyState(ctx)
	require.NoError(err)
	require.True(resp.IsReady)
	delegate.AssertExpectations(t)
}

func TestOptimizedRevisionPassthrough(t *testing.T) {
	require := require.New(t)

	delegate, _ := newReadOnlyMock()
	ds := NewReadonlyDatastore(delegate)
	ctx := context.Background()

	delegate.On("OptimizedRevision").Return(expectedRevision, nil).Times(1)

	revision, err := ds.OptimizedRevision(ctx)
	require.NoError(err)
	require.Equal(expectedRevision, revision)
	delegate.AssertExpectations(t)
}

func TestHeadRevisionPassthrough(t *testing.T) {
	require := require.New(t)

	delegate, _ := newReadOnlyMock()
	ds := NewReadonlyDatastore(delegate)
	ctx := context.Background()

	delegate.On("HeadRevision").Return(expectedRevision, nil).Times(1)

	revision, err := ds.HeadRevision(ctx)
	require.NoError(err)
	require.Equal(expectedRevision, revision)
	delegate.AssertExpectations(t)
}

func TestCheckRevisionPassthrough(t *testing.T) {
	require := require.New(t)

	delegate, _ := newReadOnlyMock()
	ds := NewReadonlyDatastore(delegate)
	ctx := context.Background()

	delegate.On("CheckRevision", expectedRevision).Return(nil).Times(1)

	err := ds.CheckRevision(ctx, expectedRevision)
	require.NoError(err)
	delegate.AssertExpectations(t)
}

func TestWatchPassthrough(t *testing.T) {
	delegate, _ := newReadOnlyMock()
	ds := NewReadonlyDatastore(delegate)
	ctx := context.Background()

	delegate.On("Watch", expectedRevision).Return(
		make(<-chan datastore.RevisionChanges),
		make(<-chan error),
	).Times(1)

	ds.Watch(ctx, expectedRevision, datastore.WatchJustRelationships())
	delegate.AssertExpectations(t)
}

func TestSnapshotReaderPassthrough(t *testing.T) {
	require := require.New(t)

	delegate, reader := newReadOnlyMock()
	ds := NewReadonlyDatastore(delegate)
	ctx := context.Background()

	reader.On("ReadNamespaceByName", "fake").Return(nil, expectedRevision, nil).Times(1)

	_, rev, err := ds.SnapshotReader(expectedRevision).ReadNamespaceByName(ctx, "fake")
	require.NoError(err)
	require.True(expectedRevision.Equal(rev))
	delegate.AssertExpectations(t)
	reader.AssertExpectations(t)
}
