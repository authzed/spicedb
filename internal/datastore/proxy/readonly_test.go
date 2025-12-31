package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/mocks"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func newReadOnlyMock(ctrl *gomock.Controller) (*mocks.MockDatastore, *mocks.MockReader) {
	dsMock := mocks.NewMockDatastore(ctrl)
	readerMock := mocks.NewMockReader(ctrl)

	dsMock.EXPECT().SnapshotReader(gomock.Any()).Return(readerMock).AnyTimes()

	return dsMock, readerMock
}

func TestRWOperationErrors(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	delegate, _ := newReadOnlyMock(ctrl)

	ds := NewReadonlyDatastore(delegate)
	ctx := t.Context()

	rev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.DeleteNamespaces(ctx, []string{"fake"}, datastore.DeleteNamespacesAndRelationships)
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

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	delegate, _ := newReadOnlyMock(ctrl)
	ds := NewReadonlyDatastore(delegate)
	ctx := t.Context()

	delegate.EXPECT().ReadyState(gomock.Any()).Return(datastore.ReadyState{IsReady: true}, nil).Times(1)

	resp, err := ds.ReadyState(ctx)
	require.NoError(err)
	require.True(resp.IsReady)
}

func TestOptimizedRevisionPassthrough(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	delegate, _ := newReadOnlyMock(ctrl)
	ds := NewReadonlyDatastore(delegate)
	ctx := t.Context()

	delegate.EXPECT().OptimizedRevision(gomock.Any()).Return(expectedRevision, nil).Times(1)

	revision, err := ds.OptimizedRevision(ctx)
	require.NoError(err)
	require.Equal(expectedRevision, revision)
}

func TestHeadRevisionPassthrough(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	delegate, _ := newReadOnlyMock(ctrl)
	ds := NewReadonlyDatastore(delegate)
	ctx := t.Context()

	delegate.EXPECT().HeadRevision(gomock.Any()).Return(expectedRevision, nil).Times(1)

	revision, err := ds.HeadRevision(ctx)
	require.NoError(err)
	require.Equal(expectedRevision, revision)
}

func TestCheckRevisionPassthrough(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	delegate, _ := newReadOnlyMock(ctrl)
	ds := NewReadonlyDatastore(delegate)
	ctx := t.Context()

	delegate.EXPECT().CheckRevision(gomock.Any(), expectedRevision).Return(nil).Times(1)

	err := ds.CheckRevision(ctx, expectedRevision)
	require.NoError(err)
}

func TestWatchPassthrough(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	delegate, _ := newReadOnlyMock(ctrl)
	ds := NewReadonlyDatastore(delegate)
	ctx := t.Context()

	delegate.EXPECT().Watch(gomock.Any(), expectedRevision, gomock.Any()).Return(
		make(<-chan datastore.RevisionChanges),
		make(<-chan error),
	).Times(1)

	ds.Watch(ctx, expectedRevision, datastore.WatchJustRelationships())
}

func TestSnapshotReaderPassthrough(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	delegate, reader := newReadOnlyMock(ctrl)
	ds := NewReadonlyDatastore(delegate)
	ctx := t.Context()

	reader.EXPECT().ReadNamespaceByName(gomock.Any(), "fake").Return(nil, expectedRevision, nil).Times(1)

	_, rev, err := ds.SnapshotReader(expectedRevision).ReadNamespaceByName(ctx, "fake")
	require.NoError(err)
	require.True(expectedRevision.Equal(rev))
}
