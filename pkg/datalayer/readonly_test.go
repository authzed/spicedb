package datalayer_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/authzed/spicedb/pkg/datalayer"
	mock_datalayer "github.com/authzed/spicedb/pkg/datalayer/mocks"
	"github.com/authzed/spicedb/pkg/datastore"
)

// fakeRevision is a simple datastore.Revision for unit tests.
type fakeRevision string

func (r fakeRevision) String() string                      { return string(r) }
func (r fakeRevision) Equal(o datastore.Revision) bool     { return r.String() == o.String() }
func (r fakeRevision) GreaterThan(datastore.Revision) bool { return false }
func (r fakeRevision) LessThan(datastore.Revision) bool    { return false }
func (r fakeRevision) ByteSortable() bool                  { return false }

func TestReadonlyDL_ReadWriteTx_ReturnsError(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dl := mock_datalayer.NewMockDataLayer(ctrl)
	rdl := datalayer.NewReadonlyDataLayer(dl)

	rev, err := rdl.ReadWriteTx(t.Context(), func(_ context.Context, _ datalayer.ReadWriteTransaction) error {
		t.Fatal("should not be called")
		return nil
	})
	require.Error(t, err)
	require.Equal(t, datastore.NoRevision, rev)

	var roErr datastore.ReadOnlyError
	require.ErrorAs(t, err, &roErr)
}

func TestReadonlyDL_SnapshotReader_DelegatesToUnderlying(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockReader := mock_datalayer.NewMockRevisionedReader(ctrl)
	mockReader.EXPECT().QueryRelationships(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)

	dl := mock_datalayer.NewMockDataLayer(ctrl)
	dl.EXPECT().SnapshotReader(gomock.Any(), datalayer.NoSchemaHashForTesting).Return(mockReader).Times(1)

	rdl := datalayer.NewReadonlyDataLayer(dl)
	reader := rdl.SnapshotReader(fakeRevision("r1"), datalayer.NoSchemaHashForTesting)
	require.NotNil(t, reader)

	_, _ = reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{})
}

func TestReadonlyDL_OptimizedRevision_Delegates(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dl := mock_datalayer.NewMockDataLayer(ctrl)
	dl.EXPECT().OptimizedRevision(gomock.Any()).Return(fakeRevision("opt"), datalayer.SchemaHash("hash"), nil).Times(1)

	rdl := datalayer.NewReadonlyDataLayer(dl)
	rev, hash, err := rdl.OptimizedRevision(t.Context())
	require.NoError(t, err)
	require.Equal(t, fakeRevision("opt"), rev)
	require.Equal(t, datalayer.SchemaHash("hash"), hash)
}

func TestReadonlyDL_HeadRevision_Delegates(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dl := mock_datalayer.NewMockDataLayer(ctrl)
	dl.EXPECT().HeadRevision(gomock.Any()).Return(fakeRevision("head"), datalayer.SchemaHash("hash"), nil).Times(1)

	rdl := datalayer.NewReadonlyDataLayer(dl)
	rev, hash, err := rdl.HeadRevision(t.Context())
	require.NoError(t, err)
	require.Equal(t, fakeRevision("head"), rev)
	require.Equal(t, datalayer.SchemaHash("hash"), hash)
}

func TestReadonlyDL_CheckRevision_Delegates(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dl := mock_datalayer.NewMockDataLayer(ctrl)
	dl.EXPECT().CheckRevision(gomock.Any(), gomock.Any()).Return(nil).Times(1)

	rdl := datalayer.NewReadonlyDataLayer(dl)
	err := rdl.CheckRevision(t.Context(), fakeRevision("r1"))
	require.NoError(t, err)
}

func TestReadonlyDL_RevisionFromString_Delegates(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dl := mock_datalayer.NewMockDataLayer(ctrl)
	dl.EXPECT().RevisionFromString("abc").Return(fakeRevision("abc"), nil).Times(1)

	rdl := datalayer.NewReadonlyDataLayer(dl)
	rev, err := rdl.RevisionFromString("abc")
	require.NoError(t, err)
	require.Equal(t, fakeRevision("abc"), rev)
}

func TestReadonlyDL_Watch_Delegates(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dl := mock_datalayer.NewMockDataLayer(ctrl)
	dl.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)

	rdl := datalayer.NewReadonlyDataLayer(dl)
	ch, errCh := rdl.Watch(t.Context(), fakeRevision("r1"), datastore.WatchOptions{})
	require.Nil(t, ch)
	require.Nil(t, errCh)
}

func TestReadonlyDL_ReadyState_Delegates(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dl := mock_datalayer.NewMockDataLayer(ctrl)
	dl.EXPECT().ReadyState(gomock.Any()).Return(datastore.ReadyState{IsReady: true}, nil).Times(1)

	rdl := datalayer.NewReadonlyDataLayer(dl)
	state, err := rdl.ReadyState(t.Context())
	require.NoError(t, err)
	require.True(t, state.IsReady)
}

func TestReadonlyDL_Features_Delegates(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dl := mock_datalayer.NewMockDataLayer(ctrl)
	dl.EXPECT().Features(gomock.Any()).Return(nil, nil).Times(1)

	rdl := datalayer.NewReadonlyDataLayer(dl)
	f, err := rdl.Features(t.Context())
	require.NoError(t, err)
	require.Nil(t, f)
}

func TestReadonlyDL_OfflineFeatures_Delegates(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dl := mock_datalayer.NewMockDataLayer(ctrl)
	dl.EXPECT().OfflineFeatures().Return(nil, nil).Times(1)

	rdl := datalayer.NewReadonlyDataLayer(dl)
	f, err := rdl.OfflineFeatures()
	require.NoError(t, err)
	require.Nil(t, f)
}

func TestReadonlyDL_Statistics_Delegates(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dl := mock_datalayer.NewMockDataLayer(ctrl)
	dl.EXPECT().Statistics(gomock.Any()).Return(datastore.Stats{}, nil).Times(1)

	rdl := datalayer.NewReadonlyDataLayer(dl)
	_, err := rdl.Statistics(t.Context())
	require.NoError(t, err)
}

func TestReadonlyDL_UniqueID_Delegates(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dl := mock_datalayer.NewMockDataLayer(ctrl)
	dl.EXPECT().UniqueID(gomock.Any()).Return("uid", nil).Times(1)

	rdl := datalayer.NewReadonlyDataLayer(dl)
	uid, err := rdl.UniqueID(t.Context())
	require.NoError(t, err)
	require.Equal(t, "uid", uid)
}

func TestReadonlyDL_MetricsID_Delegates(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dl := mock_datalayer.NewMockDataLayer(ctrl)
	dl.EXPECT().MetricsID().Return("mid", nil).Times(1)

	rdl := datalayer.NewReadonlyDataLayer(dl)
	mid, err := rdl.MetricsID()
	require.NoError(t, err)
	require.Equal(t, "mid", mid)
}

func TestReadonlyDL_Close_Delegates(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dl := mock_datalayer.NewMockDataLayer(ctrl)
	dl.EXPECT().Close().Return(nil).Times(1)

	rdl := datalayer.NewReadonlyDataLayer(dl)
	err := rdl.Close()
	require.NoError(t, err)
}

func TestReadonlyDL_Close_PropagatesError(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dl := mock_datalayer.NewMockDataLayer(ctrl)
	dl.EXPECT().Close().Return(errors.New("close err")).Times(1)

	rdl := datalayer.NewReadonlyDataLayer(dl)
	err := rdl.Close()
	require.ErrorContains(t, err, "close err")
}

func TestReadonlyDL_UnwrapDatastore_ReturnsNilForMock(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dl := mock_datalayer.NewMockDataLayer(ctrl)
	rdl := datalayer.NewReadonlyDataLayer(dl)

	require.Nil(t, datalayer.UnwrapDatastore(rdl))
}
