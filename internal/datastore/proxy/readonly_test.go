package proxy

import (
	"context"
	"testing"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
)

func TestRWOperationErrors(t *testing.T) {
	require := require.New(t)

	ds := NewReadonlyDatastore(&delegateMock{})
	ctx := context.Background()

	rev, err := ds.DeleteNamespace(ctx, "fake")
	require.ErrorAs(err, &datastore.ErrReadOnly{})
	require.Equal(datastore.NoRevision, rev)

	rev, err = ds.WriteNamespace(ctx, &v0.NamespaceDefinition{Name: "user"})
	require.ErrorAs(err, &datastore.ErrReadOnly{})
	require.Equal(datastore.NoRevision, rev)

	rev, err = ds.WriteTuples(ctx, nil, []*v0.RelationTupleUpdate{
		{
			Operation: v0.RelationTupleUpdate_CREATE,
			Tuple: &v0.RelationTuple{
				ObjectAndRelation: &v0.ObjectAndRelation{
					Namespace: "user",
					ObjectId:  "test",
					Relation:  "boss",
				},
				User: &v0.User{
					UserOneof: &v0.User_Userset{
						Userset: &v0.ObjectAndRelation{
							Namespace: "user",
							ObjectId:  "boss",
							Relation:  datastore.Ellipsis,
						},
					},
				},
			},
		},
	})
	require.ErrorAs(err, &datastore.ErrReadOnly{})
	require.Equal(datastore.NoRevision, rev)
}

var expectedRevision = decimal.NewFromInt(123)

func TestRevisionPassthrough(t *testing.T) {
	require := require.New(t)

	delegate := &delegateMock{}
	ds := NewReadonlyDatastore(delegate)
	ctx := context.Background()

	delegate.On("Revision").Return(expectedRevision, nil).Times(1)

	revision, err := ds.Revision(ctx)
	require.NoError(err)
	require.Equal(expectedRevision, revision)
	delegate.AssertExpectations(t)
}

func TestSyncRevisionPassthrough(t *testing.T) {
	require := require.New(t)

	delegate := &delegateMock{}
	ds := NewReadonlyDatastore(delegate)
	ctx := context.Background()

	delegate.On("SyncRevision").Return(expectedRevision, nil).Times(1)

	revision, err := ds.SyncRevision(ctx)
	require.NoError(err)
	require.Equal(expectedRevision, revision)
	delegate.AssertExpectations(t)
}

func TestCheckRevisionPassthrough(t *testing.T) {
	require := require.New(t)

	delegate := &delegateMock{}
	ds := NewReadonlyDatastore(delegate)
	ctx := context.Background()

	delegate.On("CheckRevision", expectedRevision).Return(nil).Times(1)

	err := ds.CheckRevision(ctx, expectedRevision)
	require.NoError(err)
	delegate.AssertExpectations(t)
}

func TestWatchPassthrough(t *testing.T) {
	delegate := &delegateMock{}
	ds := NewReadonlyDatastore(delegate)
	ctx := context.Background()

	delegate.On("Watch", expectedRevision).Return(
		make(<-chan *datastore.RevisionChanges, 0),
		make(<-chan error, 0),
	).Times(1)

	ds.Watch(ctx, expectedRevision)
	delegate.AssertExpectations(t)
}

func TestReadNamespacePassthrough(t *testing.T) {
	require := require.New(t)

	delegate := &delegateMock{}
	ds := NewReadonlyDatastore(delegate)
	ctx := context.Background()

	delegate.On("ReadNamespace", "test").Return(&v0.NamespaceDefinition{}, expectedRevision, nil).Times(1)

	ns, revision, err := ds.ReadNamespace(ctx, "test")
	require.Equal(&v0.NamespaceDefinition{}, ns)
	require.Equal(expectedRevision, revision)
	require.NoError(err)
	delegate.AssertExpectations(t)
}

func TestQueryTuplesPassthrough(t *testing.T) {
	require := require.New(t)

	delegate := &delegateMock{}
	ds := NewReadonlyDatastore(delegate)

	delegate.On("QueryTuples", &v1.ObjectFilter{ObjectType: "test"}, expectedRevision).Return().Times(1)

	query := ds.QueryTuples(&v1.ObjectFilter{ObjectType: "test"}, expectedRevision)
	require.Nil(query)
	delegate.AssertExpectations(t)
}

func TestReverseQueryTuplesFromSubjectPassthrough(t *testing.T) {
	require := require.New(t)

	delegate := &delegateMock{}
	ds := NewReadonlyDatastore(delegate)

	delegate.On("ReverseQueryTuplesFromSubject", &v0.ObjectAndRelation{}, expectedRevision).Return().Times(1)

	query := ds.ReverseQueryTuplesFromSubject(&v0.ObjectAndRelation{}, expectedRevision)
	require.Nil(query)
	delegate.AssertExpectations(t)
}

func TestReverseQueryTuplesFromSubjectRelationPassthrough(t *testing.T) {
	require := require.New(t)

	delegate := &delegateMock{}
	ds := NewReadonlyDatastore(delegate)

	delegate.On("ReverseQueryTuplesFromSubjectRelation", "subject", "relation", expectedRevision).Return().Times(1)

	query := ds.ReverseQueryTuplesFromSubjectRelation("subject", "relation", expectedRevision)
	require.Nil(query)
	delegate.AssertExpectations(t)
}

func TestReverseQueryTuplesFromSubjectNamespacePassthrough(t *testing.T) {
	require := require.New(t)

	delegate := &delegateMock{}
	ds := NewReadonlyDatastore(delegate)

	delegate.On("ReverseQueryTuplesFromSubjectNamespace", "somenamespace", expectedRevision).Return().Times(1)

	query := ds.ReverseQueryTuplesFromSubjectNamespace("somenamespace", expectedRevision)
	require.Nil(query)
	delegate.AssertExpectations(t)
}

func TestListNamespacesPassthrough(t *testing.T) {
	require := require.New(t)

	delegate := &delegateMock{}
	ds := NewReadonlyDatastore(delegate)
	ctx := context.Background()

	delegate.On("ListNamespaces").Return([]*v0.NamespaceDefinition{}, nil).Times(1)

	nsDefs, err := ds.ListNamespaces(ctx)
	require.Equal([]*v0.NamespaceDefinition{}, nsDefs)
	require.NoError(err)
	delegate.AssertExpectations(t)
}

type delegateMock struct {
	mock.Mock
}

func (dm *delegateMock) WriteTuples(ctx context.Context, preconditions []*v0.RelationTuple, mutations []*v0.RelationTupleUpdate) (datastore.Revision, error) {
	panic("shouldn't ever call write method on delegate")
}

func (dm *delegateMock) Revision(ctx context.Context) (datastore.Revision, error) {
	args := dm.Called()
	return args.Get(0).(datastore.Revision), args.Error(1)
}

func (dm *delegateMock) SyncRevision(ctx context.Context) (datastore.Revision, error) {
	args := dm.Called()
	return args.Get(0).(datastore.Revision), args.Error(1)
}

func (dm *delegateMock) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	args := dm.Called(afterRevision)

	return args.Get(0).(<-chan *datastore.RevisionChanges), args.Get(1).(<-chan error)
}

func (dm *delegateMock) WriteNamespace(ctx context.Context, newConfig *v0.NamespaceDefinition) (datastore.Revision, error) {
	panic("shouldn't ever call write method on delegate")
}

func (dm *delegateMock) ReadNamespace(ctx context.Context, nsName string) (*v0.NamespaceDefinition, datastore.Revision, error) {
	args := dm.Called(nsName)
	return args.Get(0).(*v0.NamespaceDefinition), args.Get(1).(datastore.Revision), args.Error(2)
}

func (dm *delegateMock) DeleteNamespace(ctx context.Context, nsName string) (datastore.Revision, error) {
	panic("shouldn't ever call write method on delegate")
}

func (dm *delegateMock) QueryTuples(resourceFilter *v1.ObjectFilter, revision datastore.Revision) datastore.TupleQuery {
	dm.Called(resourceFilter, revision)
	return nil
}

func (dm *delegateMock) ReverseQueryTuplesFromSubject(subject *v0.ObjectAndRelation, revision datastore.Revision) datastore.ReverseTupleQuery {
	dm.Called(subject, revision)
	return nil
}

func (dm *delegateMock) ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation string, revision datastore.Revision) datastore.ReverseTupleQuery {
	dm.Called(subjectNamespace, subjectRelation, revision)
	return nil
}

func (dm *delegateMock) ReverseQueryTuplesFromSubjectNamespace(subjectNamespace string, revision datastore.Revision) datastore.ReverseTupleQuery {
	dm.Called(subjectNamespace, revision)
	return nil
}

func (dm *delegateMock) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	args := dm.Called(revision)
	return args.Error(0)
}

func (dm *delegateMock) ListNamespaces(ctx context.Context) ([]*v0.NamespaceDefinition, error) {
	args := dm.Called()
	return args.Get(0).([]*v0.NamespaceDefinition), args.Error(1)
}
