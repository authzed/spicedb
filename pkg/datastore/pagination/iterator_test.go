package pagination

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestDownstreamErrors(t *testing.T) {
	defaultPageSize := uint64(10)
	defaultSortOrder := options.ByResource
	defaultError := errors.New("something went wrong")
	ctx := context.Background()
	var nilIter *mockedIterator
	var nilRel *core.RelationTuple

	t.Run("OnReader", func(t *testing.T) {
		require := require.New(t)
		ds := &mockedReader{}
		ds.
			On("QueryRelationships", options.Cursor(nil), defaultSortOrder, defaultPageSize).
			Return(nilIter, defaultError)

		_, err := NewPaginatedIterator(ctx, ds, datastore.RelationshipsFilter{}, defaultPageSize, defaultSortOrder, nil)
		require.ErrorIs(err, defaultError)
		require.True(ds.AssertExpectations(t))
	})

	t.Run("OnIterator", func(t *testing.T) {
		require := require.New(t)

		iterMock := &mockedIterator{}
		iterMock.On("Next").Return(nilRel)
		iterMock.On("Err").Return(defaultError)
		iterMock.On("Close")

		ds := &mockedReader{}
		ds.
			On("QueryRelationships", options.Cursor(nil), defaultSortOrder, defaultPageSize).
			Return(iterMock, nil)

		iter, err := NewPaginatedIterator(ctx, ds, datastore.RelationshipsFilter{}, defaultPageSize, defaultSortOrder, nil)
		require.NoError(err)
		require.NotNil(iter)

		require.Nil(iter.Next())
		require.ErrorIs(iter.Err(), defaultError)

		iter.Close()
		require.Nil(iter.Next())
		require.ErrorIs(iter.Err(), datastore.ErrClosedIterator)

		require.True(iterMock.AssertExpectations(t))
		require.True(ds.AssertExpectations(t))
	})

	t.Run("OnIterator", func(t *testing.T) {
		require := require.New(t)

		iterMock := &mockedIterator{}
		iterMock.On("Next").Return(&core.RelationTuple{})
		iterMock.On("Cursor").Return(options.Cursor(nil), defaultError)
		iterMock.On("Close")

		ds := &mockedReader{}
		ds.
			On("QueryRelationships", options.Cursor(nil), defaultSortOrder, defaultPageSize).
			Return(iterMock, nil)

		iter, err := NewPaginatedIterator(ctx, ds, datastore.RelationshipsFilter{}, defaultPageSize, defaultSortOrder, nil)
		require.NoError(err)
		require.NotNil(iter)

		require.NotNil(iter.Next())
		require.NoError(iter.Err())

		cursor, err := iter.Cursor()
		require.Nil(cursor)
		require.ErrorIs(err, defaultError)

		iter.Close()
		require.Nil(iter.Next())
		require.ErrorIs(iter.Err(), datastore.ErrClosedIterator)

		require.True(iterMock.AssertExpectations(t))
		require.True(ds.AssertExpectations(t))
	})

	t.Run("OnReaderAfterSuccess", func(t *testing.T) {
		require := require.New(t)

		iterMock := &mockedIterator{}
		iterMock.On("Next").Return(&core.RelationTuple{}).Once()
		iterMock.On("Next").Return(nil).Once()
		iterMock.On("Err").Return(nil).Once()
		iterMock.On("Cursor").Return(options.Cursor(nil), nil).Once()
		iterMock.On("Close")

		ds := &mockedReader{}
		ds.
			On("QueryRelationships", options.Cursor(nil), defaultSortOrder, uint64(1)).
			Return(iterMock, nil).Once().
			On("QueryRelationships", options.Cursor(nil), defaultSortOrder, uint64(1)).
			Return(nil, defaultError).Once()

		iter, err := NewPaginatedIterator(ctx, ds, datastore.RelationshipsFilter{}, 1, defaultSortOrder, nil)
		require.NoError(err)
		require.NotNil(iter)

		require.NotNil(iter.Next())
		require.NoError(iter.Err())

		require.Nil(iter.Next())
		require.Error(iter.Err())
		iter.Close()
	})
}

func TestPaginatedIterator(t *testing.T) {
	testCases := []struct {
		order              options.SortOrder
		pageSize           uint64
		totalRelationships uint64
	}{
		{options.ByResource, 1, 0},
		{options.ByResource, 1, 1},
		{options.ByResource, 1, 10},
		{options.ByResource, 10, 10},
		{options.ByResource, 100, 10},
		{options.ByResource, 10, 1000},
		{options.ByResource, 9, 20},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d/%d-%d", tc.pageSize, tc.totalRelationships, tc.order), func(t *testing.T) {
			require := require.New(t)

			tpls := make([]*core.RelationTuple, 0, tc.totalRelationships)
			for i := 0; i < int(tc.totalRelationships); i++ {
				tpls = append(tpls, &core.RelationTuple{
					ResourceAndRelation: &core.ObjectAndRelation{
						Namespace: "document",
						ObjectId:  strconv.Itoa(i),
						Relation:  "owner",
					},
					Subject: &core.ObjectAndRelation{
						Namespace: "user",
						ObjectId:  strconv.Itoa(i),
						Relation:  datastore.Ellipsis,
					},
				})
			}

			ds := generateMock(tpls, tc.pageSize, options.ByResource)

			ctx := context.Background()
			iter, err := NewPaginatedIterator(ctx, ds, datastore.RelationshipsFilter{
				ResourceType: "unused",
			}, tc.pageSize, options.ByResource, nil)
			require.NoError(err)
			defer iter.Close()

			cursor, err := iter.Cursor()
			require.ErrorIs(err, datastore.ErrCursorEmpty)
			require.Nil(cursor)

			var count uint64
			for tpl := iter.Next(); tpl != nil; tpl = iter.Next() {
				count++
				require.NoError(iter.Err())

				cursor, err := iter.Cursor()
				require.NoError(err)
				require.NotNil(cursor)
			}

			require.Equal(tc.totalRelationships, count)

			require.NoError(iter.Err())

			iter.Close()
			require.Nil(iter.Next())
			require.Error(iter.Err(), datastore.ErrClosedIterator)

			// Make sure everything got called
			require.True(ds.AssertExpectations(t))
		})
	}
}

func generateMock(tpls []*core.RelationTuple, pageSize uint64, order options.SortOrder) *mockedReader {
	mock := &mockedReader{}
	tplsLen := uint64(len(tpls))

	var last options.Cursor
	for i := uint64(0); i <= tplsLen; i += pageSize {
		pastLastIndex := i + pageSize
		if pastLastIndex > tplsLen {
			pastLastIndex = tplsLen
		}

		iter := common.NewSliceRelationshipIterator(tpls[i:pastLastIndex], order)
		mock.On("QueryRelationships", last, order, pageSize).Return(iter, nil)
		if tplsLen > 0 {
			last = tpls[pastLastIndex-1]
		}
	}

	return mock
}

type mockedReader struct {
	mock.Mock
}

var _ datastore.Reader = &mockedReader{}

func (m *mockedReader) QueryRelationships(
	_ context.Context,
	_ datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	queryOpts := options.NewQueryOptionsWithOptions(opts...)
	args := m.Called(queryOpts.After, queryOpts.Sort, *queryOpts.Limit)
	potentialRelIter := args.Get(0)
	if potentialRelIter == nil {
		return nil, args.Error(1)
	}
	return potentialRelIter.(datastore.RelationshipIterator), args.Error(1)
}

func (m *mockedReader) ReverseQueryRelationships(
	_ context.Context,
	_ datastore.SubjectsFilter,
	_ ...options.ReverseQueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	panic("not implemented")
}

func (m *mockedReader) ReadCaveatByName(_ context.Context, _ string) (caveat *core.CaveatDefinition, lastWritten datastore.Revision, err error) {
	panic("not implemented")
}

func (m *mockedReader) ListAllCaveats(_ context.Context) ([]datastore.RevisionedCaveat, error) {
	panic("not implemented")
}

func (m *mockedReader) LookupCaveatsWithNames(_ context.Context, _ []string) ([]datastore.RevisionedCaveat, error) {
	panic("not implemented")
}

func (m *mockedReader) ReadNamespaceByName(_ context.Context, _ string) (ns *core.NamespaceDefinition, lastWritten datastore.Revision, err error) {
	panic("not implemented")
}

func (m *mockedReader) ListAllNamespaces(_ context.Context) ([]datastore.RevisionedNamespace, error) {
	panic("not implemented")
}

func (m *mockedReader) LookupNamespacesWithNames(_ context.Context, _ []string) ([]datastore.RevisionedNamespace, error) {
	panic("not implemented")
}

type mockedIterator struct {
	mock.Mock
}

var _ datastore.RelationshipIterator = &mockedIterator{}

func (m *mockedIterator) Next() *core.RelationTuple {
	args := m.Called()
	potentialTuple := args.Get(0)
	if potentialTuple == nil {
		return nil
	}
	return potentialTuple.(*core.RelationTuple)
}

func (m *mockedIterator) Cursor() (options.Cursor, error) {
	args := m.Called()
	potentialCursor := args.Get(0)
	if potentialCursor == nil {
		return nil, args.Error(1)
	}
	return potentialCursor.(options.Cursor), args.Error(1)
}

func (m *mockedIterator) Err() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockedIterator) Close() {
	m.Called()
}
