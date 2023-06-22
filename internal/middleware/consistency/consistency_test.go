package consistency

import (
	"context"
	"errors"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	"github.com/authzed/spicedb/pkg/cursor"
	"github.com/authzed/spicedb/pkg/datastore/revision"
	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

var (
	zero      = revision.NewFromDecimal(decimal.NewFromInt(0))
	optimized = revision.NewFromDecimal(decimal.NewFromInt(100))
	exact     = revision.NewFromDecimal(decimal.NewFromInt(123))
	head      = revision.NewFromDecimal(decimal.NewFromInt(145))
)

func TestAddRevisionToContextNoneSupplied(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(optimized, nil).Once()

	updated := ContextWithHandle(context.Background())
	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{}, ds)
	require.NoError(err)

	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(optimized.Equal(rev))
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextMinimizeLatency(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(optimized, nil).Once()

	updated := ContextWithHandle(context.Background())
	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_MinimizeLatency{
				MinimizeLatency: true,
			},
		},
	}, ds)
	require.NoError(err)

	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(optimized.Equal(rev))
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextFullyConsistent(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("HeadRevision").Return(head, nil).Once()

	updated := ContextWithHandle(context.Background())
	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_FullyConsistent{
				FullyConsistent: true,
			},
		},
	}, ds)
	require.NoError(err)

	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(head.Equal(rev))
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextAtLeastAsFresh(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(optimized, nil).Once()
	ds.On("RevisionFromString", exact.String()).Return(exact, nil).Once()

	updated := ContextWithHandle(context.Background())
	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(exact),
			},
		},
	}, ds)
	require.NoError(err)

	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(exact.Equal(rev))
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextAtValidExactSnapshot(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("CheckRevision", exact).Return(nil).Times(1)
	ds.On("RevisionFromString", exact.String()).Return(exact, nil).Once()

	updated := ContextWithHandle(context.Background())
	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.MustNewFromRevision(exact),
			},
		},
	}, ds)
	require.NoError(err)

	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(exact.Equal(rev))
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextAtInvalidExactSnapshot(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("CheckRevision", zero).Return(errors.New("bad revision")).Times(1)
	ds.On("RevisionFromString", zero.String()).Return(zero, nil).Once()

	updated := ContextWithHandle(context.Background())
	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.MustNewFromRevision(zero),
			},
		},
	}, ds)
	require.Error(err)
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextNoConsistencyAPI(t *testing.T) {
	require := require.New(t)

	updated := ContextWithHandle(context.Background())

	_, _, err := RevisionFromContext(updated)
	require.Error(err)
}

func TestAddRevisionToContextWithCursor(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("CheckRevision", optimized).Return(nil).Times(1)
	ds.On("RevisionFromString", optimized.String()).Return(optimized, nil).Once()

	// cursor is at `optimized`
	cursor, err := cursor.EncodeFromDispatchCursor(&dispatch.Cursor{}, "somehash", optimized)
	require.NoError(err)

	// revision in context is at `exact`
	updated := ContextWithHandle(context.Background())
	err = AddRevisionToContext(updated, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.MustNewFromRevision(exact),
			},
		},
		OptionalCursor: cursor,
	}, ds)
	require.NoError(err)

	// ensure we get back `optimized` from the cursor
	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(optimized.Equal(rev))
	ds.AssertExpectations(t)
}
