package consistency

import (
	"context"
	"errors"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/cursor"
	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

var (
	zero      = revisions.NewForTransactionID(0)
	optimized = revisions.NewForTransactionID(100)
	exact     = revisions.NewForTransactionID(123)
	head      = revisions.NewForTransactionID(145)
)

func TestAddRevisionToContextNoneSupplied(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(optimized, nil).Once()

	updated := ContextWithHandle(context.Background())
	updated = datastoremw.ContextWithDatastore(updated, ds)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{}, ds, TreatMismatchingTokensAsError)
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
	updated = datastoremw.ContextWithDatastore(updated, ds)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_MinimizeLatency{
				MinimizeLatency: true,
			},
		},
	}, ds, TreatMismatchingTokensAsError)
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
	updated = datastoremw.ContextWithDatastore(updated, ds)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_FullyConsistent{
				FullyConsistent: true,
			},
		},
	}, ds, TreatMismatchingTokensAsError)
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
	updated = datastoremw.ContextWithDatastore(updated, ds)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(exact),
			},
		},
	}, ds, TreatMismatchingTokensAsError)
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
	updated = datastoremw.ContextWithDatastore(updated, ds)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.MustNewFromRevisionForTesting(exact),
			},
		},
	}, ds, TreatMismatchingTokensAsError)
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
	updated = datastoremw.ContextWithDatastore(updated, ds)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.MustNewFromRevisionForTesting(zero),
			},
		},
	}, ds, TreatMismatchingTokensAsError)
	require.Error(err)
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextNoConsistencyAPI(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}

	updated := ContextWithHandle(context.Background())
	updated = datastoremw.ContextWithDatastore(updated, ds)

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
	updated = datastoremw.ContextWithDatastore(updated, ds)

	err = AddRevisionToContext(updated, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.MustNewFromRevisionForTesting(exact),
			},
		},
		OptionalCursor: cursor,
	}, ds, TreatMismatchingTokensAsError)
	require.NoError(err)

	// ensure we get back `optimized` from the cursor
	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(optimized.Equal(rev))
	ds.AssertExpectations(t)
}

func TestAtExactSnapshotWithMismatchedToken(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("RevisionFromString", optimized.String()).Return(optimized, nil).Once()

	// revision in context is at `exact`
	updated := ContextWithHandle(context.Background())
	updated = datastoremw.ContextWithDatastore(updated, ds)

	// mint a token with a different datastore instance ID.
	ds.CurrentUniqueID = "foo"
	zedToken, err := zedtoken.NewFromRevision(context.Background(), optimized, ds)
	require.NoError(err)

	ds.CurrentUniqueID = "bar"
	err = AddRevisionToContext(updated, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedToken,
			},
		},
	}, ds, TreatMismatchingTokensAsError)
	require.Error(err)
	require.ErrorContains(err, "ZedToken specified references a different datastore instance but at-exact-snapshot")
}

func TestAtLeastAsFreshWithMismatchedTokenExpectError(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(optimized, nil).Once()
	ds.On("RevisionFromString", optimized.String()).Return(optimized, nil).Once()

	// revision in context is at `exact`
	updated := ContextWithHandle(context.Background())
	updated = datastoremw.ContextWithDatastore(updated, ds)

	// mint a token with a different datastore instance ID.
	ds.CurrentUniqueID = "foo"
	zedToken, err := zedtoken.NewFromRevision(context.Background(), optimized, ds)
	require.NoError(err)

	ds.CurrentUniqueID = "bar"
	err = AddRevisionToContext(updated, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedToken,
			},
		},
	}, ds, TreatMismatchingTokensAsError)
	require.Error(err)
	require.ErrorContains(err, "ZedToken specified references a different datastore instance and SpiceDB is configured to raise an error in this scenario")
}

func TestAtLeastAsFreshWithMismatchedTokenExpectMinLatency(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(optimized, nil).Once()
	ds.On("RevisionFromString", optimized.String()).Return(optimized, nil).Once()

	// revision in context is at `exact`
	updated := ContextWithHandle(context.Background())
	updated = datastoremw.ContextWithDatastore(updated, ds)

	// mint a token with a different datastore instance ID.
	ds.CurrentUniqueID = "foo"
	zedToken, err := zedtoken.NewFromRevision(context.Background(), optimized, ds)
	require.NoError(err)

	ds.CurrentUniqueID = "bar"
	err = AddRevisionToContext(updated, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedToken,
			},
		},
	}, ds, TreatMismatchingTokensAsMinLatency)
	require.NoError(err)

	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(optimized.Equal(rev))
	ds.AssertExpectations(t)
}

func TestAtLeastAsFreshWithMismatchedTokenExpectFullConsistency(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("HeadRevision").Return(head, nil).Once()
	ds.On("OptimizedRevision").Return(optimized, nil).Once()
	ds.On("RevisionFromString", optimized.String()).Return(optimized, nil).Once()

	// revision in context is at `exact`
	updated := ContextWithHandle(context.Background())
	updated = datastoremw.ContextWithDatastore(updated, ds)

	// mint a token with a different datastore instance ID.
	ds.CurrentUniqueID = "foo"
	zedToken, err := zedtoken.NewFromRevision(context.Background(), optimized, ds)
	require.NoError(err)

	ds.CurrentUniqueID = "bar"
	err = AddRevisionToContext(updated, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedToken,
			},
		},
	}, ds, TreatMismatchingTokensAsFullConsistency)
	require.NoError(err)

	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(head.Equal(rev))
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextAtLeastAsFreshMatchingIDs(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(optimized, nil).Once()
	ds.On("RevisionFromString", exact.String()).Return(exact, nil).Once()

	ds.CurrentUniqueID = "foo"

	updated := ContextWithHandle(context.Background())
	updated = datastoremw.ContextWithDatastore(updated, ds)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(exact),
			},
		},
	}, ds, TreatMismatchingTokensAsError)
	require.NoError(err)

	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(exact.Equal(rev))
	ds.AssertExpectations(t)
}
