package consistency

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/cursor"
	"github.com/authzed/spicedb/pkg/datastore"
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

	updated := ContextWithHandle(t.Context())
	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{}, ds, "somelabel")
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

	updated := ContextWithHandle(t.Context())
	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_MinimizeLatency{
				MinimizeLatency: true,
			},
		},
	}, ds, "somelabel")
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

	updated := ContextWithHandle(t.Context())
	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_FullyConsistent{
				FullyConsistent: true,
			},
		},
	}, ds, "somelabel")
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

	updated := ContextWithHandle(t.Context())
	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(exact),
			},
		},
	}, ds, "somelabel")
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

	updated := ContextWithHandle(t.Context())
	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.MustNewFromRevision(exact),
			},
		},
	}, ds, "somelabel")
	require.NoError(err)

	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(exact.Equal(rev))
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextAtInvalidExactSnapshot(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("CheckRevision", zero).Return(datastore.NewInvalidRevisionErr(zero, datastore.RevisionStale)).Times(1)
	ds.On("RevisionFromString", zero.String()).Return(zero, nil).Once()

	updated := ContextWithHandle(t.Context())
	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.MustNewFromRevision(zero),
			},
		},
	}, ds, "somelabel")
	require.Error(err)
	grpcutil.RequireStatus(t, codes.OutOfRange, err)
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextNoConsistencyAPI(t *testing.T) {
	require := require.New(t)

	updated := ContextWithHandle(t.Context())

	_, _, err := RevisionFromContext(updated)
	require.Error(err)
}

func TestAddRevisionToContextWithCursor(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("CheckRevision", optimized).Return(nil).Times(1)
	ds.On("RevisionFromString", optimized.String()).Return(optimized, nil).Once()

	// cursor is at `optimized`
	cursor, err := cursor.EncodeFromDispatchCursor(&dispatch.Cursor{}, "somehash", optimized, nil)
	require.NoError(err)

	// revision in context is at `exact`
	updated := ContextWithHandle(t.Context())
	err = AddRevisionToContext(updated, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.MustNewFromRevision(exact),
			},
		},
		OptionalCursor: cursor,
	}, ds, "somelabel")
	require.NoError(err)

	// ensure we get back `optimized` from the cursor
	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(optimized.Equal(rev))
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextAtMalformedExactSnapshot(t *testing.T) {
	err := AddRevisionToContext(ContextWithHandle(t.Context()), &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: &v1.ZedToken{Token: "blah"},
			},
		},
	}, nil, "")
	require.Error(t, err)
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
}

func TestAddRevisionToContextMalformedAtLeastAsFreshSnapshot(t *testing.T) {
	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(optimized, nil).Once()

	err := AddRevisionToContext(ContextWithHandle(t.Context()), &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: &v1.ZedToken{Token: "blah"},
			},
		},
	}, ds, "")
	require.Error(t, err)
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
}

func TestRevisionFromContextMissingConsistency(t *testing.T) {
	updated := ContextWithHandle(t.Context())
	_, _, err := RevisionFromContext(updated)
	require.Error(t, err)
	grpcutil.RequireStatus(t, codes.Internal, err)
	require.ErrorContains(t, err, "consistency middleware did not inject revision")
}

func TestRewriteDatastoreError(t *testing.T) {
	err := rewriteDatastoreError(errors.New("foobar"))
	require.Error(t, err)
	grpcutil.RequireStatus(t, codes.Internal, err)
	require.ErrorContains(t, err, "foobar")

	err = rewriteDatastoreError(datastore.NewInvalidRevisionErr(zero, datastore.RevisionStale))
	require.Error(t, err)
	grpcutil.RequireStatus(t, codes.OutOfRange, err)
	require.ErrorContains(t, err, "invalid revision")

	err = rewriteDatastoreError(datastore.NewReadonlyErr())
	require.Error(t, err)
	grpcutil.RequireStatus(t, codes.Unavailable, err)
	require.ErrorContains(t, err, "service read-only")
}
