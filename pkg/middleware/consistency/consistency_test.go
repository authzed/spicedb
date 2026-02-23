package consistency

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/cursor"
	"github.com/authzed/spicedb/pkg/datalayer"
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
	dl := datalayer.NewDataLayer(ds)

	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{}, dl, "somelabel", TreatMismatchingTokensAsError)
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
	dl := datalayer.NewDataLayer(ds)

	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_MinimizeLatency{
				MinimizeLatency: true,
			},
		},
	}, dl, "somelabel", TreatMismatchingTokensAsError)
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
	dl := datalayer.NewDataLayer(ds)

	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_FullyConsistent{
				FullyConsistent: true,
			},
		},
	}, dl, "somelabel", TreatMismatchingTokensAsError)
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
	dl := datalayer.NewDataLayer(ds)

	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(exact),
			},
		},
	}, dl, "somelabel", TreatMismatchingTokensAsError)
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
	dl := datalayer.NewDataLayer(ds)

	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.MustNewFromRevisionForTesting(exact),
			},
		},
	}, dl, "somelabel", TreatMismatchingTokensAsError)
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
	dl := datalayer.NewDataLayer(ds)

	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.MustNewFromRevisionForTesting(zero),
			},
		},
	}, dl, "somelabel", TreatMismatchingTokensAsError)
	require.Error(err)
	grpcutil.RequireStatus(t, codes.OutOfRange, err)
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextNoConsistencyAPI(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	dl := datalayer.NewDataLayer(ds)

	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	_, _, err := RevisionFromContext(updated)
	require.Error(err)
}

func TestAddRevisionToContextWithCursor(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("CheckRevision", optimized).Return(nil).Times(1)
	ds.On("RevisionFromString", optimized.String()).Return(optimized, nil).Once()
	dl := datalayer.NewDataLayer(ds)

	// cursor is at `optimized`
	cursor, err := cursor.EncodeFromDispatchCursor(&dispatch.Cursor{}, "somehash", optimized, nil)
	require.NoError(err)

	// revision in context is at `exact`
	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	err = AddRevisionToContext(updated, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.MustNewFromRevisionForTesting(exact),
			},
		},
		OptionalCursor: cursor,
	}, dl, "somelabel", TreatMismatchingTokensAsError)
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
	}, nil, "", TreatMismatchingTokensAsError)
	require.Error(t, err)
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
}

func TestAddRevisionToContextMalformedAtLeastAsFreshSnapshot(t *testing.T) {
	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(optimized, nil).Once()
	dl := datalayer.NewDataLayer(ds)

	err := AddRevisionToContext(ContextWithHandle(t.Context()), &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: &v1.ZedToken{Token: "blah"},
			},
		},
	}, dl, "", TreatMismatchingTokensAsError)
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
	t.Parallel()
	type tc struct {
		name        string
		err         error
		code        codes.Code
		errContains string
	}
	tcs := []tc{
		{
			name:        "internal err",
			err:         errors.New("foobar"),
			code:        codes.Internal,
			errContains: "foobar",
		},
		{
			name:        "invalid revision",
			err:         datastore.NewInvalidRevisionErr(zero, datastore.RevisionStale),
			code:        codes.OutOfRange,
			errContains: "invalid revision",
		},
		{
			name:        "readonly err",
			err:         datastore.NewReadonlyErr(),
			code:        codes.Unavailable,
			errContains: "service read-only",
		},
		{
			name:        "context canceled",
			err:         context.Canceled,
			code:        codes.Canceled,
			errContains: "context canceled",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := rewriteDatastoreError(tc.err)
			require.Error(t, err)
			grpcutil.RequireStatus(t, tc.code, err)
			require.ErrorContains(t, err, tc.errContains)
		})
	}
}

func TestAtExactSnapshotWithMismatchedToken(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("RevisionFromString", optimized.String()).Return(optimized, nil).Once()
	dl := datalayer.NewDataLayer(ds)

	// revision in context is at `exact`
	updated := ContextWithHandle(context.Background())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	// mint a token with a different datastore instance ID.
	ds.CurrentUniqueID = "foo"
	zedToken, err := zedtoken.NewFromRevision(context.Background(), optimized, dl)
	require.NoError(err)

	ds.CurrentUniqueID = "bar"
	err = AddRevisionToContext(updated, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedToken,
			},
		},
	}, dl, "somelabel", TreatMismatchingTokensAsError)
	require.Error(err)
	require.ErrorContains(err, "ZedToken specified references a different datastore instance but at-exact-snapshot")
}

func TestAtLeastAsFreshWithMismatchedTokenExpectError(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(optimized, nil).Once()
	ds.On("RevisionFromString", optimized.String()).Return(optimized, nil).Once()
	dl := datalayer.NewDataLayer(ds)

	// revision in context is at `exact`
	updated := ContextWithHandle(context.Background())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	// mint a token with a different datastore instance ID.
	ds.CurrentUniqueID = "foo"
	zedToken, err := zedtoken.NewFromRevision(context.Background(), optimized, dl)
	require.NoError(err)

	ds.CurrentUniqueID = "bar"
	err = AddRevisionToContext(updated, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedToken,
			},
		},
	}, dl, "somelabel", TreatMismatchingTokensAsError)
	require.Error(err)
	require.ErrorContains(err, "ZedToken specified references a different datastore instance and SpiceDB is configured to raise an error in this scenario")
}

func TestAtLeastAsFreshWithMismatchedTokenExpectMinLatency(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(optimized, nil).Once()
	ds.On("RevisionFromString", optimized.String()).Return(optimized, nil).Once()
	dl := datalayer.NewDataLayer(ds)

	// revision in context is at `exact`
	updated := ContextWithHandle(context.Background())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	// mint a token with a different datastore instance ID.
	ds.CurrentUniqueID = "foo"
	zedToken, err := zedtoken.NewFromRevision(context.Background(), optimized, dl)
	require.NoError(err)

	ds.CurrentUniqueID = "bar"
	err = AddRevisionToContext(updated, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedToken,
			},
		},
	}, dl, "somelabel", TreatMismatchingTokensAsMinLatency)
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
	dl := datalayer.NewDataLayer(ds)

	// revision in context is at `exact`
	updated := ContextWithHandle(context.Background())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	// mint a token with a different datastore instance ID.
	ds.CurrentUniqueID = "foo"
	zedToken, err := zedtoken.NewFromRevision(context.Background(), optimized, dl)
	require.NoError(err)

	ds.CurrentUniqueID = "bar"
	err = AddRevisionToContext(updated, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedToken,
			},
		},
	}, dl, "somelabel", TreatMismatchingTokensAsFullConsistency)
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
	dl := datalayer.NewDataLayer(ds)

	updated := ContextWithHandle(context.Background())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(exact),
			},
		},
	}, dl, "somelabel", TreatMismatchingTokensAsError)
	require.NoError(err)

	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(exact.Equal(rev))
	ds.AssertExpectations(t)
}
