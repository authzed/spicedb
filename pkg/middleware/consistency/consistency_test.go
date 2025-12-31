package consistency

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/cursor"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/mocks"
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	require := require.New(t)

	ds := mocks.NewMockDatastore(ctrl)
	ds.EXPECT().OptimizedRevision(gomock.Any()).Return(optimized, nil).Times(1)
	ds.EXPECT().UniqueID(gomock.Any()).Return("test-id", nil).AnyTimes()

	updated := ContextWithHandle(t.Context())
	updated = datastoremw.ContextWithDatastore(updated, ds)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{}, ds, "somelabel", TreatMismatchingTokensAsError)
	require.NoError(err)

	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(optimized.Equal(rev))
}

func TestAddRevisionToContextMinimizeLatency(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	require := require.New(t)

	ds := mocks.NewMockDatastore(ctrl)
	ds.EXPECT().OptimizedRevision(gomock.Any()).Return(optimized, nil).Times(1)
	ds.EXPECT().UniqueID(gomock.Any()).Return("test-id", nil).AnyTimes()

	updated := ContextWithHandle(t.Context())
	updated = datastoremw.ContextWithDatastore(updated, ds)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_MinimizeLatency{
				MinimizeLatency: true,
			},
		},
	}, ds, "somelabel", TreatMismatchingTokensAsError)
	require.NoError(err)

	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(optimized.Equal(rev))
}

func TestAddRevisionToContextFullyConsistent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	require := require.New(t)

	ds := mocks.NewMockDatastore(ctrl)
	ds.EXPECT().HeadRevision(gomock.Any()).Return(head, nil).Times(1)
	ds.EXPECT().UniqueID(gomock.Any()).Return("test-id", nil).AnyTimes()

	updated := ContextWithHandle(t.Context())
	updated = datastoremw.ContextWithDatastore(updated, ds)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_FullyConsistent{
				FullyConsistent: true,
			},
		},
	}, ds, "somelabel", TreatMismatchingTokensAsError)
	require.NoError(err)

	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(head.Equal(rev))
}

func TestAddRevisionToContextAtLeastAsFresh(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	require := require.New(t)

	ds := mocks.NewMockDatastore(ctrl)
	ds.EXPECT().OptimizedRevision(gomock.Any()).Return(optimized, nil).Times(1)
	ds.EXPECT().RevisionFromString(exact.String()).Return(exact, nil).Times(1)
	ds.EXPECT().UniqueID(gomock.Any()).Return("test-id", nil).AnyTimes()

	updated := ContextWithHandle(t.Context())
	updated = datastoremw.ContextWithDatastore(updated, ds)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(exact),
			},
		},
	}, ds, "somelabel", TreatMismatchingTokensAsError)
	require.NoError(err)

	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(exact.Equal(rev))
}

func TestAddRevisionToContextAtValidExactSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	require := require.New(t)

	ds := mocks.NewMockDatastore(ctrl)
	ds.EXPECT().CheckRevision(gomock.Any(), exact).Return(nil).Times(1)
	ds.EXPECT().RevisionFromString(exact.String()).Return(exact, nil).Times(1)
	ds.EXPECT().UniqueID(gomock.Any()).Return("test-id", nil).AnyTimes()

	updated := ContextWithHandle(t.Context())
	updated = datastoremw.ContextWithDatastore(updated, ds)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.MustNewFromRevisionForTesting(exact),
			},
		},
	}, ds, "somelabel", TreatMismatchingTokensAsError)
	require.NoError(err)

	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(exact.Equal(rev))
}

func TestAddRevisionToContextAtInvalidExactSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	require := require.New(t)

	ds := mocks.NewMockDatastore(ctrl)
	ds.EXPECT().CheckRevision(gomock.Any(), zero).Return(datastore.NewInvalidRevisionErr(zero, datastore.RevisionStale)).Times(1)
	ds.EXPECT().RevisionFromString(zero.String()).Return(zero, nil).Times(1)

	updated := ContextWithHandle(t.Context())
	updated = datastoremw.ContextWithDatastore(updated, ds)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.MustNewFromRevisionForTesting(zero),
			},
		},
	}, ds, "somelabel", TreatMismatchingTokensAsError)
	require.Error(err)
	grpcutil.RequireStatus(t, codes.OutOfRange, err)
}

func TestAddRevisionToContextNoConsistencyAPI(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	require := require.New(t)

	ds := mocks.NewMockDatastore(ctrl)

	updated := ContextWithHandle(t.Context())
	updated = datastoremw.ContextWithDatastore(updated, ds)

	_, _, err := RevisionFromContext(updated)
	require.Error(err)
}

func TestAddRevisionToContextWithCursor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	require := require.New(t)

	ds := mocks.NewMockDatastore(ctrl)
	ds.EXPECT().CheckRevision(gomock.Any(), optimized).Return(nil).Times(1)
	ds.EXPECT().RevisionFromString(optimized.String()).Return(optimized, nil).Times(1)
	ds.EXPECT().UniqueID(gomock.Any()).Return("test-id", nil).AnyTimes()

	// cursor is at `optimized`
	cursor, err := cursor.EncodeFromDispatchCursor(&dispatch.Cursor{}, "somehash", optimized, nil)
	require.NoError(err)

	// revision in context is at `exact`
	updated := ContextWithHandle(t.Context())
	updated = datastoremw.ContextWithDatastore(updated, ds)

	err = AddRevisionToContext(updated, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.MustNewFromRevisionForTesting(exact),
			},
		},
		OptionalCursor: cursor,
	}, ds, "somelabel", TreatMismatchingTokensAsError)
	require.NoError(err)

	// ensure we get back `optimized` from the cursor
	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(optimized.Equal(rev))
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ds := mocks.NewMockDatastore(ctrl)
	ds.EXPECT().OptimizedRevision(gomock.Any()).Return(optimized, nil).Times(1)

	err := AddRevisionToContext(ContextWithHandle(t.Context()), &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: &v1.ZedToken{Token: "blah"},
			},
		},
	}, ds, "", TreatMismatchingTokensAsError)
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	require := require.New(t)

	ds := mocks.NewMockDatastore(ctrl)
	ds.EXPECT().RevisionFromString(optimized.String()).Return(optimized, nil).Times(1)

	// revision in context is at `exact`
	updated := ContextWithHandle(context.Background())
	updated = datastoremw.ContextWithDatastore(updated, ds)

	// mint a token with a different datastore instance ID.
	ds.EXPECT().UniqueID(gomock.Any()).Return("foo", nil).Times(1)
	zedToken, err := zedtoken.NewFromRevision(context.Background(), optimized, ds)
	require.NoError(err)

	ds.EXPECT().UniqueID(gomock.Any()).Return("bar", nil).Times(1)
	err = AddRevisionToContext(updated, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedToken,
			},
		},
	}, ds, "somelabel", TreatMismatchingTokensAsError)
	require.Error(err)
	require.ErrorContains(err, "ZedToken specified references a different datastore instance but at-exact-snapshot")
}

func TestAtLeastAsFreshWithMismatchedTokenExpectError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	require := require.New(t)

	ds := mocks.NewMockDatastore(ctrl)
	ds.EXPECT().OptimizedRevision(gomock.Any()).Return(optimized, nil).Times(1)
	ds.EXPECT().RevisionFromString(optimized.String()).Return(optimized, nil).Times(1)

	// revision in context is at `exact`
	updated := ContextWithHandle(context.Background())
	updated = datastoremw.ContextWithDatastore(updated, ds)

	// mint a token with a different datastore instance ID.
	ds.EXPECT().UniqueID(gomock.Any()).Return("foo", nil).Times(1)
	zedToken, err := zedtoken.NewFromRevision(context.Background(), optimized, ds)
	require.NoError(err)

	ds.EXPECT().UniqueID(gomock.Any()).Return("bar", nil).Times(1)
	err = AddRevisionToContext(updated, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedToken,
			},
		},
	}, ds, "somelabel", TreatMismatchingTokensAsError)
	require.Error(err)
	require.ErrorContains(err, "ZedToken specified references a different datastore instance and SpiceDB is configured to raise an error in this scenario")
}

func TestAtLeastAsFreshWithMismatchedTokenExpectMinLatency(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	require := require.New(t)

	ds := mocks.NewMockDatastore(ctrl)
	ds.EXPECT().OptimizedRevision(gomock.Any()).Return(optimized, nil).Times(1)
	ds.EXPECT().RevisionFromString(optimized.String()).Return(optimized, nil).Times(1)

	// revision in context is at `exact`
	updated := ContextWithHandle(context.Background())
	updated = datastoremw.ContextWithDatastore(updated, ds)

	// mint a token with a different datastore instance ID.
	firstCall := ds.EXPECT().UniqueID(gomock.Any()).Return("foo", nil).Times(1)
	zedToken, err := zedtoken.NewFromRevision(context.Background(), optimized, ds)
	require.NoError(err)

	ds.EXPECT().UniqueID(gomock.Any()).Return("bar", nil).After(firstCall).AnyTimes()
	err = AddRevisionToContext(updated, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedToken,
			},
		},
	}, ds, "somelabel", TreatMismatchingTokensAsMinLatency)
	require.NoError(err)

	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(optimized.Equal(rev))
}

func TestAtLeastAsFreshWithMismatchedTokenExpectFullConsistency(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	require := require.New(t)

	ds := mocks.NewMockDatastore(ctrl)
	ds.EXPECT().HeadRevision(gomock.Any()).Return(head, nil).Times(1)
	ds.EXPECT().OptimizedRevision(gomock.Any()).Return(optimized, nil).Times(1)
	ds.EXPECT().RevisionFromString(optimized.String()).Return(optimized, nil).Times(1)

	// revision in context is at `exact`
	updated := ContextWithHandle(context.Background())
	updated = datastoremw.ContextWithDatastore(updated, ds)

	// mint a token with a different datastore instance ID.
	firstCall := ds.EXPECT().UniqueID(gomock.Any()).Return("foo", nil).Times(1)
	zedToken, err := zedtoken.NewFromRevision(context.Background(), optimized, ds)
	require.NoError(err)

	ds.EXPECT().UniqueID(gomock.Any()).Return("bar", nil).After(firstCall).AnyTimes()
	err = AddRevisionToContext(updated, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedToken,
			},
		},
	}, ds, "somelabel", TreatMismatchingTokensAsFullConsistency)
	require.NoError(err)

	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(head.Equal(rev))
}

func TestAddRevisionToContextAtLeastAsFreshMatchingIDs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	require := require.New(t)

	ds := mocks.NewMockDatastore(ctrl)
	ds.EXPECT().OptimizedRevision(gomock.Any()).Return(optimized, nil).Times(1)
	ds.EXPECT().RevisionFromString(exact.String()).Return(exact, nil).Times(1)

	ds.EXPECT().UniqueID(gomock.Any()).Return("foo", nil).AnyTimes()

	updated := ContextWithHandle(context.Background())
	updated = datastoremw.ContextWithDatastore(updated, ds)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(exact),
			},
		},
	}, ds, "somelabel", TreatMismatchingTokensAsError)
	require.NoError(err)

	rev, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(exact.Equal(rev))
}
