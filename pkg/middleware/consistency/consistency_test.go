package consistency

import (
	"context"
	"errors"
	"slices"
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
	impl "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

var (
	zero      = revisions.NewForTransactionID(0)
	optimized = revisions.NewForTransactionID(100)
	exact     = revisions.NewForTransactionID(123)
	head      = revisions.NewForTransactionID(145)

	optimizedWithHash = datastore.RevisionWithSchemaHash{Revision: optimized, SchemaHash: "testhash"}
	headWithHash      = datastore.RevisionWithSchemaHash{Revision: head, SchemaHash: "testhash"}
)

func TestAddRevisionToContextNoneSupplied(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(datastore.RevisionWithSchemaHashAndValidity{Revision: optimizedWithHash.Revision, SchemaHash: optimizedWithHash.SchemaHash}, nil).Once()
	dl := datalayer.NewDataLayer(ds)

	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{}, dl, "somelabel", TreatMismatchingTokensAsError)
	require.NoError(err)

	rev, _, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(optimized.Equal(rev))
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextMinimizeLatency(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(datastore.RevisionWithSchemaHashAndValidity{Revision: optimizedWithHash.Revision, SchemaHash: optimizedWithHash.SchemaHash}, nil).Once()
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

	rev, _, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(optimized.Equal(rev))
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextFullyConsistent(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("HeadRevision").Return(headWithHash, nil).Once()
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

	rev, _, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(head.Equal(rev))
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextAtLeastAsFresh(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(datastore.RevisionWithSchemaHashAndValidity{Revision: optimizedWithHash.Revision, SchemaHash: optimizedWithHash.SchemaHash}, nil).Once()
	ds.On("RevisionFromString", exact.String()).Return(exact, nil).Once()
	dl := datalayer.NewDataLayer(ds)

	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(exact, datalayer.NoSchemaHashInLegacyZedToken),
			},
		},
	}, dl, "somelabel", TreatMismatchingTokensAsError)
	require.NoError(err)

	rev, _, _, err := RevisionFromContext(updated)
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
				AtExactSnapshot: zedtoken.MustNewFromRevisionForTesting(exact, datalayer.NoSchemaHashInLegacyZedToken),
			},
		},
	}, dl, "somelabel", TreatMismatchingTokensAsError)
	require.NoError(err)

	rev, _, _, err := RevisionFromContext(updated)
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
				AtExactSnapshot: zedtoken.MustNewFromRevisionForTesting(zero, datalayer.NoSchemaHashInLegacyZedToken),
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

	_, _, _, err := RevisionFromContext(updated)
	require.Error(err)
}

func TestAddRevisionToContextWithCursor(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("CheckRevision", optimized).Return(nil).Times(1)
	ds.On("RevisionFromString", optimized.String()).Return(optimized, nil).Once()
	dl := datalayer.NewDataLayer(ds)

	// cursor is at `optimized`
	cursor, err := cursor.EncodeFromDispatchCursor(&dispatch.Cursor{}, "somehash", optimized, datalayer.NoSchemaHashForLegacyCursor, nil)
	require.NoError(err)

	// revision in context is at `exact`
	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	err = AddRevisionToContext(updated, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.MustNewFromRevisionForTesting(exact, datalayer.NoSchemaHashInLegacyZedToken),
			},
		},
		OptionalCursor: cursor,
	}, dl, "somelabel", TreatMismatchingTokensAsError)
	require.NoError(err)

	// ensure we get back `optimized` from the cursor
	rev, _, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(optimized.Equal(rev))
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextWithCursorAndSchemaHash(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("CheckRevision", optimized).Return(nil).Times(1)
	ds.On("RevisionFromString", optimized.String()).Return(optimized, nil).Once()
	dl := datalayer.NewDataLayer(ds)

	// Encode a cursor with DatastoreUniqueId set so the schema hash roundtrips.
	// The mock datastore returns "mockds" as its unique ID.
	encodedCursor, err := cursor.Encode(&impl.DecodedCursor{
		VersionOneof: &impl.DecodedCursor_V1{
			V1: &impl.V1Cursor{
				Revision:              optimized.String(),
				DispatchVersion:       1,
				CallAndParametersHash: "somehash",
				DatastoreUniqueId:     "mockds",
				SchemaHash:            []byte("myspecialschema"),
			},
		},
	})
	require.NoError(err)

	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	err = AddRevisionToContext(updated, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.MustNewFromRevisionForTesting(exact, datalayer.NoSchemaHashInLegacyZedToken),
			},
		},
		OptionalCursor: encodedCursor,
	}, dl, "somelabel", TreatMismatchingTokensAsError)
	require.NoError(err)

	rev, schemaHash, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(optimized.Equal(rev))
	require.Equal(datalayer.SchemaHash("myspecialschema"), schemaHash)
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
	ds.On("OptimizedRevision").Return(datastore.RevisionWithSchemaHashAndValidity{Revision: optimizedWithHash.Revision, SchemaHash: optimizedWithHash.SchemaHash}, nil).Once()
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
	_, _, _, err := RevisionFromContext(updated)
	require.Error(t, err)
	grpcutil.RequireStatus(t, codes.Internal, err)
	require.ErrorContains(t, err, "consistency middleware did not inject revision")
}

func TestRewriteDatastoreError(t *testing.T) {
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
	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	// mint a token with a different datastore instance ID.
	ds.CurrentUniqueID = "foo"
	zedToken, err := zedtoken.NewFromRevision(t.Context(), optimized, datalayer.NoSchemaHashInLegacyZedToken, dl)
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
	ds.On("OptimizedRevision").Return(datastore.RevisionWithSchemaHashAndValidity{Revision: optimizedWithHash.Revision, SchemaHash: optimizedWithHash.SchemaHash}, nil).Once()
	ds.On("RevisionFromString", optimized.String()).Return(optimized, nil).Once()
	dl := datalayer.NewDataLayer(ds)

	// revision in context is at `exact`
	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	// mint a token with a different datastore instance ID.
	ds.CurrentUniqueID = "foo"
	zedToken, err := zedtoken.NewFromRevision(t.Context(), optimized, datalayer.NoSchemaHashInLegacyZedToken, dl)
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
	ds.On("OptimizedRevision").Return(datastore.RevisionWithSchemaHashAndValidity{Revision: optimizedWithHash.Revision, SchemaHash: optimizedWithHash.SchemaHash}, nil).Once()
	ds.On("RevisionFromString", optimized.String()).Return(optimized, nil).Once()
	dl := datalayer.NewDataLayer(ds)

	// revision in context is at `exact`
	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	// mint a token with a different datastore instance ID.
	ds.CurrentUniqueID = "foo"
	zedToken, err := zedtoken.NewFromRevision(t.Context(), optimized, datalayer.NoSchemaHashInLegacyZedToken, dl)
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

	rev, _, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(optimized.Equal(rev))
	ds.AssertExpectations(t)
}

func TestAtLeastAsFreshWithMismatchedTokenExpectFullConsistency(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("HeadRevision").Return(headWithHash, nil).Once()
	ds.On("OptimizedRevision").Return(datastore.RevisionWithSchemaHashAndValidity{Revision: optimizedWithHash.Revision, SchemaHash: optimizedWithHash.SchemaHash}, nil).Once()
	ds.On("RevisionFromString", optimized.String()).Return(optimized, nil).Once()
	dl := datalayer.NewDataLayer(ds)

	// revision in context is at `exact`
	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	// mint a token with a different datastore instance ID.
	ds.CurrentUniqueID = "foo"
	zedToken, err := zedtoken.NewFromRevision(t.Context(), optimized, datalayer.NoSchemaHashInLegacyZedToken, dl)
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

	rev, _, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(head.Equal(rev))
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextAtExactSnapshotPropagatesSchemaHash(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("CheckRevision", exact).Return(nil).Times(1)
	ds.On("RevisionFromString", exact.String()).Return(exact, nil).Once()
	dl := datalayer.NewDataLayer(ds)

	// Build a zedtoken WITH a non-sentinel hash.
	expectedHash := datalayer.SchemaHash("at-exact-snapshot-test-hash")
	encoded, err := zedtoken.NewFromRevision(t.Context(), exact, expectedHash, dl)
	require.NoError(err)

	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	err = AddRevisionToContext(updated, &v1.CheckPermissionRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: encoded,
			},
		},
	}, dl, "test", TreatMismatchingTokensAsFullConsistency)
	require.NoError(err)

	_, gotHash, _, err := RevisionFromContext(updated)
	require.NoError(err)
	require.Equal(expectedHash, gotHash)
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextAtLeastAsFreshUsesTokenSchemaHashWhenTokenWins(t *testing.T) {
	require := require.New(t)

	// Make OptimizedRevision return `optimized` (100) so the token at `exact` (123) wins.
	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(datastore.RevisionWithSchemaHashAndValidity{Revision: optimizedWithHash.Revision, SchemaHash: optimizedWithHash.SchemaHash}, nil).Once()
	ds.On("RevisionFromString", exact.String()).Return(exact, nil).Once()
	dl := datalayer.NewDataLayer(ds)

	// Build a zedtoken at `exact` with a non-sentinel schema hash.
	expectedHash := datalayer.SchemaHash("at-least-as-fresh-test-hash")
	encoded, err := zedtoken.NewFromRevision(t.Context(), exact, expectedHash, dl)
	require.NoError(err)

	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	err = AddRevisionToContext(updated, &v1.CheckPermissionRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{AtLeastAsFresh: encoded},
		},
	}, dl, "test", TreatMismatchingTokensAsFullConsistency)
	require.NoError(err)

	_, gotHash, _, err := RevisionFromContext(updated)
	require.NoError(err)
	require.Equal(expectedHash, gotHash)
	ds.AssertExpectations(t)
}

func TestRevisionFromContextEmitsTokenWithSchemaHash(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("RevisionFromString", exact.String()).Return(exact, nil)
	dl := datalayer.NewDataLayer(ds)

	ctx := datalayer.ContextWithDataLayer(t.Context(), dl)
	ctx = ContextWithHandle(ctx)

	expectedHash := datalayer.SchemaHash("emit-test-hash")
	handle := ctx.Value(revisionKey).(*revisionHandle)
	handle.revision = exact
	handle.schemaHash = expectedHash

	_, gotHash, token, err := RevisionFromContext(ctx)
	require.NoError(err)
	require.Equal(expectedHash, gotHash)

	// Round-trip the issued token and confirm the hash made it onto the wire.
	decoded, err := zedtoken.DecodeRevision(token, dl)
	require.NoError(err)
	require.Equal(expectedHash, decoded.SchemaHash)
}

func TestAddRevisionToContextAtLeastAsFreshMatchingIDs(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(datastore.RevisionWithSchemaHashAndValidity{Revision: optimizedWithHash.Revision, SchemaHash: optimizedWithHash.SchemaHash}, nil).Once()
	ds.On("RevisionFromString", exact.String()).Return(exact, nil).Once()

	ds.CurrentUniqueID = "foo"
	dl := datalayer.NewDataLayer(ds)

	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(exact, datalayer.NoSchemaHashInLegacyZedToken),
			},
		},
	}, dl, "somelabel", TreatMismatchingTokensAsError)
	require.NoError(err)

	rev, _, _, err := RevisionFromContext(updated)
	require.NoError(err)

	require.True(exact.Equal(rev))
	ds.AssertExpectations(t)
}

func addAtLeastAsFresh(t *testing.T, dl datalayer.DataLayer, token *v1.ZedToken) (datastore.Revision, datalayer.SchemaHash) {
	t.Helper()
	updated := ContextWithHandle(t.Context())
	updated = datalayer.ContextWithDataLayer(updated, dl)

	err := AddRevisionToContext(updated, &v1.CheckPermissionRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{AtLeastAsFresh: token},
		},
	}, dl, "somelabel", TreatMismatchingTokensAsError)
	require.NoError(t, err)

	rev, hash, _, err := RevisionFromContext(updated)
	require.NoError(t, err)
	return rev, hash
}

func TestAtLeastAsFreshHashlessTokenUsesObservedRevision(t *testing.T) {
	ds := &proxy_test.MockDatastore{}
	ds.On("HeadRevision").Return(headWithHash, nil).Once()
	ds.On("OptimizedRevision").Return(datastore.RevisionWithSchemaHashAndValidity{Revision: optimizedWithHash.Revision, SchemaHash: optimizedWithHash.SchemaHash}, nil).Once()
	ds.On("RevisionFromString", exact.String()).Return(exact, nil).Once()
	dl := datalayer.NewDataLayer(ds, datalayer.WithSchemaMode(datalayer.SchemaModeReadNewWriteNew))

	// Observe head (145) and its hash.
	_, _, err := dl.HeadRevision(t.Context())
	require.NoError(t, err)

	// The token (123) is newer than the optimized revision (100) and has no schema hash. The
	// observed pair (145) is at least as fresh, so the middleware uses it and does not call
	// HeadRevision again.
	rev, hash := addAtLeastAsFresh(t, dl, zedtoken.MustNewFromRevisionForTesting(exact, datalayer.NoSchemaHashInLegacyZedToken))
	require.True(t, head.Equal(rev))
	require.Equal(t, datalayer.SchemaHash("testhash"), hash)
	ds.AssertExpectations(t)
}

func TestAtLeastAsFreshHashlessTokenNewerThanObservedUsesHead(t *testing.T) {
	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(datastore.RevisionWithSchemaHashAndValidity{Revision: optimizedWithHash.Revision, SchemaHash: optimizedWithHash.SchemaHash}, nil).Once()
	ds.On("RevisionFromString", exact.String()).Return(exact, nil).Once()
	ds.On("HeadRevision").Return(headWithHash, nil).Once()
	dl := datalayer.NewDataLayer(ds, datalayer.WithSchemaMode(datalayer.SchemaModeReadNewWriteNew))

	rev, hash := addAtLeastAsFresh(t, dl, zedtoken.MustNewFromRevisionForTesting(exact, datalayer.NoSchemaHashInLegacyZedToken))
	require.True(t, head.Equal(rev))
	require.Equal(t, datalayer.SchemaHash("testhash"), hash)
	ds.AssertExpectations(t)
}

func TestAtLeastAsFreshHashlessTokenNewerThanHeadUsesToken(t *testing.T) {
	behind := revisions.NewForTransactionID(110)

	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(datastore.RevisionWithSchemaHashAndValidity{Revision: optimizedWithHash.Revision, SchemaHash: optimizedWithHash.SchemaHash}, nil).Once()
	ds.On("RevisionFromString", exact.String()).Return(exact, nil).Once()
	ds.On("HeadRevision").Return(datastore.RevisionWithSchemaHash{Revision: behind, SchemaHash: "testhash"}, nil).Once()
	dl := datalayer.NewDataLayer(ds, datalayer.WithSchemaMode(datalayer.SchemaModeReadNewWriteNew))

	rev, hash := addAtLeastAsFresh(t, dl, zedtoken.MustNewFromRevisionForTesting(exact, datalayer.NoSchemaHashInLegacyZedToken))
	require.True(t, exact.Equal(rev))
	require.Equal(t, datalayer.NoSchemaHashInLegacyZedToken, hash)
	ds.AssertExpectations(t)
}

func TestAtLeastAsFreshHashlessTokenLegacyModeSkipsHeadRevision(t *testing.T) {
	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(datastore.RevisionWithSchemaHashAndValidity{Revision: optimizedWithHash.Revision, SchemaHash: optimizedWithHash.SchemaHash}, nil).Once()
	ds.On("RevisionFromString", exact.String()).Return(exact, nil).Once()
	dl := datalayer.NewDataLayer(ds, datalayer.WithSchemaMode(datalayer.SchemaModeReadLegacyWriteLegacy))

	rev, hash := addAtLeastAsFresh(t, dl, zedtoken.MustNewFromRevisionForTesting(exact, datalayer.NoSchemaHashInLegacyZedToken))
	require.True(t, exact.Equal(rev))
	require.Equal(t, datalayer.NoSchemaHashInLegacyZedToken, hash)
	ds.AssertExpectations(t)
}

// partialRevision is a partially ordered revision, like a Postgres snapshot. A revision is
// greater than only the revisions named in above.
type partialRevision struct {
	name  string
	above []string
}

func (r partialRevision) String() string     { return r.name }
func (r partialRevision) ByteSortable() bool { return false }
func (r partialRevision) Equal(rhs datastore.Revision) bool {
	o, ok := rhs.(partialRevision)
	return ok && o.name == r.name
}

func (r partialRevision) GreaterThan(rhs datastore.Revision) bool {
	if rhs == datastore.NoRevision {
		return true
	}
	o, ok := rhs.(partialRevision)
	return ok && slices.Contains(r.above, o.name)
}

func (r partialRevision) LessThan(rhs datastore.Revision) bool {
	o, ok := rhs.(partialRevision)
	return ok && o.GreaterThan(r)
}

func TestAtLeastAsFreshHashlessTokenConcurrentWithObservedUsesHead(t *testing.T) {
	opt := partialRevision{name: "opt"}
	observed := partialRevision{name: "observed", above: []string{"opt"}}
	token := partialRevision{name: "token", above: []string{"opt"}}
	headRev := partialRevision{name: "head", above: []string{"opt", "observed", "token"}}

	ds := &proxy_test.MockDatastore{}
	ds.On("HeadRevision").Return(datastore.RevisionWithSchemaHash{Revision: observed, SchemaHash: "observedhash"}, nil).Once()
	ds.On("OptimizedRevision").Return(datastore.RevisionWithSchemaHashAndValidity{Revision: opt, SchemaHash: "testhash"}, nil).Once()
	ds.On("RevisionFromString", token.String()).Return(token, nil).Once()
	ds.On("HeadRevision").Return(datastore.RevisionWithSchemaHash{Revision: headRev, SchemaHash: "headhash"}, nil).Once()
	dl := datalayer.NewDataLayer(ds, datalayer.WithSchemaMode(datalayer.SchemaModeReadNewWriteNew))

	_, _, err := dl.HeadRevision(t.Context())
	require.NoError(t, err)

	// The observed revision is concurrent with the token and may not include the token's write.
	// The middleware must use head instead.
	rev, hash := addAtLeastAsFresh(t, dl, zedtoken.MustNewFromRevisionForTesting(token, datalayer.NoSchemaHashInLegacyZedToken))
	require.True(t, headRev.Equal(rev))
	require.Equal(t, datalayer.SchemaHash("headhash"), hash)
	ds.AssertExpectations(t)
}
