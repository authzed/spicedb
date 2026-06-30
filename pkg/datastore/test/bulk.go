package test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func BulkUploadTest(t *testing.T, tester DatastoreTester) {
	testCases := []int{0, 1, 10, 100, 1_000, 10_000}

	for _, tc := range testCases {
		t.Run(strconv.Itoa(tc), func(t *testing.T) {
			require := require.New(t)
			ctx := t.Context()

			rawDS, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
			require.NoError(err)

			ds, _ := testfixtures.StandardDatastoreWithSchema(t, rawDS)
			bulkSource := testfixtures.NewBulkRelationshipGenerator(
				testfixtures.DocumentNS.Name,
				"viewer",
				testfixtures.UserNS.Name,
				tc,
				t,
			)

			uintTc := safecast.RequireConvert[uint64](t, tc)
			_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				loaded, err := rwt.BulkLoad(ctx, bulkSource)
				require.NoError(err)
				require.Equal(uintTc, loaded)
				return err
			})
			require.NoError(err)

			tRequire := testfixtures.RelationshipChecker{Require: require, DS: ds}

			headResult, err := ds.HeadRevision(ctx)
			require.NoError(err)

			iter, err := ds.SnapshotReader(headResult.Revision).QueryRelationships(ctx, datastore.RelationshipsFilter{
				OptionalResourceType: testfixtures.DocumentNS.Name,
			}, options.WithQueryShape(queryshape.FindResourceOfType))
			require.NoError(err)

			tRequire.VerifyIteratorCount(iter, tc)
		})
	}
}

func BulkUploadErrorsTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	ctx := t.Context()

	rawDS, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(t, rawDS)

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		inserted, err := rwt.BulkLoad(ctx, &onlyErrorSource{})

		// We can't check the specific error because pgx is not wrapping
		require.Error(err)
		require.Zero(inserted)
		return err
	})
	require.Error(err)
}

func BulkUploadWithCaveats(t *testing.T, tester DatastoreTester) {
	tc := 10
	require := require.New(t)
	ctx := t.Context()

	rawDS, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(t, rawDS)
	bulkSource := testfixtures.NewBulkRelationshipGenerator(
		testfixtures.DocumentNS.Name,
		"caveated_viewer",
		testfixtures.UserNS.Name,
		tc,
		t,
	)
	bulkSource.WithCaveat = true

	uintTc := safecast.RequireConvert[uint64](t, tc)
	lastRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		loaded, err := rwt.BulkLoad(ctx, bulkSource)
		require.NoError(err)
		require.Equal(uintTc, loaded)
		return err
	})
	require.NoError(err)

	iter, err := ds.SnapshotReader(lastRevision).QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: testfixtures.DocumentNS.Name,
	}, options.WithQueryShape(queryshape.FindResourceOfType))
	require.NoError(err)

	for found, err := range iter {
		require.NoError(err)
		require.Nil(found.OptionalExpiration)
		require.NotNil(found.OptionalCaveat)
		require.NotEmpty(found.OptionalCaveat.CaveatName)
		require.NotNil(found.OptionalCaveat.Context)
		require.Equal(map[string]any{"secret": "1235"}, found.OptionalCaveat.Context.AsMap())
	}
}

func BulkUploadWithExpiration(t *testing.T, tester DatastoreTester) {
	tc := 10
	require := require.New(t)
	ctx := t.Context()

	rawDS, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(t, rawDS)
	bulkSource := testfixtures.NewBulkRelationshipGenerator(
		testfixtures.DocumentNS.Name,
		"expired_viewer",
		testfixtures.UserNS.Name,
		tc,
		t,
	)
	bulkSource.WithExpiration = true

	uintTc := safecast.RequireConvert[uint64](t, tc)
	lastRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		loaded, err := rwt.BulkLoad(ctx, bulkSource)
		require.NoError(err)
		require.Equal(uintTc, loaded)
		return err
	})
	require.NoError(err)

	iter, err := ds.SnapshotReader(lastRevision).QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: testfixtures.DocumentNS.Name,
	}, options.WithQueryShape(queryshape.FindResourceOfType))
	require.NoError(err)

	for found, err := range iter {
		require.NoError(err)
		require.NotNil(found.OptionalExpiration)
	}
}

func BulkUploadEditCaveat(t *testing.T, tester DatastoreTester) {
	tc := 10
	require := require.New(t)
	ctx := t.Context()

	rawDS, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(t, rawDS)
	bulkSource := testfixtures.NewBulkRelationshipGenerator(
		testfixtures.DocumentNS.Name,
		"caveated_viewer",
		testfixtures.UserNS.Name,
		tc,
		t,
	)

	uintTc := safecast.RequireConvert[uint64](t, tc)
	lastRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		loaded, err := rwt.BulkLoad(ctx, bulkSource)
		require.NoError(err)
		require.Equal(uintTc, loaded)
		return err
	})
	require.NoError(err)

	iter, err := ds.SnapshotReader(lastRevision).QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: testfixtures.DocumentNS.Name,
	}, options.WithQueryShape(queryshape.FindResourceOfType))
	require.NoError(err)

	updates := make([]tuple.RelationshipUpdate, 0, tc)

	for found, err := range iter {
		require.NoError(err)

		updates = append(updates, tuple.Touch(found.WithCaveat(&core.ContextualizedCaveat{
			CaveatName: testfixtures.CaveatDef.Name,
			Context:    nil,
		})))
	}

	require.Len(updates, tc)

	lastRevision, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.WriteRelationships(ctx, updates)
		require.NoError(err)
		return err
	})
	require.NoError(err)

	iter, err = ds.SnapshotReader(lastRevision).QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: testfixtures.DocumentNS.Name,
	}, options.WithQueryShape(queryshape.FindResourceOfType))
	require.NoError(err)

	foundChanged := 0
	for found, err := range iter {
		require.NoError(err)
		require.NotNil(found.OptionalCaveat)
		require.NotEmpty(found.OptionalCaveat.CaveatName)
		foundChanged++
	}

	require.Equal(tc, foundChanged)
}

// BulkUploadIdempotentTest verifies that BulkLoad has TOUCH-like (idempotent)
// semantics: re-loading relationships that already exist is a no-op rather than
// an error, and no duplicate relationships are created.
//
// NOTE: the count returned by BulkLoad reflects only newly-inserted rows for
// most datastores, but Spanner cannot cheaply distinguish new from existing
// relationships and reports the number processed instead. This test therefore
// asserts the no-duplicate invariant via queries (which holds for every
// datastore) and only checks the returned count for the initial load.
func BulkUploadIdempotentTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	ctx := t.Context()

	rawDS, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(t, rawDS)

	// bulkLoad loads `count` deterministic relationships (object IDs count-1..0)
	// in their own transaction and returns the count reported by BulkLoad.
	bulkLoad := func(count int) uint64 {
		var loaded uint64
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			var ierr error
			loaded, ierr = rwt.BulkLoad(ctx, testfixtures.NewBulkRelationshipGenerator(
				testfixtures.DocumentNS.Name,
				"viewer",
				testfixtures.UserNS.Name,
				count,
				t,
			))
			return ierr
		}, options.WithDisableRetries(true))
		require.NoError(err)
		return loaded
	}

	countRelationships := func() int {
		headResult, err := ds.HeadRevision(ctx)
		require.NoError(err)

		iter, err := ds.SnapshotReader(headResult.Revision).QueryRelationships(ctx, datastore.RelationshipsFilter{
			OptionalResourceType: testfixtures.DocumentNS.Name,
		}, options.WithQueryShape(queryshape.FindResourceOfType))
		require.NoError(err)

		found := 0
		for _, qerr := range iter {
			require.NoError(qerr)
			found++
		}
		return found
	}

	// Initial load inserts all relationships.
	require.Equal(uint64(10), bulkLoad(10))
	require.Equal(10, countRelationships())

	// Re-loading the exact same relationships is a no-op: it does not error and
	// creates no duplicates (TOUCH semantics, rather than an AlreadyExists
	// error).
	bulkLoad(10)
	require.Equal(10, countRelationships())

	// Loading a superset silently skips the relationships that already exist and
	// inserts only the previously-unseen ones.
	bulkLoad(20)
	require.Equal(20, countRelationships())

	// Re-loading duplicates twice within a single transaction is also a no-op.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		if _, ierr := rwt.BulkLoad(ctx, testfixtures.NewBulkRelationshipGenerator(
			testfixtures.DocumentNS.Name, "viewer", testfixtures.UserNS.Name, 5, t,
		)); ierr != nil {
			return ierr
		}
		_, ierr := rwt.BulkLoad(ctx, testfixtures.NewBulkRelationshipGenerator(
			testfixtures.DocumentNS.Name, "viewer", testfixtures.UserNS.Name, 5, t,
		))
		return ierr
	}, options.WithDisableRetries(true))
	require.NoError(err)
	require.Equal(20, countRelationships())
}

type onlyErrorSource struct{}

var errOnlyError = errors.New("source iterator error")

func (oes onlyErrorSource) Next(_ context.Context) (*tuple.Relationship, error) {
	return nil, errOnlyError
}

var _ datastore.BulkWriteRelationshipSource = onlyErrorSource{}
