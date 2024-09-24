package test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/ccoveille/go-safecast"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func BulkUploadTest(t *testing.T, tester DatastoreTester) {
	testCases := []int{0, 1, 10, 100, 1_000, 10_000}

	for _, tc := range testCases {
		t.Run(strconv.Itoa(tc), func(t *testing.T) {
			require := require.New(t)
			ctx := context.Background()

			rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
			require.NoError(err)

			ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)
			bulkSource := testfixtures.NewBulkTupleGenerator(
				testfixtures.DocumentNS.Name,
				"viewer",
				testfixtures.UserNS.Name,
				tc,
				t,
			)

			// This is statically defined so we can cast straight.
			uintTc, _ := safecast.ToUint64(tc)
			_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				loaded, err := rwt.BulkLoad(ctx, bulkSource)
				require.NoError(err)
				require.Equal(uintTc, loaded)
				return err
			})
			require.NoError(err)

			tRequire := testfixtures.TupleChecker{Require: require, DS: ds}

			head, err := ds.HeadRevision(ctx)
			require.NoError(err)

			iter, err := ds.SnapshotReader(head).QueryRelationships(ctx, datastore.RelationshipsFilter{
				OptionalResourceType: testfixtures.DocumentNS.Name,
			})
			require.NoError(err)
			defer iter.Close()

			tRequire.VerifyIteratorCount(iter, tc)
		})
	}
}

func BulkUploadErrorsTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		inserted, err := rwt.BulkLoad(ctx, &onlyErrorSource{})

		// We can't check the specific error because pgx is not wrapping
		require.Error(err)
		require.Zero(inserted)
		return err
	})
	require.Error(err)
}

func BulkUploadAlreadyExistsSameCallErrorTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		inserted, err := rwt.BulkLoad(ctx, testfixtures.NewBulkTupleGenerator(
			testfixtures.DocumentNS.Name,
			"viewer",
			testfixtures.UserNS.Name,
			1,
			t,
		))
		require.NoError(err)
		require.Equal(uint64(1), inserted)

		_, serr := rwt.BulkLoad(ctx, testfixtures.NewBulkTupleGenerator(
			testfixtures.DocumentNS.Name,
			"viewer",
			testfixtures.UserNS.Name,
			1,
			t,
		))
		return serr
	}, options.WithDisableRetries(true))

	// NOTE: spanner does not return an error for duplicates.
	if err == nil {
		return
	}

	grpcutil.RequireStatus(t, codes.AlreadyExists, err)
}

func BulkUploadEditCaveat(t *testing.T, tester DatastoreTester) {
	tc := 10
	require := require.New(t)
	ctx := context.Background()

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)
	bulkSource := testfixtures.NewBulkTupleGenerator(
		testfixtures.DocumentNS.Name,
		"caveated_viewer",
		testfixtures.UserNS.Name,
		tc,
		t,
	)

	// This is statically defined so we can cast straight.
	uintTc, _ := safecast.ToUint64(tc)
	lastRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		loaded, err := rwt.BulkLoad(ctx, bulkSource)
		require.NoError(err)
		require.Equal(uintTc, loaded)
		return err
	})
	require.NoError(err)

	iter, err := ds.SnapshotReader(lastRevision).QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: testfixtures.DocumentNS.Name,
	})
	require.NoError(err)
	defer iter.Close()

	updates := make([]*core.RelationTupleUpdate, 0, tc)

	for found := iter.Next(); found != nil; found = iter.Next() {
		updates = append(updates, &core.RelationTupleUpdate{
			Operation: core.RelationTupleUpdate_TOUCH,
			Tuple: &core.RelationTuple{
				ResourceAndRelation: found.ResourceAndRelation,
				Subject:             found.Subject,
				Caveat: &core.ContextualizedCaveat{
					CaveatName: testfixtures.CaveatDef.Name,
					Context:    nil,
				},
			},
		})
	}

	require.Equal(tc, len(updates))

	lastRevision, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.WriteRelationships(ctx, updates)
		require.NoError(err)
		return err
	})
	require.NoError(err)

	iter, err = ds.SnapshotReader(lastRevision).QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: testfixtures.DocumentNS.Name,
	})
	require.NoError(err)
	defer iter.Close()

	foundChanged := 0

	for found := iter.Next(); found != nil; found = iter.Next() {
		require.NotNil(found.Caveat)
		require.NotEmpty(found.Caveat.CaveatName)
		foundChanged++
	}

	require.Equal(tc, foundChanged)
}

func BulkUploadAlreadyExistsErrorTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)

	// Bulk write a single relationship.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		inserted, err := rwt.BulkLoad(ctx, testfixtures.NewBulkTupleGenerator(
			testfixtures.DocumentNS.Name,
			"viewer",
			testfixtures.UserNS.Name,
			1,
			t,
		))
		require.NoError(err)
		require.Equal(uint64(1), inserted)
		return nil
	}, options.WithDisableRetries(true))
	require.NoError(err)

	// Bulk write it again and ensure we get the expected error.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		_, serr := rwt.BulkLoad(ctx, testfixtures.NewBulkTupleGenerator(
			testfixtures.DocumentNS.Name,
			"viewer",
			testfixtures.UserNS.Name,
			1,
			t,
		))
		return serr
	}, options.WithDisableRetries(true))

	// NOTE: spanner does not return an error for duplicates.
	if err == nil {
		return
	}

	grpcutil.RequireStatus(t, codes.AlreadyExists, err)
}

type onlyErrorSource struct{}

var errOnlyError = errors.New("source iterator error")

func (oes onlyErrorSource) Next(_ context.Context) (*core.RelationTuple, error) {
	return nil, errOnlyError
}

var _ datastore.BulkWriteRelationshipSource = onlyErrorSource{}
