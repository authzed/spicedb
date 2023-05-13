package test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
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

			_, err = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
				loaded, err := rwt.BulkLoad(ctx, bulkSource)
				require.NoError(err)
				require.Equal(uint64(tc), loaded)
				return err
			})
			require.NoError(err)

			tRequire := testfixtures.TupleChecker{Require: require, DS: ds}

			head, err := ds.HeadRevision(ctx)
			require.NoError(err)

			iter, err := ds.SnapshotReader(head).QueryRelationships(ctx, datastore.RelationshipsFilter{
				ResourceType: testfixtures.DocumentNS.Name,
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

	_, err = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		inserted, err := rwt.BulkLoad(ctx, &onlyErrorSource{})

		// We can't check the specific error because pgx is not wrapping
		require.Error(err)
		require.Zero(inserted)
		return err
	})
	require.Error(err)
}

type onlyErrorSource struct{}

var errOnlyError = errors.New("source iterator error")

func (oes onlyErrorSource) Next(_ context.Context) (*core.RelationTuple, error) {
	return nil, errOnlyError
}

var _ datastore.BulkWriteRelationshipSource = onlyErrorSource{}
