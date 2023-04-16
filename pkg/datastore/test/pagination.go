package test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func OrderingTest(t *testing.T, tester DatastoreTester) {
	testCases := []struct {
		objectType string
		ordering   options.SortOrder
	}{
		{testfixtures.DocumentNS.Name, options.ByResource},
		{testfixtures.DocumentNS.Name, options.BySubject},
		{testfixtures.FolderNS.Name, options.ByResource},
		{testfixtures.FolderNS.Name, options.BySubject},
		{testfixtures.UserNS.Name, options.ByResource},
		{testfixtures.UserNS.Name, options.BySubject},
	}

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, rev := testfixtures.StandardDatastoreWithData(rawDS, require.New(t))
	tRequire := testfixtures.TupleChecker{Require: require.New(t), DS: ds}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s-%d", tc.objectType, tc.ordering), func(t *testing.T) {
			require := require.New(t)
			ctx := context.Background()

			expected := sortedStandardData(tc.objectType, tc.ordering)

			// Check the snapshot reader order
			iter, err := ds.SnapshotReader(rev).QueryRelationships(ctx, datastore.RelationshipsFilter{
				ResourceType: tc.objectType,
			}, options.WithSort(tc.ordering))

			require.NoError(err)
			defer iter.Close()

			cursor, err := iter.Cursor()
			require.Empty(cursor)
			require.ErrorIs(err, datastore.ErrCursorEmpty)

			tRequire.VerifyOrderedIteratorResults(iter, expected...)

			cursor, err = iter.Cursor()
			if len(expected) > 0 {
				require.NotEmpty(cursor)
				require.NoError(err)
			} else {
				require.Empty(cursor)
				require.ErrorIs(err, datastore.ErrCursorEmpty)
			}

			// Check a reader from with a transaction
			_, err = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
				iter, err := rwt.QueryRelationships(ctx, datastore.RelationshipsFilter{
					ResourceType: tc.objectType,
				}, options.WithSort(tc.ordering))
				require.NoError(err)
				defer iter.Close()

				cursor, err := iter.Cursor()
				require.Empty(cursor)
				require.ErrorIs(err, datastore.ErrCursorEmpty)

				tRequire.VerifyOrderedIteratorResults(iter, expected...)

				cursor, err = iter.Cursor()
				if len(expected) > 0 {
					require.NotEmpty(cursor)
					require.NoError(err)
				} else {
					require.Empty(cursor)
					require.ErrorIs(err, datastore.ErrCursorEmpty)
				}

				return nil
			})
			require.NoError(err)
		})
	}
}

func LimitTest(t *testing.T, tester DatastoreTester) {
	testCases := []string{
		testfixtures.DocumentNS.Name,
		testfixtures.UserNS.Name,
		testfixtures.FolderNS.Name,
	}

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, rev := testfixtures.StandardDatastoreWithData(rawDS, require.New(t))
	tRequire := testfixtures.TupleChecker{Require: require.New(t), DS: ds}

	for _, objectType := range testCases {
		expected := sortedStandardData(objectType, options.ByResource)
		for limit := 1; limit <= len(expected)+1; limit++ {
			testLimit := uint64(limit)
			t.Run(fmt.Sprintf("%s-%d", objectType, limit), func(t *testing.T) {
				require := require.New(t)
				ctx := context.Background()

				foreachTxType(ctx, ds, rev, func(reader datastore.Reader) {
					iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
						ResourceType: objectType,
					}, options.WithLimit(&testLimit))

					require.NoError(err)
					defer iter.Close()

					expectedCount := limit
					if expectedCount > len(expected) {
						expectedCount = len(expected)
					}

					cursor, err := iter.Cursor()
					require.Empty(cursor)
					require.ErrorIs(err, datastore.ErrCursorsWithoutSorting)

					tRequire.VerifyIteratorCount(iter, expectedCount)

					cursor, err = iter.Cursor()
					require.Empty(cursor)
					require.ErrorIs(err, datastore.ErrCursorsWithoutSorting)
				})
			})
		}
	}
}

func OrderedLimitTest(t *testing.T, tester DatastoreTester) {
	testCases := []struct {
		objectType string
		ordering   options.SortOrder
	}{
		{testfixtures.DocumentNS.Name, options.ByResource},
		{testfixtures.DocumentNS.Name, options.BySubject},
		{testfixtures.FolderNS.Name, options.ByResource},
		{testfixtures.FolderNS.Name, options.BySubject},
		{testfixtures.UserNS.Name, options.ByResource},
		{testfixtures.UserNS.Name, options.BySubject},
	}

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, rev := testfixtures.StandardDatastoreWithData(rawDS, require.New(t))
	tRequire := testfixtures.TupleChecker{Require: require.New(t), DS: ds}

	for _, tc := range testCases {
		expected := sortedStandardData(tc.objectType, options.ByResource)
		for limit := 1; limit <= len(expected); limit++ {
			testLimit := uint64(limit)

			t.Run(fmt.Sprintf("%s-%d", tc.objectType, tc.ordering), func(t *testing.T) {
				require := require.New(t)
				ctx := context.Background()

				expected := sortedStandardData(tc.objectType, tc.ordering)

				foreachTxType(ctx, ds, rev, func(reader datastore.Reader) {
					iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
						ResourceType: tc.objectType,
					}, options.WithSort(tc.ordering), options.WithLimit(&testLimit))

					require.NoError(err)
					defer iter.Close()

					cursor, err := iter.Cursor()
					require.Empty(cursor)
					require.ErrorIs(err, datastore.ErrCursorEmpty)

					tRequire.VerifyOrderedIteratorResults(iter, expected[0:limit]...)

					cursor, err = iter.Cursor()
					if len(expected) > 0 {
						require.NotEmpty(cursor)
						require.NoError(err)
					} else {
						require.Empty(cursor)
						require.ErrorIs(err, datastore.ErrCursorEmpty)
					}
				})
			})
		}
	}
}

func ResumeTest(t *testing.T, tester DatastoreTester) {
	testCases := []struct {
		objectType string
		ordering   options.SortOrder
	}{
		{testfixtures.DocumentNS.Name, options.ByResource},
		{testfixtures.DocumentNS.Name, options.BySubject},
		{testfixtures.FolderNS.Name, options.ByResource},
		{testfixtures.FolderNS.Name, options.BySubject},
		{testfixtures.UserNS.Name, options.ByResource},
		{testfixtures.UserNS.Name, options.BySubject},
	}

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, rev := testfixtures.StandardDatastoreWithData(rawDS, require.New(t))
	tRequire := testfixtures.TupleChecker{Require: require.New(t), DS: ds}

	for _, tc := range testCases {
		expected := sortedStandardData(tc.objectType, options.ByResource)
		for batchSize := 1; batchSize <= len(expected); batchSize++ {
			testLimit := uint64(batchSize)

			t.Run(fmt.Sprintf("%s-%d-batches-%d", tc.objectType, tc.ordering, batchSize), func(t *testing.T) {
				require := require.New(t)
				ctx := context.Background()

				expected := sortedStandardData(tc.objectType, tc.ordering)

				foreachTxType(ctx, ds, rev, func(reader datastore.Reader) {
					// Test that if you ask for resume without an order we error
					_, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
						ResourceType: tc.objectType,
					}, options.WithLimit(&testLimit), options.WithAfter(&core.RelationTuple{}))
					require.ErrorIs(err, datastore.ErrCursorsWithoutSorting)

					cursor := options.Cursor(nil)
					for offset := 0; offset <= len(expected); offset += batchSize {
						iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
							ResourceType: tc.objectType,
						}, options.WithSort(tc.ordering), options.WithLimit(&testLimit), options.WithAfter(cursor))
						require.NoError(err)
						defer iter.Close()

						emptyCursor, err := iter.Cursor()
						require.Empty(emptyCursor)
						require.ErrorIs(err, datastore.ErrCursorEmpty)

						upperBound := offset + batchSize
						if upperBound > len(expected) {
							upperBound = len(expected)
						}
						tRequire.VerifyOrderedIteratorResults(iter, expected[offset:upperBound]...)

						cursor, err = iter.Cursor()
						if upperBound-offset > 0 {
							require.NotEmpty(cursor)
							require.NoError(err)
						} else {
							require.Empty(cursor)
							require.ErrorIs(err, datastore.ErrCursorEmpty)
						}
					}
				})
			})
		}
	}
}

func foreachTxType(
	ctx context.Context,
	ds datastore.Datastore,
	snapshotRev datastore.Revision,
	fn func(reader datastore.Reader),
) {
	reader := ds.SnapshotReader(snapshotRev)
	fn(reader)

	_, _ = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		fn(rwt)
		return nil
	})
}

func sortedStandardData(objectType string, order options.SortOrder) []*core.RelationTuple {
	asTuples := lo.Map(testfixtures.StandardTuples, func(item string, _ int) *core.RelationTuple {
		return tuple.Parse(item)
	})

	filteredToType := lo.Filter(asTuples, func(item *core.RelationTuple, _ int) bool {
		return item.ResourceAndRelation.Namespace == objectType
	})

	sort.Slice(filteredToType, func(i, j int) bool {
		lhsResource := tuple.StringONR(filteredToType[i].ResourceAndRelation)
		lhsSubject := tuple.StringONR(filteredToType[i].Subject)
		rhsResource := tuple.StringONR(filteredToType[j].ResourceAndRelation)
		rhsSubject := tuple.StringONR(filteredToType[j].Subject)
		switch order {
		case options.ByResource:
			return lhsResource < rhsResource || (lhsResource == rhsResource && lhsSubject < rhsSubject)
		case options.BySubject:
			return lhsSubject < rhsSubject || (lhsSubject == rhsSubject && lhsResource < rhsResource)
		default:
			panic("request for sorted test data with no sort order")
		}
	})

	return filteredToType
}
