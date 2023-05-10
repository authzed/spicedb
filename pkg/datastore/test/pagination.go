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
		resourceType string
		ordering     options.SortOrder
	}{
		{testfixtures.DocumentNS.Name, options.ByResource},
		{testfixtures.FolderNS.Name, options.ByResource},
		{testfixtures.UserNS.Name, options.ByResource},

		{testfixtures.DocumentNS.Name, options.BySubject},
		{testfixtures.FolderNS.Name, options.BySubject},
		{testfixtures.UserNS.Name, options.BySubject},
	}

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, rev := testfixtures.StandardDatastoreWithData(rawDS, require.New(t))
	tRequire := testfixtures.TupleChecker{Require: require.New(t), DS: ds}

	for _, tc := range testCases {
		tc := tc

		t.Run(fmt.Sprintf("%s-%d", tc.resourceType, tc.ordering), func(t *testing.T) {
			require := require.New(t)
			ctx := context.Background()

			expected := sortedStandardData(tc.resourceType, tc.ordering)

			// Check the snapshot reader order
			iter, err := ds.SnapshotReader(rev).QueryRelationships(ctx, datastore.RelationshipsFilter{
				ResourceType: tc.resourceType,
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
					ResourceType: tc.resourceType,
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

type (
	iterator func(ctx context.Context, reader datastore.Reader, limit uint64, cursor options.Cursor) (datastore.RelationshipIterator, error)
)

func forwardIterator(resourceType string, _ options.SortOrder) iterator {
	return func(ctx context.Context, reader datastore.Reader, limit uint64, cursor options.Cursor) (datastore.RelationshipIterator, error) {
		return reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
			ResourceType: resourceType,
		}, options.WithSort(options.ByResource), options.WithLimit(&limit), options.WithAfter(cursor))
	}
}

func reverseIterator(subjectType string, _ options.SortOrder) iterator {
	return func(ctx context.Context, reader datastore.Reader, limit uint64, cursor options.Cursor) (datastore.RelationshipIterator, error) {
		return reader.ReverseQueryRelationships(ctx, datastore.SubjectsFilter{
			SubjectType: subjectType,
		}, options.WithSortForReverse(options.ByResource), options.WithLimitForReverse(&limit), options.WithAfterForReverse(cursor))
	}
}

var orderedTestCases = []struct {
	name           string
	objectType     string
	sortOrder      options.SortOrder
	isForwardQuery bool
}{
	{
		"document resources by resource",
		testfixtures.DocumentNS.Name,
		options.ByResource,
		true,
	},
	{
		"folder resources by resource",
		testfixtures.FolderNS.Name,
		options.ByResource,
		true,
	},
	{
		"user resources by resource",
		testfixtures.UserNS.Name,
		options.ByResource,
		true,
	},
	{
		"resources with user subject",
		testfixtures.UserNS.Name,
		options.ByResource,
		false,
	},
}

func OrderedLimitTest(t *testing.T, tester DatastoreTester) {
	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, rev := testfixtures.StandardDatastoreWithData(rawDS, require.New(t))
	tRequire := testfixtures.TupleChecker{Require: require.New(t), DS: ds}

	for _, tc := range orderedTestCases {
		expected := sortedStandardData(tc.objectType, tc.sortOrder)
		if !tc.isForwardQuery {
			expected = sortedStandardDataBySubject(tc.objectType, tc.sortOrder)
		}

		for limit := 1; limit <= len(expected); limit++ {
			testLimit := uint64(limit)

			t.Run(tc.name, func(t *testing.T) {
				require := require.New(t)
				ctx := context.Background()

				foreachTxType(ctx, ds, rev, func(reader datastore.Reader) {
					var iter datastore.RelationshipIterator
					var err error

					if tc.isForwardQuery {
						iter, err = forwardIterator(tc.objectType, tc.sortOrder)(ctx, reader, testLimit, nil)
					} else {
						iter, err = reverseIterator(tc.objectType, tc.sortOrder)(ctx, reader, testLimit, nil)
					}

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
	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, rev := testfixtures.StandardDatastoreWithData(rawDS, require.New(t))
	tRequire := testfixtures.TupleChecker{Require: require.New(t), DS: ds}

	for _, tc := range orderedTestCases {
		expected := sortedStandardData(tc.objectType, tc.sortOrder)
		if !tc.isForwardQuery {
			expected = sortedStandardDataBySubject(tc.objectType, tc.sortOrder)
		}

		for batchSize := 1; batchSize <= len(expected); batchSize++ {
			testLimit := uint64(batchSize)
			expected := expected

			t.Run(fmt.Sprintf("%s-batches-%d", tc.name, batchSize), func(t *testing.T) {
				require := require.New(t)
				ctx := context.Background()

				foreachTxType(ctx, ds, rev, func(reader datastore.Reader) {
					cursor := options.Cursor(nil)
					for offset := 0; offset <= len(expected); offset += batchSize {
						var iter datastore.RelationshipIterator
						var err error

						if tc.isForwardQuery {
							iter, err = forwardIterator(tc.objectType, tc.sortOrder)(ctx, reader, testLimit, cursor)
						} else {
							iter, err = reverseIterator(tc.objectType, tc.sortOrder)(ctx, reader, testLimit, cursor)
						}

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

func CursorErrorsTest(t *testing.T, tester DatastoreTester) {
	testCases := []struct {
		order              options.SortOrder
		defaultCursorError error
	}{
		{options.Unsorted, datastore.ErrCursorsWithoutSorting},
		{options.ByResource, nil},
	}

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, rev := testfixtures.StandardDatastoreWithData(rawDS, require.New(t))
	ctx := context.Background()

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Order-%d", tc.order), func(t *testing.T) {
			require := require.New(t)

			foreachTxType(ctx, ds, rev, func(reader datastore.Reader) {
				iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
					ResourceType: testfixtures.DocumentNS.Name,
				}, options.WithSort(tc.order))
				require.NoError(err)
				require.NotNil(iter)
				defer iter.Close()

				cursor, err := iter.Cursor()
				require.Nil(cursor)
				if tc.defaultCursorError != nil {
					require.ErrorIs(err, tc.defaultCursorError)
				} else {
					require.ErrorIs(err, datastore.ErrCursorEmpty)
				}

				val := iter.Next()
				require.NotNil(val)

				cursor, err = iter.Cursor()
				if tc.defaultCursorError != nil {
					require.Nil(cursor)
					require.ErrorIs(err, tc.defaultCursorError)
				} else {
					require.NotNil(cursor)
					require.Nil(err)
				}

				iter.Close()
				valAfterClose := iter.Next()
				require.Nil(valAfterClose)

				err = iter.Err()
				require.ErrorIs(err, datastore.ErrClosedIterator)
				cursorAfterClose, err := iter.Cursor()
				require.Nil(cursorAfterClose)
				require.ErrorIs(err, datastore.ErrClosedIterator)
			})
		})
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

func sortedStandardData(resourceType string, order options.SortOrder) []*core.RelationTuple {
	asTuples := lo.Map(testfixtures.StandardTuples, func(item string, _ int) *core.RelationTuple {
		return tuple.Parse(item)
	})

	filteredToType := lo.Filter(asTuples, func(item *core.RelationTuple, _ int) bool {
		return item.ResourceAndRelation.Namespace == resourceType
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

func sortedStandardDataBySubject(subjectType string, order options.SortOrder) []*core.RelationTuple {
	asTuples := lo.Map(testfixtures.StandardTuples, func(item string, _ int) *core.RelationTuple {
		return tuple.Parse(item)
	})

	filteredToType := lo.Filter(asTuples, func(item *core.RelationTuple, _ int) bool {
		if subjectType == "" {
			return true
		}
		return item.Subject.Namespace == subjectType
	})

	sort.Slice(filteredToType, func(i, j int) bool {
		lhsResource := tuple.StringONR(filteredToType[i].ResourceAndRelation)
		lhsSubject := tuple.StringONR(filteredToType[i].Subject)
		rhsResource := tuple.StringONR(filteredToType[j].ResourceAndRelation)
		rhsSubject := tuple.StringONR(filteredToType[j].Subject)
		switch order {
		case options.ByResource:
			return lhsResource < rhsResource || (lhsResource == rhsResource && lhsSubject < rhsSubject)
		default:
			panic("request for sorted test data with no sort order")
		}
	})

	return filteredToType
}
