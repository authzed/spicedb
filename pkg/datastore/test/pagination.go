package test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/ccoveille/go-safecast"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
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
	tRequire := testfixtures.RelationshipChecker{Require: require.New(t), DS: ds}

	for _, tc := range testCases {
		tc := tc

		t.Run(fmt.Sprintf("%s-%d", tc.resourceType, tc.ordering), func(t *testing.T) {
			require := require.New(t)
			ctx := context.Background()

			expected := sortedStandardData(tc.resourceType, tc.ordering)

			// Check the snapshot reader order
			iter, err := ds.SnapshotReader(rev).QueryRelationships(ctx, datastore.RelationshipsFilter{
				OptionalResourceType: tc.resourceType,
			}, options.WithSort(tc.ordering))

			require.NoError(err)
			tRequire.VerifyOrderedIteratorResults(iter, expected...)

			// Check a reader from with a transaction
			_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				iter, err := rwt.QueryRelationships(ctx, datastore.RelationshipsFilter{
					OptionalResourceType: tc.resourceType,
				}, options.WithSort(tc.ordering))
				require.NoError(err)
				tRequire.VerifyOrderedIteratorResults(iter, expected...)
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
	tRequire := testfixtures.RelationshipChecker{Require: require.New(t), DS: ds}

	for _, objectType := range testCases {
		expected := sortedStandardData(objectType, options.ByResource)
		for limit := 1; limit <= len(expected)+1; limit++ {
			testLimit, _ := safecast.ToUint64(limit)
			t.Run(fmt.Sprintf("%s-%d", objectType, limit), func(t *testing.T) {
				require := require.New(t)
				ctx := context.Background()

				foreachTxType(ctx, ds, rev, func(reader datastore.Reader) {
					iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
						OptionalResourceType: objectType,
					}, options.WithLimit(&testLimit))

					require.NoError(err)

					expectedCount := limit
					if expectedCount > len(expected) {
						expectedCount = len(expected)
					}

					tRequire.VerifyIteratorCount(iter, expectedCount)
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
			OptionalResourceType: resourceType,
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
	tRequire := testfixtures.RelationshipChecker{Require: require.New(t), DS: ds}

	for _, tc := range orderedTestCases {
		expected := sortedStandardData(tc.objectType, tc.sortOrder)
		if !tc.isForwardQuery {
			expected = sortedStandardDataBySubject(tc.objectType, tc.sortOrder)
		}

		for limit := 1; limit <= len(expected); limit++ {
			testLimit, _ := safecast.ToUint64(limit)

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
					tRequire.VerifyOrderedIteratorResults(iter, expected[0:limit]...)
				})
			})
		}
	}
}

func ResumeTest(t *testing.T, tester DatastoreTester) {
	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, rev := testfixtures.StandardDatastoreWithData(rawDS, require.New(t))
	tRequire := testfixtures.RelationshipChecker{Require: require.New(t), DS: ds}

	for _, tc := range orderedTestCases {
		expected := sortedStandardData(tc.objectType, tc.sortOrder)
		if !tc.isForwardQuery {
			expected = sortedStandardDataBySubject(tc.objectType, tc.sortOrder)
		}

		for batchSize := 1; batchSize <= len(expected); batchSize++ {
			testLimit, _ := safecast.ToUint64(batchSize)
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

						upperBound := offset + batchSize
						if upperBound > len(expected) {
							upperBound = len(expected)
						}

						cursor = tRequire.VerifyOrderedIteratorResults(iter, expected[offset:upperBound]...)
					}
				})
			})
		}
	}
}

func ReverseQueryFilteredOverMultipleValuesCursorTest(t *testing.T, tester DatastoreTester) {
	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	// Create a datastore with the standard schema but no data.
	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require.New(t))

	// Add test relationships.
	rev, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("document:firstdoc#viewer@user:alice")),
			tuple.Create(tuple.MustParse("document:firstdoc#viewer@user:tom")),
			tuple.Create(tuple.MustParse("document:firstdoc#viewer@user:fred")),
			tuple.Create(tuple.MustParse("document:seconddoc#viewer@user:alice")),
			tuple.Create(tuple.MustParse("document:seconddoc#viewer@user:*")),
			tuple.Create(tuple.MustParse("document:thirddoc#viewer@user:*")),
		})
	})
	require.NoError(t, err)

	// Issue a reverse query call with a limit.
	for _, sortBy := range []options.SortOrder{options.ByResource, options.BySubject} {
		t.Run(fmt.Sprintf("SortBy-%d", sortBy), func(t *testing.T) {
			reader := ds.SnapshotReader(rev)

			var limit uint64 = 2
			var cursor options.Cursor

			foundTuples := mapz.NewSet[string]()

			for i := 0; i < 5; i++ {
				iter, err := reader.ReverseQueryRelationships(context.Background(), datastore.SubjectsFilter{
					SubjectType:        testfixtures.UserNS.Name,
					OptionalSubjectIds: []string{"alice", "tom", "fred", "*"},
				}, options.WithResRelation(&options.ResourceRelation{
					Namespace: "document",
					Relation:  "viewer",
				}), options.WithSortForReverse(sortBy), options.WithLimitForReverse(&limit), options.WithAfterForReverse(cursor))
				require.NoError(t, err)

				encounteredTuples := mapz.NewSet[string]()
				for rel, err := range iter {
					require.NoError(t, err)
					require.True(t, encounteredTuples.Add(tuple.MustString(rel)))
					cursor = options.ToCursor(rel)
				}

				require.LessOrEqual(t, encounteredTuples.Len(), 2)
				foundTuples = foundTuples.Union(encounteredTuples)
				if encounteredTuples.IsEmpty() {
					break
				}
			}

			require.Equal(t, 6, foundTuples.Len())
		})
	}
}

func ReverseQueryCursorTest(t *testing.T, tester DatastoreTester) {
	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	// Create a datastore with the standard schema but no data.
	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require.New(t))

	// Add test relationships.
	rev, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("document:firstdoc#viewer@user:alice")),
			tuple.Create(tuple.MustParse("document:firstdoc#viewer@user:tom")),
			tuple.Create(tuple.MustParse("document:firstdoc#viewer@user:fred")),
			tuple.Create(tuple.MustParse("document:seconddoc#viewer@user:alice")),
			tuple.Create(tuple.MustParse("document:seconddoc#viewer@user:*")),
			tuple.Create(tuple.MustParse("document:thirddoc#viewer@user:*")),
		})
	})
	require.NoError(t, err)

	// Issue a reverse query call with a limit.
	for _, sortBy := range []options.SortOrder{options.ByResource, options.BySubject} {
		t.Run(fmt.Sprintf("SortBy-%d", sortBy), func(t *testing.T) {
			reader := ds.SnapshotReader(rev)

			var limit uint64 = 2
			var cursor options.Cursor

			foundTuples := mapz.NewSet[string]()

			for i := 0; i < 5; i++ {
				iter, err := reader.ReverseQueryRelationships(context.Background(), datastore.SubjectsFilter{
					SubjectType: testfixtures.UserNS.Name,
				}, options.WithSortForReverse(sortBy), options.WithLimitForReverse(&limit), options.WithAfterForReverse(cursor))
				require.NoError(t, err)

				encounteredTuples := mapz.NewSet[string]()
				for rel, err := range iter {
					require.NoError(t, err)
					require.True(t, encounteredTuples.Add(tuple.MustString(rel)))
					cursor = options.ToCursor(rel)
				}

				require.LessOrEqual(t, encounteredTuples.Len(), 2)
				foundTuples = foundTuples.Union(encounteredTuples)
				if encounteredTuples.IsEmpty() {
					break
				}
			}

			require.Equal(t, 6, foundTuples.Len())
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

	_, _ = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		fn(rwt)
		return nil
	})
}

func sortedStandardData(resourceType string, order options.SortOrder) []tuple.Relationship {
	asTuples := lo.Map(testfixtures.StandardRelationships, func(item string, _ int) tuple.Relationship {
		return tuple.MustParse(item)
	})

	filteredToType := lo.Filter(asTuples, func(item tuple.Relationship, _ int) bool {
		return item.Resource.ObjectType == resourceType
	})

	sort.Slice(filteredToType, func(i, j int) bool {
		lhsResource := tuple.StringONR(filteredToType[i].Resource)
		lhsSubject := tuple.StringONR(filteredToType[i].Subject)
		rhsResource := tuple.StringONR(filteredToType[j].Resource)
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

func sortedStandardDataBySubject(subjectType string, order options.SortOrder) []tuple.Relationship {
	asTuples := lo.Map(testfixtures.StandardRelationships, func(item string, _ int) tuple.Relationship {
		return tuple.MustParse(item)
	})

	filteredToType := lo.Filter(asTuples, func(item tuple.Relationship, _ int) bool {
		if subjectType == "" {
			return true
		}
		return item.Subject.ObjectType == subjectType
	})

	sort.Slice(filteredToType, func(i, j int) bool {
		lhsResource := tuple.StringONR(filteredToType[i].Resource)
		lhsSubject := tuple.StringONR(filteredToType[i].Subject)
		rhsResource := tuple.StringONR(filteredToType[j].Resource)
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
