package test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	testUserNamespace     = "test/user"
	testResourceNamespace = "test/resource"
	testReaderRelation    = "reader"
	ellipsis              = "..."
)

// SimpleTest tests whether or not the requirements for simple reading and
// writing of relationships hold for a particular datastore.
func SimpleTest(t *testing.T, tester DatastoreTester) {
	testCases := []int{1, 2, 4, 32, 256}

	for _, numTuples := range testCases {
		t.Run(strconv.Itoa(numTuples), func(t *testing.T) {
			require := require.New(t)

			ds, err := tester.New(0, veryLargeGCWindow, 1)
			require.NoError(err)
			defer ds.Close()

			ctx := context.Background()

			ok, err := ds.IsReady(ctx)
			require.NoError(err)
			require.True(ok)

			setupDatastore(ds, require)

			tRequire := testfixtures.TupleChecker{Require: require, DS: ds}

			var testTuples []*v0.RelationTuple

			var lastRevision datastore.Revision
			for i := 0; i < numTuples; i++ {
				resourceName := fmt.Sprintf("resource%d", i)
				userName := fmt.Sprintf("user%d", i)

				newTuple := makeTestTuple(resourceName, userName)
				testTuples = append(testTuples, newTuple)

				writtenAt, err := ds.WriteTuples(
					ctx,
					nil,
					[]*v1.RelationshipUpdate{{
						Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
						Relationship: tuple.MustToRelationship(newTuple),
					}},
				)
				require.NoError(err)
				require.True(writtenAt.GreaterThan(lastRevision))

				tRequire.TupleExists(ctx, newTuple, writtenAt)

				lastRevision = writtenAt
			}

			// Write a duplicate tuple to make sure the datastore rejects it
			_, err = ds.WriteTuples(
				ctx,
				nil,
				[]*v1.RelationshipUpdate{{
					Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
					Relationship: tuple.MustToRelationship(testTuples[0]),
				}},
			)
			require.Error(err)

			for _, tupleToFind := range testTuples {
				// Check that we can find the tuple a number of ways
				iter, err := ds.QueryTuples(ctx, &v1.RelationshipFilter{
					ResourceType:       tupleToFind.ObjectAndRelation.Namespace,
					OptionalResourceId: tupleToFind.ObjectAndRelation.ObjectId,
				}, lastRevision)
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				iter, err = ds.QueryTuples(ctx, &v1.RelationshipFilter{
					ResourceType: tupleToFind.ObjectAndRelation.Namespace,
				}, lastRevision, options.WithUsersets(tupleToFind.User.GetUserset()))
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				iter, err = ds.QueryTuples(ctx, &v1.RelationshipFilter{
					ResourceType:       tupleToFind.ObjectAndRelation.Namespace,
					OptionalResourceId: tupleToFind.ObjectAndRelation.ObjectId,
					OptionalRelation:   tupleToFind.ObjectAndRelation.Relation,
				}, lastRevision)
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				iter, err = ds.QueryTuples(ctx, &v1.RelationshipFilter{
					ResourceType:       tupleToFind.ObjectAndRelation.Namespace,
					OptionalResourceId: tupleToFind.ObjectAndRelation.ObjectId,
				}, lastRevision, options.WithUsersets(tupleToFind.User.GetUserset()))
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				iter, err = ds.QueryTuples(ctx, &v1.RelationshipFilter{
					ResourceType:     tupleToFind.ObjectAndRelation.Namespace,
					OptionalRelation: tupleToFind.ObjectAndRelation.Relation,
				}, lastRevision, options.WithUsersets(tupleToFind.User.GetUserset()))
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				iter, err = ds.QueryTuples(ctx, &v1.RelationshipFilter{
					ResourceType:     tupleToFind.ObjectAndRelation.Namespace,
					OptionalRelation: tupleToFind.ObjectAndRelation.Relation,
				}, lastRevision, options.WithUsersets(tupleToFind.User.GetUserset()), options.WithLimit(options.LimitOne))
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				iter, err = ds.ReverseQueryTuples(
					ctx,
					tuple.UsersetToSubjectFilter(tupleToFind.User.GetUserset()),
					lastRevision,
					options.WithResRelation(&options.ResourceRelation{
						Namespace: tupleToFind.ObjectAndRelation.Namespace,
						Relation:  tupleToFind.ObjectAndRelation.Relation,
					}),
				)
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				iter, err = ds.ReverseQueryTuples(
					ctx,
					tuple.UsersetToSubjectFilter(tupleToFind.User.GetUserset()),
					lastRevision,
					options.WithResRelation(&options.ResourceRelation{
						Namespace: tupleToFind.ObjectAndRelation.Namespace,
						Relation:  tupleToFind.ObjectAndRelation.Relation,
					}),
					options.WithReverseLimit(options.LimitOne),
				)
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)
			}

			// Check a query that returns a number of tuples
			iter, err := ds.QueryTuples(ctx, &v1.RelationshipFilter{
				ResourceType: testResourceNamespace,
			}, lastRevision)
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testTuples...)

			// Filter it down to a single tuple with a userset
			iter, err = ds.QueryTuples(ctx, &v1.RelationshipFilter{
				ResourceType: testResourceNamespace,
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:       testUserNamespace,
					OptionalSubjectId: "user0",
				},
			}, lastRevision)
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testTuples[0])

			// Check for larger reverse queries.
			iter, err = ds.ReverseQueryTuples(ctx, &v1.SubjectFilter{
				SubjectType: testUserNamespace,
			}, lastRevision)
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testTuples...)

			// Check limit.
			if len(testTuples) > 1 {
				limit := uint64(len(testTuples) - 1)
				iter, err := ds.ReverseQueryTuples(ctx, &v1.SubjectFilter{
					SubjectType: testUserNamespace,
				}, lastRevision, options.WithReverseLimit(&limit))
				require.NoError(err)
				tRequire.VerifyIteratorCount(iter, len(testTuples)-1)
			}

			// Check that we can find the group of tuples too
			iter, err = ds.QueryTuples(ctx, &v1.RelationshipFilter{
				ResourceType: testTuples[0].ObjectAndRelation.Namespace,
			}, lastRevision)
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testTuples...)

			iter, err = ds.QueryTuples(ctx, &v1.RelationshipFilter{
				ResourceType:     testTuples[0].ObjectAndRelation.Namespace,
				OptionalRelation: testTuples[0].ObjectAndRelation.Relation,
			}, lastRevision)
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testTuples...)

			// Try some bad queries
			iter, err = ds.QueryTuples(ctx, &v1.RelationshipFilter{
				ResourceType:       testTuples[0].ObjectAndRelation.Namespace,
				OptionalResourceId: "fakeobectid",
			}, lastRevision)
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter)

			iter, err = ds.QueryTuples(ctx, &v1.RelationshipFilter{
				ResourceType: testTuples[0].ObjectAndRelation.Namespace,
			}, lastRevision, options.WithUsersets(&v0.ObjectAndRelation{
				Namespace: "test/user",
				ObjectId:  "fakeuser",
				Relation:  ellipsis,
			}))
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter)

			// Delete the first tuple
			deletedAt, err := ds.WriteTuples(
				ctx,
				nil,
				[]*v1.RelationshipUpdate{{
					Operation:    v1.RelationshipUpdate_OPERATION_DELETE,
					Relationship: tuple.MustToRelationship(testTuples[0]),
				}},
			)
			require.NoError(err)

			// Delete it AGAIN (idempotent delete) and make sure there's no error
			_, err = ds.WriteTuples(
				ctx,
				nil,
				[]*v1.RelationshipUpdate{{
					Operation:    v1.RelationshipUpdate_OPERATION_DELETE,
					Relationship: tuple.MustToRelationship(testTuples[0]),
				}},
			)
			require.NoError(err)

			// Verify it can still be read at the old revision
			tRequire.TupleExists(ctx, testTuples[0], lastRevision)

			// Verify that it does not show up at the new revision
			tRequire.NoTupleExists(ctx, testTuples[0], deletedAt)
			alreadyDeletedIter, err := ds.QueryTuples(ctx, &v1.RelationshipFilter{
				ResourceType: testTuples[0].ObjectAndRelation.Namespace,
			}, deletedAt)
			require.NoError(err)
			tRequire.VerifyIteratorResults(alreadyDeletedIter, testTuples[1:]...)

			// Write it back
			returnedAt, err := ds.WriteTuples(
				ctx,
				nil,
				[]*v1.RelationshipUpdate{{
					Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
					Relationship: tuple.MustToRelationship(testTuples[0]),
				}},
			)
			require.NoError(err)
			tRequire.TupleExists(ctx, testTuples[0], returnedAt)

			// Delete with DeleteRelationship
			deletedAt, err = ds.DeleteRelationships(ctx, nil, &v1.RelationshipFilter{
				ResourceType: testResourceNamespace,
			})
			require.NoError(err)
			tRequire.NoTupleExists(ctx, testTuples[0], deletedAt)
		})
	}
}

// WritePreconditionsTest tests whether or not the requirements for checking
// preconditions via WriteTuples hold for a particular datastore.
func WritePreconditionsTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(err)
	defer ds.Close()

	setupDatastore(ds, require)

	first := makeTestTuple("first", "owner")
	second := makeTestTuple("second", "owner")

	ctx := context.Background()

	_, err = ds.WriteTuples(
		ctx,
		[]*v1.Precondition{{
			Operation: v1.Precondition_OPERATION_MUST_MATCH,
			Filter:    tuple.MustToFilter(first),
		}},
		[]*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
			Relationship: tuple.MustToRelationship(second),
		}},
	)
	require.True(errors.As(err, &datastore.ErrPreconditionFailed{}))

	_, err = ds.WriteTuples(ctx, nil, []*v1.RelationshipUpdate{{
		Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
		Relationship: tuple.MustToRelationship(first),
	}})
	require.NoError(err)

	_, err = ds.WriteTuples(
		ctx,
		[]*v1.Precondition{{
			Operation: v1.Precondition_OPERATION_MUST_MATCH,
			Filter:    tuple.MustToFilter(first),
		}},
		[]*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
			Relationship: tuple.MustToRelationship(second),
		}},
	)
	require.NoError(err)
}

// DeletePreconditionsTest tests whether or not the requirements for checking
// preconditions via DeleteRelationships hold for a particular datastore.
func DeletePreconditionsTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(err)
	defer ds.Close()

	setupDatastore(ds, require)

	relTpl := makeTestTuple("first", "owner")
	filter := &v1.RelationshipFilter{
		ResourceType:       testResourceNamespace,
		OptionalResourceId: "second",
	}

	ctx := context.Background()

	_, err = ds.DeleteRelationships(
		ctx,
		[]*v1.Precondition{{
			Operation: v1.Precondition_OPERATION_MUST_MATCH,
			Filter:    tuple.MustToFilter(relTpl),
		}},
		filter,
	)
	require.True(errors.As(err, &datastore.ErrPreconditionFailed{}))

	_, err = ds.DeleteRelationships(ctx, nil, filter)
	require.NoError(err)

	_, err = ds.WriteTuples(ctx, nil, []*v1.RelationshipUpdate{{
		Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
		Relationship: tuple.MustToRelationship(relTpl),
	}})
	require.NoError(err)

	_, err = ds.DeleteRelationships(
		ctx,
		[]*v1.Precondition{{
			Operation: v1.Precondition_OPERATION_MUST_MATCH,
			Filter:    tuple.MustToFilter(relTpl),
		}},
		filter,
	)
	require.NoError(err)
}

// DeleteRelationshipsTest tests whether or not the requirements for deleting
// relationships hold for a particular datastore.
func DeleteRelationshipsTest(t *testing.T, tester DatastoreTester) {
	var testTuples []*v0.RelationTuple
	for i := 0; i < 10; i++ {
		newTuple := makeTestTuple(fmt.Sprintf("resource%d", i), fmt.Sprintf("user%d", i%2))
		testTuples = append(testTuples, newTuple)
	}
	testTuples[len(testTuples)-1].ObjectAndRelation.Relation = "writer"

	table := []struct {
		name                      string
		inputTuples               []*v0.RelationTuple
		filter                    *v1.RelationshipFilter
		expectedExistingTuples    []*v0.RelationTuple
		expectedNonExistingTuples []*v0.RelationTuple
	}{
		{
			"resourceID",
			testTuples,
			&v1.RelationshipFilter{
				ResourceType:       testResourceNamespace,
				OptionalResourceId: "resource0",
			},
			testTuples[1:],
			testTuples[:1],
		},
		{
			"relation",
			testTuples,
			&v1.RelationshipFilter{
				ResourceType:     testResourceNamespace,
				OptionalRelation: "writer",
			},
			testTuples[:len(testTuples)-1],
			[]*v0.RelationTuple{testTuples[len(testTuples)-1]},
		},
		{
			"subjectID",
			testTuples,
			&v1.RelationshipFilter{
				ResourceType:          testResourceNamespace,
				OptionalSubjectFilter: &v1.SubjectFilter{SubjectType: testUserNamespace, OptionalSubjectId: "user0"},
			},
			[]*v0.RelationTuple{testTuples[1], testTuples[3], testTuples[5], testTuples[7], testTuples[9]},
			[]*v0.RelationTuple{testTuples[0], testTuples[2], testTuples[4], testTuples[6], testTuples[8]},
		},
		{
			"subjectRelation",
			testTuples,
			&v1.RelationshipFilter{
				ResourceType:          testResourceNamespace,
				OptionalSubjectFilter: &v1.SubjectFilter{SubjectType: testUserNamespace, OptionalRelation: &v1.SubjectFilter_RelationFilter{Relation: ""}},
			},
			nil,
			testTuples,
		},
		{
			"duplicates",
			append(testTuples, testTuples[0]),
			&v1.RelationshipFilter{
				ResourceType:       testResourceNamespace,
				OptionalResourceId: "resource0",
			},
			testTuples[1:],
			testTuples[:1],
		},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			ctx := context.Background()

			ds, err := tester.New(0, veryLargeGCWindow, 1)
			require.NoError(err)
			defer ds.Close()

			setupDatastore(ds, require)

			tRequire := testfixtures.TupleChecker{Require: require, DS: ds}

			for _, tpl := range tt.inputTuples {
				update := &v1.RelationshipUpdate{
					Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
					Relationship: tuple.MustToRelationship(tpl),
				}
				_, err = ds.WriteTuples(ctx, nil, []*v1.RelationshipUpdate{update})
				require.NoError(err)
			}

			deletedAt, err := ds.DeleteRelationships(ctx, nil, tt.filter)
			require.NoError(err)

			for _, tpl := range tt.expectedExistingTuples {
				tRequire.TupleExists(ctx, tpl, deletedAt)
			}

			for _, tpl := range tt.expectedNonExistingTuples {
				tRequire.NoTupleExists(ctx, tpl, deletedAt)
			}
		})
	}
}

// InvalidReadsTest tests whether or not the requirements for reading via
// invalid revisions hold for a particular datastore.
func InvalidReadsTest(t *testing.T, tester DatastoreTester) {
	t.Run("revision expiration", func(t *testing.T) {
		testGCDuration := 600 * time.Millisecond

		require := require.New(t)

		ds, err := tester.New(0, testGCDuration, 1)
		require.NoError(err)
		defer ds.Close()

		setupDatastore(ds, require)

		ctx := context.Background()

		// Check that we get an error when there are no revisions
		err = ds.CheckRevision(ctx, datastore.NoRevision)

		revisionErr := datastore.ErrInvalidRevision{}
		require.True(errors.As(err, &revisionErr))

		newTuple := makeTestTuple("one", "one")
		firstWrite, err := ds.WriteTuples(
			ctx,
			nil,
			[]*v1.RelationshipUpdate{{
				Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
				Relationship: tuple.MustToRelationship(newTuple),
			}},
		)
		require.NoError(err)

		// Check that we can read at the just written revision
		err = ds.CheckRevision(ctx, firstWrite)
		require.NoError(err)

		// Wait the duration required to allow the revision to expire
		time.Sleep(testGCDuration * 2)

		// Write another tuple which will allow the first revision to expire
		nextWrite, err := ds.WriteTuples(
			ctx,
			nil,
			[]*v1.RelationshipUpdate{{
				Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
				Relationship: tuple.MustToRelationship(newTuple),
			}},
		)
		require.NoError(err)

		// Check that we can read at the just written revision
		err = ds.CheckRevision(ctx, nextWrite)
		require.NoError(err)

		// Check that we can no longer read the old revision (now allowed to expire)
		err = ds.CheckRevision(ctx, firstWrite)
		require.True(errors.As(err, &revisionErr))
		require.Equal(datastore.RevisionStale, revisionErr.Reason())

		// Check that we can't read a revision that's ahead of the latest
		err = ds.CheckRevision(ctx, nextWrite.Add(decimal.NewFromInt(1_000_000_000)))
		require.True(errors.As(err, &revisionErr))
		require.Equal(datastore.RevisionInFuture, revisionErr.Reason())
	})
}

// UsersetsTest tests whether or not the requirements for reading usersets hold
// for a particular datastore.
func UsersetsTest(t *testing.T, tester DatastoreTester) {
	testCases := []int{1, 2, 4, 32, 1024}

	t.Run("multiple usersets tuple query", func(t *testing.T) {
		for _, numTuples := range testCases {
			t.Run(strconv.Itoa(numTuples), func(t *testing.T) {
				require := require.New(t)

				ds, err := tester.New(0, veryLargeGCWindow, 1)
				require.NoError(err)
				defer ds.Close()

				setupDatastore(ds, require)

				tRequire := testfixtures.TupleChecker{Require: require, DS: ds}

				var testTuples []*v0.RelationTuple

				ctx := context.Background()

				// Add test tuples on the same resource but with different users.
				var lastRevision datastore.Revision

				usersets := []*v0.ObjectAndRelation{}
				for i := 0; i < numTuples; i++ {
					resourceName := "theresource"
					userName := fmt.Sprintf("user%d", i)

					newTuple := makeTestTuple(resourceName, userName)
					testTuples = append(testTuples, newTuple)
					usersets = append(usersets, newTuple.User.GetUserset())

					writtenAt, err := ds.WriteTuples(ctx, nil, []*v1.RelationshipUpdate{{
						Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
						Relationship: tuple.MustToRelationship(newTuple),
					}})
					require.NoError(err)
					require.True(writtenAt.GreaterThan(lastRevision))

					tRequire.TupleExists(ctx, newTuple, writtenAt)

					lastRevision = writtenAt
				}

				// Query for the tuples as a single query.
				iter, err := ds.QueryTuples(ctx, &v1.RelationshipFilter{
					ResourceType: testResourceNamespace,
				}, lastRevision, options.SetUsersets(usersets))
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, testTuples...)
			})
		}
	})
}
