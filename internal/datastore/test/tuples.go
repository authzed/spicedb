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
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	testUserNamespace     = "test/user"
	testResourceNamespace = "test/resource"
	testReaderRelation    = "reader"
	ellipsis              = "..."
)

func TestSimple(t *testing.T, tester DatastoreTester) {
	testCases := []int{1, 2, 4, 32, 1024}

	for _, numTuples := range testCases {
		t.Run(strconv.Itoa(numTuples), func(t *testing.T) {
			require := require.New(t)

			ds, err := tester.New(0, veryLargeGCWindow, 1)
			require.NoError(err)

			ctx := context.Background()

			setupDatastore(ds, require)
			require.True(ds.IsReady(ctx))

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
					[]*v0.RelationTupleUpdate{tuple.Create(newTuple)},
				)
				require.NoError(err)
				require.True(writtenAt.GreaterThan(lastRevision))

				tRequire.TupleExists(ctx, newTuple, writtenAt)

				lastRevision = writtenAt
			}

			for _, tupleToFind := range testTuples {
				// Check that we can find the tuple a number of ways
				rq := ds.ReverseQueryTuplesFromSubject(tupleToFind.User.GetUserset(), lastRevision)

				queries := []datastore.CommonTupleQuery{
					ds.QueryTuples(&v1.ObjectFilter{
						ObjectType:       tupleToFind.ObjectAndRelation.Namespace,
						OptionalObjectId: tupleToFind.ObjectAndRelation.ObjectId,
					}, lastRevision),
					ds.QueryTuples(&v1.ObjectFilter{
						ObjectType: tupleToFind.ObjectAndRelation.Namespace,
					}, lastRevision).WithUsersetFilter(onrToFilter(tupleToFind.User.GetUserset())),
					ds.QueryTuples(&v1.ObjectFilter{
						ObjectType:       tupleToFind.ObjectAndRelation.Namespace,
						OptionalObjectId: tupleToFind.ObjectAndRelation.ObjectId,
						OptionalRelation: tupleToFind.ObjectAndRelation.Relation,
					}, lastRevision),
					ds.QueryTuples(&v1.ObjectFilter{
						ObjectType:       tupleToFind.ObjectAndRelation.Namespace,
						OptionalObjectId: tupleToFind.ObjectAndRelation.ObjectId,
					}, lastRevision).WithUsersetFilter(onrToFilter(tupleToFind.User.GetUserset())),
					ds.QueryTuples(&v1.ObjectFilter{
						ObjectType:       tupleToFind.ObjectAndRelation.Namespace,
						OptionalRelation: tupleToFind.ObjectAndRelation.Relation,
					}, lastRevision).WithUsersetFilter(onrToFilter(tupleToFind.User.GetUserset())),
					ds.QueryTuples(&v1.ObjectFilter{
						ObjectType:       tupleToFind.ObjectAndRelation.Namespace,
						OptionalRelation: tupleToFind.ObjectAndRelation.Relation,
					}, lastRevision).WithUsersetFilter(onrToFilter(tupleToFind.User.GetUserset())).Limit(1),

					rq.WithObjectRelation(tupleToFind.ObjectAndRelation.Namespace, tupleToFind.ObjectAndRelation.Relation),
					rq.WithObjectRelation(tupleToFind.ObjectAndRelation.Namespace, tupleToFind.ObjectAndRelation.Relation).Limit(1),
				}
				for _, query := range queries {
					iter, err := query.Execute(ctx)
					require.NoError(err)
					tRequire.VerifyIteratorResults(iter, tupleToFind)
				}
			}

			// Check for larger reverse queries.
			rq := ds.ReverseQueryTuplesFromSubjectRelation(testUserNamespace, ellipsis, lastRevision)
			iter, err := rq.Execute(ctx)
			require.NoError(err)

			tRequire.VerifyIteratorResults(iter, testTuples...)

			// Check limit.
			if len(testTuples) > 1 {
				iter, err = rq.Limit(uint64(len(testTuples) - 1)).Execute(ctx)
				require.NoError(err)
				tRequire.VerifyIteratorCount(iter, len(testTuples)-1)
			}

			// Check that we can find the group of tuples too
			queries := []datastore.TupleQuery{
				ds.QueryTuples(&v1.ObjectFilter{
					ObjectType: testTuples[0].ObjectAndRelation.Namespace,
				}, lastRevision),
				ds.QueryTuples(&v1.ObjectFilter{
					ObjectType:       testTuples[0].ObjectAndRelation.Namespace,
					OptionalRelation: testTuples[0].ObjectAndRelation.Relation,
				}, lastRevision),
			}
			for _, query := range queries {
				iter, err := query.Execute(ctx)
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, testTuples...)
			}

			// Try some bad queries
			badQueries := []datastore.TupleQuery{
				ds.QueryTuples(&v1.ObjectFilter{
					ObjectType:       testTuples[0].ObjectAndRelation.Namespace,
					OptionalObjectId: "fakeobjectid",
				}, lastRevision),
				ds.QueryTuples(&v1.ObjectFilter{
					ObjectType: testTuples[0].ObjectAndRelation.Namespace,
				}, lastRevision).WithUsersetFilter(onrToFilter(&v0.ObjectAndRelation{
					Namespace: "test/user",
					ObjectId:  "fakeuser",
					Relation:  ellipsis,
				})),
			}
			for _, badQuery := range badQueries {
				iter, err := badQuery.Execute(ctx)
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter)
			}

			// Delete the first tuple
			deletedAt, err := ds.WriteTuples(
				ctx,
				nil,
				[]*v0.RelationTupleUpdate{tuple.Delete(testTuples[0])},
			)
			require.NoError(err)

			// Delete it AGAIN (idempotent delete) and make sure there's no error
			_, err = ds.WriteTuples(
				ctx,
				nil,
				[]*v0.RelationTupleUpdate{tuple.Delete(testTuples[0])},
			)
			require.NoError(err)

			// Verify it can still be read at the old revision
			tRequire.TupleExists(ctx, testTuples[0], lastRevision)

			// Verify that it does not show up at the new revision
			tRequire.NoTupleExists(ctx, testTuples[0], deletedAt)
			alreadyDeletedIter, err := ds.QueryTuples(&v1.ObjectFilter{
				ObjectType: testTuples[0].ObjectAndRelation.Namespace,
			}, deletedAt).Execute(ctx)
			require.NoError(err)
			tRequire.VerifyIteratorResults(alreadyDeletedIter, testTuples[1:]...)
		})
	}
}

func TestPreconditions(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(err)

	setupDatastore(ds, require)

	first := makeTestTuple("first", "owner")
	second := makeTestTuple("second", "owner")

	ctx := context.Background()

	_, err = ds.WriteTuples(
		ctx,
		[]*v0.RelationTuple{first},
		[]*v0.RelationTupleUpdate{tuple.Create(second)},
	)
	require.True(errors.As(err, &datastore.ErrPreconditionFailed{}))

	_, err = ds.WriteTuples(ctx, nil, []*v0.RelationTupleUpdate{tuple.Create(first)})
	require.NoError(err)

	_, err = ds.WriteTuples(
		ctx,
		[]*v0.RelationTuple{first},
		[]*v0.RelationTupleUpdate{tuple.Create(second)},
	)
	require.NoError(err)
}

func TestInvalidReads(t *testing.T, tester DatastoreTester) {
	t.Run("revision expiration", func(t *testing.T) {
		testGCDuration := 40 * time.Millisecond

		require := require.New(t)

		ds, err := tester.New(0, testGCDuration, 1)
		require.NoError(err)

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
			[]*v0.RelationTupleUpdate{tuple.Create(newTuple)},
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
			[]*v0.RelationTupleUpdate{tuple.Touch(newTuple)},
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

func TestUsersets(t *testing.T, tester DatastoreTester) {
	testCases := []int{1, 2, 4, 32, 1024}

	t.Run("multiple usersets tuple query", func(t *testing.T) {
		for _, numTuples := range testCases {
			t.Run(strconv.Itoa(numTuples), func(t *testing.T) {
				require := require.New(t)

				ds, err := tester.New(0, veryLargeGCWindow, 1)
				require.NoError(err)

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

					writtenAt, err := ds.WriteTuples(
						ctx,
						nil,
						[]*v0.RelationTupleUpdate{tuple.Create(newTuple)},
					)
					require.NoError(err)
					require.True(writtenAt.GreaterThan(lastRevision))

					tRequire.TupleExists(ctx, newTuple, writtenAt)

					lastRevision = writtenAt
				}

				// Query for the tuples as a single query.
				iter, err := ds.QueryTuples(&v1.ObjectFilter{
					ObjectType: testResourceNamespace,
				}, lastRevision).WithUsersets(usersets).Execute(ctx)
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, testTuples...)
			})
		}
	})
}

func onrToFilter(userset *v0.ObjectAndRelation) *v1.ObjectFilter {
	return &v1.ObjectFilter{
		ObjectType:       userset.Namespace,
		OptionalObjectId: userset.ObjectId,
		OptionalRelation: userset.Relation,
	}
}
