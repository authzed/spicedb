package test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
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

			var testTuples []*core.RelationTuple

			lastRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				for i := 0; i < numTuples; i++ {
					resourceName := fmt.Sprintf("resource%d", i)
					userName := fmt.Sprintf("user%d", i)

					newTuple := makeTestTuple(resourceName, userName)
					testTuples = append(testTuples, newTuple)

					err := rwt.WriteRelationships(
						[]*v1.RelationshipUpdate{{
							Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
							Relationship: tuple.MustToRelationship(newTuple),
						}},
					)
					require.NoError(err)
				}

				return nil
			})
			require.NoError(err)

			for _, toCheck := range testTuples {
				tRequire.TupleExists(ctx, toCheck, lastRevision)
			}

			// Write a duplicate tuple to make sure the datastore rejects it
			_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				return rwt.WriteRelationships(
					[]*v1.RelationshipUpdate{{
						Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
						Relationship: tuple.MustToRelationship(testTuples[0]),
					}},
				)
			})
			require.Error(err)

			dsReader := ds.SnapshotReader(lastRevision)
			for _, tupleToFind := range testTuples {
				tupleUserset := tupleToFind.User.GetUserset()

				// Check that we can find the tuple a number of ways
				iter, err := dsReader.QueryRelationships(ctx, &v1.RelationshipFilter{
					ResourceType:       tupleToFind.ObjectAndRelation.Namespace,
					OptionalResourceId: tupleToFind.ObjectAndRelation.ObjectId,
				})
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				iter, err = dsReader.QueryRelationships(ctx, &v1.RelationshipFilter{
					ResourceType: tupleToFind.ObjectAndRelation.Namespace,
				}, options.WithUsersets(tupleUserset))
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				iter, err = dsReader.QueryRelationships(ctx, &v1.RelationshipFilter{
					ResourceType:       tupleToFind.ObjectAndRelation.Namespace,
					OptionalResourceId: tupleToFind.ObjectAndRelation.ObjectId,
					OptionalRelation:   tupleToFind.ObjectAndRelation.Relation,
				})
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				iter, err = dsReader.QueryRelationships(ctx, &v1.RelationshipFilter{
					ResourceType:       tupleToFind.ObjectAndRelation.Namespace,
					OptionalResourceId: tupleToFind.ObjectAndRelation.ObjectId,
				}, options.WithUsersets(tupleUserset))
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				iter, err = dsReader.QueryRelationships(ctx, &v1.RelationshipFilter{
					ResourceType:     tupleToFind.ObjectAndRelation.Namespace,
					OptionalRelation: tupleToFind.ObjectAndRelation.Relation,
				}, options.WithUsersets(tupleUserset))
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				iter, err = dsReader.QueryRelationships(ctx, &v1.RelationshipFilter{
					ResourceType:     tupleToFind.ObjectAndRelation.Namespace,
					OptionalRelation: tupleToFind.ObjectAndRelation.Relation,
				}, options.WithUsersets(tupleUserset), options.WithLimit(options.LimitOne))
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				iter, err = dsReader.ReverseQueryRelationships(
					ctx,
					tuple.UsersetToSubjectFilter(tupleUserset),
					options.WithResRelation(&options.ResourceRelation{
						Namespace: tupleToFind.ObjectAndRelation.Namespace,
						Relation:  tupleToFind.ObjectAndRelation.Relation,
					}),
				)
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				iter, err = dsReader.ReverseQueryRelationships(
					ctx,
					tuple.UsersetToSubjectFilter(tupleUserset),
					options.WithResRelation(&options.ResourceRelation{
						Namespace: tupleToFind.ObjectAndRelation.Namespace,
						Relation:  tupleToFind.ObjectAndRelation.Relation,
					}),
					options.WithReverseLimit(options.LimitOne),
				)
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				// Check that we fail to find the tuple with the wrong filters
				iter, err = dsReader.QueryRelationships(ctx, &v1.RelationshipFilter{
					ResourceType:       tupleToFind.ObjectAndRelation.Namespace,
					OptionalResourceId: tupleToFind.ObjectAndRelation.ObjectId,
					OptionalRelation:   "fake",
				})
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter)

				incorrectUserset := &core.ObjectAndRelation{
					Namespace: tupleUserset.Namespace,
					ObjectId:  tupleUserset.ObjectId,
					Relation:  "fake",
				}
				iter, err = dsReader.QueryRelationships(ctx, &v1.RelationshipFilter{
					ResourceType: tupleToFind.ObjectAndRelation.Namespace,
				}, options.WithUsersets(incorrectUserset))
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter)

				iter, err = dsReader.QueryRelationships(ctx, &v1.RelationshipFilter{
					ResourceType:       tupleToFind.ObjectAndRelation.Namespace,
					OptionalResourceId: tupleToFind.ObjectAndRelation.ObjectId,
					OptionalRelation:   tupleToFind.ObjectAndRelation.Relation,
				}, options.WithUsersets(incorrectUserset))
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter)

				iter, err = dsReader.QueryRelationships(ctx, &v1.RelationshipFilter{
					ResourceType:       tupleToFind.ObjectAndRelation.Namespace,
					OptionalResourceId: tupleToFind.ObjectAndRelation.ObjectId,
					OptionalRelation:   "fake",
				}, options.WithUsersets(tupleUserset))
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter)

				iter, err = dsReader.QueryRelationships(ctx, &v1.RelationshipFilter{
					ResourceType:       tupleToFind.ObjectAndRelation.Namespace,
					OptionalResourceId: "fake",
					OptionalRelation:   tupleToFind.ObjectAndRelation.Relation,
				}, options.WithUsersets(tupleUserset))
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter)

				iter, err = dsReader.QueryRelationships(ctx, &v1.RelationshipFilter{
					ResourceType:       tupleToFind.ObjectAndRelation.Namespace,
					OptionalResourceId: "fake",
					OptionalRelation:   tupleToFind.ObjectAndRelation.Relation,
				}, options.WithUsersets(tupleUserset), options.WithLimit(options.LimitOne))
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter)

				iter, err = dsReader.ReverseQueryRelationships(
					ctx,
					tuple.UsersetToSubjectFilter(incorrectUserset),
					options.WithResRelation(&options.ResourceRelation{
						Namespace: tupleToFind.ObjectAndRelation.Namespace,
						Relation:  tupleToFind.ObjectAndRelation.Relation,
					}),
				)
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter)
			}

			// Check a query that returns a number of tuples
			iter, err := dsReader.QueryRelationships(ctx, &v1.RelationshipFilter{
				ResourceType: testResourceNamespace,
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testTuples...)

			// Filter it down to a single tuple with a userset
			iter, err = dsReader.QueryRelationships(ctx, &v1.RelationshipFilter{
				ResourceType: testResourceNamespace,
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:       testUserNamespace,
					OptionalSubjectId: "user0",
				},
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testTuples[0])

			// Check for larger reverse queries.
			iter, err = dsReader.ReverseQueryRelationships(ctx, &v1.SubjectFilter{
				SubjectType: testUserNamespace,
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testTuples...)

			// Check limit.
			if len(testTuples) > 1 {
				limit := uint64(len(testTuples) - 1)
				iter, err := dsReader.ReverseQueryRelationships(ctx, &v1.SubjectFilter{
					SubjectType: testUserNamespace,
				}, options.WithReverseLimit(&limit))
				require.NoError(err)
				tRequire.VerifyIteratorCount(iter, len(testTuples)-1)
			}

			// Check that we can find the group of tuples too
			iter, err = dsReader.QueryRelationships(ctx, &v1.RelationshipFilter{
				ResourceType: testTuples[0].ObjectAndRelation.Namespace,
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testTuples...)

			iter, err = dsReader.QueryRelationships(ctx, &v1.RelationshipFilter{
				ResourceType:     testTuples[0].ObjectAndRelation.Namespace,
				OptionalRelation: testTuples[0].ObjectAndRelation.Relation,
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testTuples...)

			// Try some bad queries
			iter, err = dsReader.QueryRelationships(ctx, &v1.RelationshipFilter{
				ResourceType:       testTuples[0].ObjectAndRelation.Namespace,
				OptionalResourceId: "fakeobectid",
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter)

			iter, err = dsReader.QueryRelationships(ctx, &v1.RelationshipFilter{
				ResourceType: testTuples[0].ObjectAndRelation.Namespace,
			}, options.WithUsersets(&core.ObjectAndRelation{
				Namespace: "test/user",
				ObjectId:  "fakeuser",
				Relation:  ellipsis,
			}))
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter)

			// Delete the first tuple
			deletedAt, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				err := rwt.WriteRelationships(
					[]*v1.RelationshipUpdate{{
						Operation:    v1.RelationshipUpdate_OPERATION_DELETE,
						Relationship: tuple.MustToRelationship(testTuples[0]),
					}},
				)
				require.NoError(err)
				return err
			})
			require.NoError(err)

			// Delete it AGAIN (idempotent delete) and make sure there's no error
			_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				err := rwt.WriteRelationships(
					[]*v1.RelationshipUpdate{{
						Operation:    v1.RelationshipUpdate_OPERATION_DELETE,
						Relationship: tuple.MustToRelationship(testTuples[0]),
					}},
				)
				require.NoError(err)
				return err
			})
			require.NoError(err)

			// Verify it can still be read at the old revision
			tRequire.TupleExists(ctx, testTuples[0], lastRevision)

			// Verify that it does not show up at the new revision
			tRequire.NoTupleExists(ctx, testTuples[0], deletedAt)
			alreadyDeletedIter, err := ds.SnapshotReader(deletedAt).QueryRelationships(
				ctx,
				&v1.RelationshipFilter{
					ResourceType: testTuples[0].ObjectAndRelation.Namespace,
				},
			)
			require.NoError(err)
			tRequire.VerifyIteratorResults(alreadyDeletedIter, testTuples[1:]...)

			// Write it back
			returnedAt, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				err := rwt.WriteRelationships(
					[]*v1.RelationshipUpdate{{
						Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
						Relationship: tuple.MustToRelationship(testTuples[0]),
					}},
				)
				require.NoError(err)
				return err
			})
			require.NoError(err)
			tRequire.TupleExists(ctx, testTuples[0], returnedAt)

			// Delete with DeleteRelationship
			deletedAt, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				err := rwt.DeleteRelationships(&v1.RelationshipFilter{
					ResourceType: testResourceNamespace,
				})
				require.NoError(err)
				return err
			})
			require.NoError(err)
			tRequire.NoTupleExists(ctx, testTuples[0], deletedAt)
		})
	}
}

// DeleteRelationshipsTest tests whether or not the requirements for deleting
// relationships hold for a particular datastore.
func DeleteRelationshipsTest(t *testing.T, tester DatastoreTester) {
	var testTuples []*core.RelationTuple
	for i := 0; i < 10; i++ {
		newTuple := makeTestTuple(fmt.Sprintf("resource%d", i), fmt.Sprintf("user%d", i%2))
		testTuples = append(testTuples, newTuple)
	}
	testTuples[len(testTuples)-1].ObjectAndRelation.Relation = "writer"

	table := []struct {
		name                      string
		inputTuples               []*core.RelationTuple
		filter                    *v1.RelationshipFilter
		expectedExistingTuples    []*core.RelationTuple
		expectedNonExistingTuples []*core.RelationTuple
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
			[]*core.RelationTuple{testTuples[len(testTuples)-1]},
		},
		{
			"subjectID",
			testTuples,
			&v1.RelationshipFilter{
				ResourceType:          testResourceNamespace,
				OptionalSubjectFilter: &v1.SubjectFilter{SubjectType: testUserNamespace, OptionalSubjectId: "user0"},
			},
			[]*core.RelationTuple{testTuples[1], testTuples[3], testTuples[5], testTuples[7], testTuples[9]},
			[]*core.RelationTuple{testTuples[0], testTuples[2], testTuples[4], testTuples[6], testTuples[8]},
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

			_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				for _, tpl := range tt.inputTuples {
					update := &v1.RelationshipUpdate{
						Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
						Relationship: tuple.MustToRelationship(tpl),
					}
					err := rwt.WriteRelationships([]*v1.RelationshipUpdate{update})
					require.NoError(err)
				}
				return nil
			})
			require.NoError(err)

			deletedAt, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				err := rwt.DeleteRelationships(tt.filter)
				require.NoError(err)
				return err
			})
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
		firstWrite, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			err := rwt.WriteRelationships([]*v1.RelationshipUpdate{{
				Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
				Relationship: tuple.MustToRelationship(newTuple),
			}})
			require.NoError(err)
			return err
		})
		require.NoError(err)

		// Check that we can read at the just written revision
		err = ds.CheckRevision(ctx, firstWrite)
		require.NoError(err)

		// Wait the duration required to allow the revision to expire
		time.Sleep(testGCDuration * 2)

		// Write another tuple which will allow the first revision to expire
		nextWrite, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			err := rwt.WriteRelationships(
				[]*v1.RelationshipUpdate{{
					Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
					Relationship: tuple.MustToRelationship(newTuple),
				}},
			)
			require.NoError(err)
			return err
		})
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

				var testTuples []*core.RelationTuple

				ctx := context.Background()

				// Add test tuples on the same resource but with different users.
				var lastRevision datastore.Revision

				usersets := []*core.ObjectAndRelation{}
				for i := 0; i < numTuples; i++ {
					resourceName := "theresource"
					userName := fmt.Sprintf("user%d", i)

					newTuple := makeTestTuple(resourceName, userName)
					testTuples = append(testTuples, newTuple)
					usersets = append(usersets, newTuple.User.GetUserset())

					writtenAt, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
						err := rwt.WriteRelationships([]*v1.RelationshipUpdate{{
							Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
							Relationship: tuple.MustToRelationship(newTuple),
						}})
						require.NoError(err)
						return err
					})
					require.NoError(err)
					require.True(writtenAt.GreaterThan(lastRevision))

					tRequire.TupleExists(ctx, newTuple, writtenAt)

					lastRevision = writtenAt
				}

				// Query for the tuples as a single query.
				iter, err := ds.SnapshotReader(lastRevision).QueryRelationships(ctx, &v1.RelationshipFilter{
					ResourceType: testResourceNamespace,
				}, options.SetUsersets(usersets))
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, testTuples...)
			})
		}
	})
}

func MultipleReadsInRWTTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		it, err := rwt.QueryRelationships(ctx, &v1.RelationshipFilter{
			ResourceType: "document",
		})
		require.NoError(err)
		it.Close()

		it, err = rwt.QueryRelationships(ctx, &v1.RelationshipFilter{
			ResourceType: "folder",
		})
		require.NoError(err)
		it.Close()

		return nil
	})
	require.NoError(err)
}

func ConcurrentWriteSerializationTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	g := errgroup.Group{}

	waitToStart := make(chan struct{})
	waitToFinish := make(chan struct{})
	waitToStartCloser := sync.Once{}
	waitToFinishCloser := sync.Once{}

	g.Go(func() error {
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			iter, err := rwt.QueryRelationships(ctx, &v1.RelationshipFilter{
				ResourceType: testResourceNamespace,
			})
			iter.Close()
			require.NoError(err)

			// We do NOT assert the error here because serialization problems can manifest as errors
			// on the individual writes.
			err = rwt.WriteRelationships([]*v1.RelationshipUpdate{
				{
					Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
					Relationship: makeTestRelationship("new_resource", "new_user"),
				},
			})

			waitToStartCloser.Do(func() {
				close(waitToStart)
			})
			<-waitToFinish

			return err
		})
		require.NoError(err)
		return nil
	})

	<-waitToStart

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		defer waitToFinishCloser.Do(func() {
			close(waitToFinish)
		})

		return rwt.WriteRelationships([]*v1.RelationshipUpdate{
			{
				Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
				Relationship: makeTestRelationship("another_resource", "another_user"),
			},
		})
	})
	require.NoError(err)
	require.NoError(g.Wait())
}
