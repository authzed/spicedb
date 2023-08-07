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
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	testUserNamespace     = "test/user"
	testResourceNamespace = "test/resource"
	testGroupNamespace    = "test/group"
	testReaderRelation    = "reader"
	testMemberRelation    = "member"
	ellipsis              = "..."
)

// SimpleTest tests whether or not the requirements for simple reading and
// writing of relationships hold for a particular datastore.
func SimpleTest(t *testing.T, tester DatastoreTester) {
	testCases := []int{1, 2, 4, 32, 256}

	for _, numTuples := range testCases {
		numTuples := numTuples
		t.Run(strconv.Itoa(numTuples), func(t *testing.T) {
			require := require.New(t)

			ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
			require.NoError(err)
			defer ds.Close()

			ctx := context.Background()

			ok, err := ds.ReadyState(ctx)
			require.NoError(err)
			require.True(ok.IsReady)

			setupDatastore(ds, require)

			tRequire := testfixtures.TupleChecker{Require: require, DS: ds}

			var testTuples []*core.RelationTuple
			for i := 0; i < numTuples; i++ {
				resourceName := fmt.Sprintf("resource%d", i)
				userName := fmt.Sprintf("user%d", i)

				newTuple := makeTestTuple(resourceName, userName)
				testTuples = append(testTuples, newTuple)
			}

			lastRevision, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, testTuples...)
			require.NoError(err)

			for _, toCheck := range testTuples {
				tRequire.TupleExists(ctx, toCheck, lastRevision)
			}

			// Write a duplicate tuple to make sure the datastore rejects it
			_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, testTuples...)
			require.Error(err)

			dsReader := ds.SnapshotReader(lastRevision)
			for _, tupleToFind := range testTuples {
				tupleSubject := tupleToFind.Subject

				// Check that we can find the tuple a number of ways
				iter, err := dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
					ResourceType:        tupleToFind.ResourceAndRelation.Namespace,
					OptionalResourceIds: []string{tupleToFind.ResourceAndRelation.ObjectId},
				})
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				iter, err = dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
					ResourceType:             tupleToFind.ResourceAndRelation.Namespace,
					OptionalResourceIds:      []string{tupleToFind.ResourceAndRelation.ObjectId},
					OptionalResourceRelation: tupleToFind.ResourceAndRelation.Relation,
				})
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				iter, err = dsReader.ReverseQueryRelationships(
					ctx,
					onrToSubjectsFilter(tupleSubject),
					options.WithResRelation(&options.ResourceRelation{
						Namespace: tupleToFind.ResourceAndRelation.Namespace,
						Relation:  tupleToFind.ResourceAndRelation.Relation,
					}),
				)
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				iter, err = dsReader.ReverseQueryRelationships(
					ctx,
					onrToSubjectsFilter(tupleSubject),
					options.WithResRelation(&options.ResourceRelation{
						Namespace: tupleToFind.ResourceAndRelation.Namespace,
						Relation:  tupleToFind.ResourceAndRelation.Relation,
					}),
					options.WithLimitForReverse(options.LimitOne),
				)
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				// Check that we fail to find the tuple with the wrong filters
				iter, err = dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
					ResourceType:             tupleToFind.ResourceAndRelation.Namespace,
					OptionalResourceIds:      []string{tupleToFind.ResourceAndRelation.ObjectId},
					OptionalResourceRelation: "fake",
				})
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter)

				incorrectUserset := &core.ObjectAndRelation{
					Namespace: tupleSubject.Namespace,
					ObjectId:  tupleSubject.ObjectId,
					Relation:  "fake",
				}

				iter, err = dsReader.ReverseQueryRelationships(
					ctx,
					onrToSubjectsFilter(incorrectUserset),
					options.WithResRelation(&options.ResourceRelation{
						Namespace: tupleToFind.ResourceAndRelation.Namespace,
						Relation:  tupleToFind.ResourceAndRelation.Relation,
					}),
				)
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter)
			}

			// Check a query that returns a number of tuples
			iter, err := dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
				ResourceType: testResourceNamespace,
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testTuples...)

			// Filter it down to a single tuple with a userset
			iter, err = dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
				ResourceType: testResourceNamespace,
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: testUserNamespace,
						OptionalSubjectIds:  []string{"user0"},
					},
				},
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testTuples[0])

			// Check for larger reverse queries.
			iter, err = dsReader.ReverseQueryRelationships(ctx, datastore.SubjectsFilter{
				SubjectType: testUserNamespace,
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testTuples...)

			// Check limit.
			if len(testTuples) > 1 {
				limit := uint64(len(testTuples) - 1)
				iter, err := dsReader.ReverseQueryRelationships(ctx, datastore.SubjectsFilter{
					SubjectType: testUserNamespace,
				}, options.WithLimitForReverse(&limit))
				require.NoError(err)
				defer iter.Close()
				tRequire.VerifyIteratorCount(iter, len(testTuples)-1)
			}

			// Check that we can find the group of tuples too
			iter, err = dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
				ResourceType: testTuples[0].ResourceAndRelation.Namespace,
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testTuples...)

			iter, err = dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
				ResourceType:             testTuples[0].ResourceAndRelation.Namespace,
				OptionalResourceRelation: testTuples[0].ResourceAndRelation.Relation,
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testTuples...)

			// Try some bad queries
			iter, err = dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
				ResourceType:        testTuples[0].ResourceAndRelation.Namespace,
				OptionalResourceIds: []string{"fakeobectid"},
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter)

			// Delete the first tuple
			deletedAt, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_DELETE, testTuples[0])
			require.NoError(err)

			// Delete it AGAIN (idempotent delete) and make sure there's no error
			_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_DELETE, testTuples[0])
			require.NoError(err)

			// Verify it can still be read at the old revision
			tRequire.TupleExists(ctx, testTuples[0], lastRevision)

			// Verify that it does not show up at the new revision
			tRequire.NoTupleExists(ctx, testTuples[0], deletedAt)
			alreadyDeletedIter, err := ds.SnapshotReader(deletedAt).QueryRelationships(
				ctx,
				datastore.RelationshipsFilter{
					ResourceType: testTuples[0].ResourceAndRelation.Namespace,
				},
			)
			require.NoError(err)
			tRequire.VerifyIteratorResults(alreadyDeletedIter, testTuples[1:]...)

			// Write it back
			returnedAt, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, testTuples[0])
			require.NoError(err)
			tRequire.TupleExists(ctx, testTuples[0], returnedAt)

			// Delete with DeleteRelationship
			deletedAt, err = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
				err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
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

func ObjectIDsTest(t *testing.T, tester DatastoreTester) {
	testCases := []string{
		"simple",
		"google|123123123123",
		"--=base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==",
		"veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylong",
	}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			ctx := context.Background()
			require := require.New(t)

			ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
			require.NoError(err)
			defer ds.Close()

			tpl := makeTestTuple(tc, tc)
			require.NoError(tpl.Validate())

			// Write the test tuple
			_, err = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
				return rwt.WriteRelationships(ctx, []*core.RelationTupleUpdate{
					{
						Operation: core.RelationTupleUpdate_CREATE,
						Tuple:     tpl,
					},
				})
			})
			require.NoError(err)

			// Read it back
			rev, err := ds.HeadRevision(ctx)
			require.NoError(err)
			iter, err := ds.SnapshotReader(rev).QueryRelationships(ctx, datastore.RelationshipsFilter{
				ResourceType:        testResourceNamespace,
				OptionalResourceIds: []string{tc},
			})
			require.NoError(err)
			defer iter.Close()

			first := iter.Next()
			require.NotNil(first)
			require.Equal(tc, first.ResourceAndRelation.ObjectId)
			require.Equal(tc, first.Subject.ObjectId)

			shouldBeNil := iter.Next()
			require.Nil(shouldBeNil)
			require.NoError(iter.Err())
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
	testTuples[len(testTuples)-1].ResourceAndRelation.Relation = "writer"

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
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			ctx := context.Background()

			ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
			require.NoError(err)
			defer ds.Close()

			setupDatastore(ds, require)

			tRequire := testfixtures.TupleChecker{Require: require, DS: ds}

			// TODO temporarily store tuples in multiple calls to ReadWriteTransaction since no Datastore
			// handles correctly duplicate tuples
			_, err = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
				for _, tpl := range tt.inputTuples {
					update := tuple.Touch(tpl)
					err := rwt.WriteRelationships(ctx, []*core.RelationTupleUpdate{update})
					if err != nil {
						return err
					}
				}
				return nil
			})
			require.NoError(err)

			deletedAt, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
				err := rwt.DeleteRelationships(ctx, tt.filter)
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

		ds, err := tester.New(0, veryLargeGCInterval, testGCDuration, 1)
		require.NoError(err)
		defer ds.Close()

		setupDatastore(ds, require)

		ctx := context.Background()

		// Check that we get an error when there are no revisions
		err = ds.CheckRevision(ctx, datastore.NoRevision)

		revisionErr := datastore.ErrInvalidRevision{}
		require.True(errors.As(err, &revisionErr))

		newTuple := makeTestTuple("one", "one")
		firstWrite, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, newTuple)
		require.NoError(err)

		// Check that we can read at the just written revision
		err = ds.CheckRevision(ctx, firstWrite)
		require.NoError(err)

		// Wait the duration required to allow the revision to expire
		time.Sleep(testGCDuration * 2)

		// Write another tuple which will allow the first revision to expire
		nextWrite, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, newTuple)
		require.NoError(err)

		// Check that we can read at the just written revision
		err = ds.CheckRevision(ctx, nextWrite)
		require.NoError(err)

		// Check that we can no longer read the old revision (now allowed to expire)
		err = ds.CheckRevision(ctx, firstWrite)
		require.True(errors.As(err, &revisionErr))
		require.Equal(datastore.RevisionStale, revisionErr.Reason())
	})
}

// DeleteNotExistantTest tests the deletion of a non-existant relationship.
func DeleteNotExistantTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	_, err = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		err := rwt.WriteRelationships(ctx, []*core.RelationTupleUpdate{
			tuple.Delete(tuple.MustParse("document:foo#viewer@user:tom#...")),
		})
		require.NoError(err)

		return nil
	})
	require.NoError(err)
}

// DeleteAlreadyDeletedTest tests the deletion of an already-deleted relationship.
func DeleteAlreadyDeletedTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	_, err = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		// Write the relationship.
		return rwt.WriteRelationships(ctx, []*core.RelationTupleUpdate{
			tuple.Create(tuple.MustParse("document:foo#viewer@user:tom#...")),
		})
	})
	require.NoError(err)

	_, err = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		// Delete the relationship.
		return rwt.WriteRelationships(ctx, []*core.RelationTupleUpdate{
			tuple.Delete(tuple.MustParse("document:foo#viewer@user:tom#...")),
		})
	})
	require.NoError(err)

	_, err = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		// Delete the relationship again.
		return rwt.WriteRelationships(ctx, []*core.RelationTupleUpdate{
			tuple.Delete(tuple.MustParse("document:foo#viewer@user:tom#...")),
		})
	})
	require.NoError(err)
}

// WriteDeleteWriteTest tests writing a relationship, deleting it, and then writing it again.
func WriteDeleteWriteTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	tpl := makeTestTuple("foo", "tom")
	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, tpl)
	require.NoError(err)

	ensureTuples(ctx, require, ds, tpl)

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_DELETE, tpl)
	require.NoError(err)

	ensureNotTuples(ctx, require, ds, tpl)

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, tpl)
	require.NoError(err)

	ensureTuples(ctx, require, ds, tpl)
}

// CreateAlreadyExistingTest tests creating a relationship twice.
func CreateAlreadyExistingTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	tpl1 := makeTestTuple("foo", "tom")
	tpl2 := makeTestTuple("foo", "sarah")
	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, tpl1, tpl2)
	require.NoError(err)

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, tpl1)
	require.ErrorAs(err, &common.CreateRelationshipExistsError{})
	require.Contains(err.Error(), "could not CREATE relationship ")
	grpcutil.RequireStatus(t, codes.AlreadyExists, err)
}

// TouchAlreadyExistingTest tests touching a relationship twice.
func TouchAlreadyExistingTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	tpl1 := makeTestTuple("foo", "tom")
	tpl2 := makeTestTuple("foo", "sarah")

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, tpl1, tpl2)
	require.NoError(err)

	ensureTuples(ctx, require, ds, tpl1, tpl2)

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, tpl1, tpl2)
	require.NoError(err)

	ensureTuples(ctx, require, ds, tpl1, tpl2)

	tpl3 := makeTestTuple("foo", "fred")
	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, tpl1, tpl3)
	require.NoError(err)

	ensureTuples(ctx, require, ds, tpl1, tpl2, tpl3)
}

// CreateDeleteTouchTest tests writing a relationship, deleting it, and then touching it.
func CreateDeleteTouchTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	tpl1 := makeTestTuple("foo", "tom")
	tpl2 := makeTestTuple("foo", "sarah")

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, tpl1, tpl2)
	require.NoError(err)

	ensureTuples(ctx, require, ds, tpl1, tpl2)

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_DELETE, tpl1, tpl2)
	require.NoError(err)

	ensureNotTuples(ctx, require, ds, tpl1, tpl2)

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, tpl1, tpl2)
	require.NoError(err)

	ensureTuples(ctx, require, ds, tpl1, tpl2)
}

// CreateTouchDeleteTouchTest tests writing a relationship, touching it, deleting it, and then touching it.
func CreateTouchDeleteTouchTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	tpl1 := makeTestTuple("foo", "tom")
	tpl2 := makeTestTuple("foo", "sarah")

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, tpl1, tpl2)
	require.NoError(err)

	ensureTuples(ctx, require, ds, tpl1, tpl2)

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, tpl1, tpl2)
	require.NoError(err)

	ensureTuples(ctx, require, ds, tpl1, tpl2)

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_DELETE, tpl1, tpl2)
	require.NoError(err)

	ensureNotTuples(ctx, require, ds, tpl1, tpl2)

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, tpl1, tpl2)
	require.NoError(err)

	ensureTuples(ctx, require, ds, tpl1, tpl2)
}

// TouchAlreadyExistingCaveatedTest tests touching a relationship twice.
func TouchAlreadyExistingCaveatedTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	tpl1 := tuple.MustWithCaveat(makeTestTuple("foo", "tom"), "formercaveat")
	tpl2 := makeTestTuple("foo", "sarah")
	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, tpl1, tpl2)
	require.NoError(err)

	ensureTuples(ctx, require, ds, tpl1, tpl2)

	ctpl1 := tuple.MustWithCaveat(makeTestTuple("foo", "tom"), "somecaveat")
	tpl3 := makeTestTuple("foo", "fred")

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, ctpl1, tpl3)
	require.NoError(err)

	ensureTuples(ctx, require, ds, tpl2, tpl3, ctpl1)
}

func MultipleReadsInRWTTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	_, err = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		it, err := rwt.QueryRelationships(ctx, datastore.RelationshipsFilter{
			ResourceType: "document",
		})
		require.NoError(err)
		it.Close()

		it, err = rwt.QueryRelationships(ctx, datastore.RelationshipsFilter{
			ResourceType: "folder",
		})
		require.NoError(err)
		it.Close()

		return nil
	})
	require.NoError(err)
}

// ConcurrentWriteSerializationTest uses goroutines and channels to intentionally set up a
// deadlocking dependency between transactions.
func ConcurrentWriteSerializationTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	g := errgroup.Group{}

	waitToStart := make(chan struct{})
	waitToFinish := make(chan struct{})
	waitToStartCloser := sync.Once{}
	waitToFinishCloser := sync.Once{}

	startTime := time.Now()

	g.Go(func() error {
		_, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
			iter, err := rwt.QueryRelationships(ctx, datastore.RelationshipsFilter{
				ResourceType: testResourceNamespace,
			})
			iter.Close()
			require.NoError(err)

			// We do NOT assert the error here because serialization problems can manifest as errors
			// on the individual writes.
			rtu := tuple.Touch(makeTestTuple("new_resource", "new_user"))
			err = rwt.WriteRelationships(ctx, []*core.RelationTupleUpdate{rtu})

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

	_, err = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		defer waitToFinishCloser.Do(func() {
			close(waitToFinish)
		})

		rtu := tuple.Touch(makeTestTuple("another_resource", "another_user"))
		return rwt.WriteRelationships(ctx, []*core.RelationTupleUpdate{rtu})
	})
	require.NoError(err)
	require.NoError(g.Wait())
	require.Less(time.Since(startTime), 10*time.Second)
}

func onrToSubjectsFilter(onr *core.ObjectAndRelation) datastore.SubjectsFilter {
	return datastore.SubjectsFilter{
		SubjectType:        onr.Namespace,
		OptionalSubjectIds: []string{onr.ObjectId},
		RelationFilter:     datastore.SubjectRelationFilter{}.WithNonEllipsisRelation(onr.Relation),
	}
}

func ensureTuples(ctx context.Context, require *require.Assertions, ds datastore.Datastore, tpls ...*core.RelationTuple) {
	ensureTuplesStatus(ctx, require, ds, tpls, true)
}

func ensureNotTuples(ctx context.Context, require *require.Assertions, ds datastore.Datastore, tpls ...*core.RelationTuple) {
	ensureTuplesStatus(ctx, require, ds, tpls, false)
}

func ensureTuplesStatus(ctx context.Context, require *require.Assertions, ds datastore.Datastore, tpls []*core.RelationTuple, mustExist bool) {
	headRev, err := ds.HeadRevision(ctx)
	require.NoError(err)

	reader := ds.SnapshotReader(headRev)

	for _, tpl := range tpls {
		iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
			ResourceType:             tpl.ResourceAndRelation.Namespace,
			OptionalResourceIds:      []string{tpl.ResourceAndRelation.ObjectId},
			OptionalResourceRelation: tpl.ResourceAndRelation.Relation,
			OptionalSubjectsSelectors: []datastore.SubjectsSelector{
				{
					OptionalSubjectType: tpl.Subject.Namespace,
					OptionalSubjectIds:  []string{tpl.Subject.ObjectId},
				},
			},
		})
		require.NoError(err)
		defer iter.Close()

		found := iter.Next()

		if mustExist {
			require.NotNil(found, "expected tuple %s", tuple.MustString(tpl))
		} else {
			require.Nil(found, "expected tuple %s to not exist", tuple.MustString(tpl))
		}

		iter.Close()

		if mustExist {
			require.Equal(tuple.MustString(tpl), tuple.MustString(found))
		}
	}
}
