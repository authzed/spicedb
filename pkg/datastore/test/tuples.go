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
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	testUserNamespace     = "test/user"
	testResourceNamespace = "test/resource"
	testGroupNamespace    = "test/group"
	testReaderRelation    = "reader"
	testEditorRelation    = "editor"
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
					OptionalResourceType: tupleToFind.ResourceAndRelation.Namespace,
					OptionalResourceIds:  []string{tupleToFind.ResourceAndRelation.ObjectId},
				})
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				// Check without a resource type.
				iter, err = dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
					OptionalResourceIds: []string{tupleToFind.ResourceAndRelation.ObjectId},
				})
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, tupleToFind)

				iter, err = dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
					OptionalResourceType:     tupleToFind.ResourceAndRelation.Namespace,
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
					OptionalResourceType:     tupleToFind.ResourceAndRelation.Namespace,
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
				OptionalResourceType: testResourceNamespace,
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testTuples...)

			// Filter it down to a single tuple with a userset
			iter, err = dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
				OptionalResourceType: testResourceNamespace,
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
				OptionalResourceType: testTuples[0].ResourceAndRelation.Namespace,
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testTuples...)

			iter, err = dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
				OptionalResourceType:     testTuples[0].ResourceAndRelation.Namespace,
				OptionalResourceRelation: testTuples[0].ResourceAndRelation.Relation,
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testTuples...)

			// Try some bad queries
			iter, err = dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
				OptionalResourceType: testTuples[0].ResourceAndRelation.Namespace,
				OptionalResourceIds:  []string{"fakeobectid"},
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
					OptionalResourceType: testTuples[0].ResourceAndRelation.Namespace,
				},
			)
			require.NoError(err)
			tRequire.VerifyIteratorResults(alreadyDeletedIter, testTuples[1:]...)

			// Write it back
			returnedAt, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, testTuples[0])
			require.NoError(err)
			tRequire.TupleExists(ctx, testTuples[0], returnedAt)

			// Delete with DeleteRelationship
			deletedAt, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				_, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
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
			_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
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
				OptionalResourceType: testResourceNamespace,
				OptionalResourceIds:  []string{tc},
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
			"only resourceID",
			testTuples,
			&v1.RelationshipFilter{
				OptionalResourceId: "resource0",
			},
			testTuples[1:],
			testTuples[:1],
		},
		{
			"only relation",
			testTuples,
			&v1.RelationshipFilter{
				OptionalRelation: "writer",
			},
			testTuples[:len(testTuples)-1],
			[]*core.RelationTuple{testTuples[len(testTuples)-1]},
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
			"subjectID without resource type",
			testTuples,
			&v1.RelationshipFilter{
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

			toTouch := make([]*core.RelationTupleUpdate, 0, len(tt.inputTuples))
			for _, tpl := range tt.inputTuples {
				toTouch = append(toTouch, tuple.Touch(tpl))
			}

			_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				return rwt.WriteRelationships(ctx, toTouch)
			})
			require.NoError(err)

			deletedAt, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				_, err := rwt.DeleteRelationships(ctx, tt.filter)
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

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
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

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		// Write the relationship.
		return rwt.WriteRelationships(ctx, []*core.RelationTupleUpdate{
			tuple.Create(tuple.MustParse("document:foo#viewer@user:tom#...")),
		})
	})
	require.NoError(err)

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		// Delete the relationship.
		return rwt.WriteRelationships(ctx, []*core.RelationTupleUpdate{
			tuple.Delete(tuple.MustParse("document:foo#viewer@user:tom#...")),
		})
	})
	require.NoError(err)

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
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

	f := func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		_, err := rwt.BulkLoad(ctx, testfixtures.NewBulkTupleGenerator(testResourceNamespace, testReaderRelation, testUserNamespace, 1, t))
		return err
	}
	_, _ = ds.ReadWriteTx(ctx, f)
	_, err = ds.ReadWriteTx(ctx, f)
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

// DeleteOneThousandIndividualInOneCallTest tests deleting 1000 relationships, individually.
func DeleteOneThousandIndividualInOneCallTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	// Write the 1000 relationships.
	tuples := make([]*core.RelationTuple, 0, 1000)
	for i := 0; i < 1000; i++ {
		tpl := makeTestTuple("foo", fmt.Sprintf("user%d", i))
		tuples = append(tuples, tpl)
	}

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, tuples...)
	require.NoError(err)
	ensureTuples(ctx, require, ds, tuples...)

	// Add an extra tuple.
	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, makeTestTuple("foo", "extra"))
	require.NoError(err)
	ensureTuples(ctx, require, ds, makeTestTuple("foo", "extra"))

	// Delete the first 1000 tuples.
	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_DELETE, tuples...)
	require.NoError(err)
	ensureNotTuples(ctx, require, ds, tuples...)

	// Ensure the extra tuple is still present.
	ensureTuples(ctx, require, ds, makeTestTuple("foo", "extra"))
}

// DeleteWithLimitTest tests deleting relationships with a limit.
func DeleteWithLimitTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)
	ctx := context.Background()

	// Write the 1000 relationships.
	tuples := make([]*core.RelationTuple, 0, 1000)
	for i := 0; i < 1000; i++ {
		tpl := makeTestTuple("foo", fmt.Sprintf("user%d", i))
		tuples = append(tuples, tpl)
	}

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, tuples...)
	require.NoError(err)
	ensureTuples(ctx, require, ds, tuples...)

	// Delete 100 tuples.
	var deleteLimit uint64 = 100
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		limitReached, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
			ResourceType: testResourceNamespace,
		}, options.WithDeleteLimit(&deleteLimit))
		require.NoError(err)
		require.True(limitReached)
		return nil
	})
	require.NoError(err)

	// Ensure 900 tuples remain.
	found := countTuples(ctx, require, ds, testResourceNamespace)
	require.Equal(900, found)

	// Delete the remainder.
	deleteLimit = 1000
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		limitReached, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
			ResourceType: testResourceNamespace,
		}, options.WithDeleteLimit(&deleteLimit))
		require.NoError(err)
		require.False(limitReached)
		return nil
	})
	require.NoError(err)

	found = countTuples(ctx, require, ds, testResourceNamespace)
	require.Equal(0, found)
}

// DeleteCaveatedTupleTest tests deleting a relationship with a caveat.
func DeleteCaveatedTupleTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	tpl := tuple.Parse("test/resource:someresource#viewer@test/user:someuser[somecaveat]")

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, tpl)
	require.NoError(err)
	ensureTuples(ctx, require, ds, tpl)

	// Delete the tuple.
	withoutCaveat := tuple.Parse("test/resource:someresource#viewer@test/user:someuser")

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_DELETE, withoutCaveat)
	require.NoError(err)
	ensureNotTuples(ctx, require, ds, tpl, withoutCaveat)
}

// DeleteRelationshipsWithVariousFiltersTest tests deleting relationships with various filters.
func DeleteRelationshipsWithVariousFiltersTest(t *testing.T, tester DatastoreTester) {
	tcs := []struct {
		name            string
		filter          *v1.RelationshipFilter
		relationships   []string
		expectedDeleted []string
	}{
		{
			name: "resource type",
			filter: &v1.RelationshipFilter{
				ResourceType: "document",
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:second#viewer@user:tom",
				"folder:secondfolder#viewer@user:tom",
				"folder:someotherfolder#viewer@user:tom",
			},
			expectedDeleted: []string{"document:first#viewer@user:tom", "document:second#viewer@user:tom"},
		},
		{
			name: "resource id",
			filter: &v1.RelationshipFilter{
				ResourceType:       "document",
				OptionalResourceId: "first",
			},
			relationships:   []string{"document:first#viewer@user:tom", "document:second#viewer@user:tom"},
			expectedDeleted: []string{"document:first#viewer@user:tom"},
		},
		{
			name: "resource id without resource type",
			filter: &v1.RelationshipFilter{
				OptionalResourceId: "first",
			},
			relationships:   []string{"document:first#viewer@user:tom", "document:second#viewer@user:tom"},
			expectedDeleted: []string{"document:first#viewer@user:tom"},
		},
		{
			name: "resource id prefix with resource type",
			filter: &v1.RelationshipFilter{
				ResourceType:             "document",
				OptionalResourceIdPrefix: "f",
			},
			relationships:   []string{"document:first#viewer@user:tom", "document:second#viewer@user:tom", "document:fourth#viewer@user:tom", "folder:fsomething#viewer@user:tom"},
			expectedDeleted: []string{"document:first#viewer@user:tom", "document:fourth#viewer@user:tom"},
		},
		{
			name: "resource id prefix without resource type",
			filter: &v1.RelationshipFilter{
				OptionalResourceIdPrefix: "f",
			},
			relationships:   []string{"document:first#viewer@user:tom", "document:second#viewer@user:tom", "document:fourth#viewer@user:tom", "folder:fsomething#viewer@user:tom"},
			expectedDeleted: []string{"document:first#viewer@user:tom", "document:fourth#viewer@user:tom", "folder:fsomething#viewer@user:tom"},
		},
		{
			name: "resource relation",
			filter: &v1.RelationshipFilter{
				OptionalRelation: "viewer",
			},
			relationships:   []string{"document:first#viewer@user:tom", "document:second#viewer@user:tom", "document:third#editor@user:tom", "folder:fsomething#viewer@user:tom"},
			expectedDeleted: []string{"document:first#viewer@user:tom", "document:second#viewer@user:tom", "folder:fsomething#viewer@user:tom"},
		},
		{
			name: "subject id",
			filter: &v1.RelationshipFilter{
				OptionalSubjectFilter: &v1.SubjectFilter{SubjectType: "user", OptionalSubjectId: "tom"},
			},
			relationships:   []string{"document:first#viewer@user:tom", "document:second#viewer@user:tom", "document:third#editor@user:tom", "document:first#viewer@user:alice"},
			expectedDeleted: []string{"document:first#viewer@user:tom", "document:second#viewer@user:tom", "document:third#editor@user:tom"},
		},
		{
			name: "subject filter with relation",
			filter: &v1.RelationshipFilter{
				OptionalSubjectFilter: &v1.SubjectFilter{SubjectType: "user", OptionalRelation: &v1.SubjectFilter_RelationFilter{Relation: "something"}},
			},
			relationships:   []string{"document:first#viewer@user:tom", "document:second#viewer@user:tom#something"},
			expectedDeleted: []string{"document:second#viewer@user:tom#something"},
		},
		{
			name: "full match",
			filter: &v1.RelationshipFilter{
				ResourceType:          "document",
				OptionalResourceId:    "first",
				OptionalRelation:      "viewer",
				OptionalSubjectFilter: &v1.SubjectFilter{SubjectType: "user", OptionalSubjectId: "tom"},
			},
			relationships:   []string{"document:first#viewer@user:tom", "document:second#viewer@user:tom", "document:first#editor@user:tom", "document:firster#viewer@user:tom"},
			expectedDeleted: []string{"document:first#viewer@user:tom"},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, withLimit := range []bool{false, true} {
				t.Run(fmt.Sprintf("withLimit=%v", withLimit), func(t *testing.T) {
					require := require.New(t)

					rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
					require.NoError(err)

					// Write the initial relationships.
					ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)
					ctx := context.Background()

					allRelationships := mapz.NewSet[string]()
					for _, rel := range tc.relationships {
						allRelationships.Add(rel)

						tpl := tuple.MustParse(rel)
						_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, tpl)
						require.NoError(err)
					}

					writtenRev, err := ds.HeadRevision(ctx)
					require.NoError(err)

					var delLimit *uint64
					if withLimit {
						limit := uint64(len(tc.expectedDeleted))
						delLimit = &limit
					}

					// Delete the relationships and ensure matching are no longer found.
					_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
						_, err := rwt.DeleteRelationships(ctx, tc.filter, options.WithDeleteLimit(delLimit))
						return err
					})
					require.NoError(err)

					// Read the updated relationships and ensure no matching relationships are found.
					headRev, err := ds.HeadRevision(ctx)
					require.NoError(err)

					filter, err := datastore.RelationshipsFilterFromPublicFilter(tc.filter)
					require.NoError(err)

					reader := ds.SnapshotReader(headRev)
					iter, err := reader.QueryRelationships(ctx, filter)
					require.NoError(err)
					t.Cleanup(iter.Close)

					found := iter.Next()
					if found != nil {
						require.Nil(found, "got relationship: %s", tuple.MustString(found))
					}
					iter.Close()

					// Ensure the expected relationships were deleted.
					resourceTypes := mapz.NewSet[string]()
					for _, rel := range tc.relationships {
						tpl := tuple.MustParse(rel)
						resourceTypes.Add(tpl.ResourceAndRelation.Namespace)
					}

					allRemainingRelationships := mapz.NewSet[string]()
					for _, resourceType := range resourceTypes.AsSlice() {
						iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
							OptionalResourceType: resourceType,
						})
						require.NoError(err)
						t.Cleanup(iter.Close)

						for {
							rel := iter.Next()
							if rel == nil {
								break
							}
							allRemainingRelationships.Add(tuple.MustString(rel))
						}
						iter.Close()
					}

					deletedRelationships := allRelationships.Subtract(allRemainingRelationships).AsSlice()
					require.ElementsMatch(tc.expectedDeleted, deletedRelationships)

					// Ensure the initial relationships are still present at the previous revision.
					allInitialRelationships := mapz.NewSet[string]()
					olderReader := ds.SnapshotReader(writtenRev)
					for _, resourceType := range resourceTypes.AsSlice() {
						iter, err := olderReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
							OptionalResourceType: resourceType,
						})
						require.NoError(err)
						t.Cleanup(iter.Close)

						for {
							rel := iter.Next()
							if rel == nil {
								break
							}
							allInitialRelationships.Add(tuple.MustString(rel))
						}
						iter.Close()
					}

					require.ElementsMatch(tc.relationships, allInitialRelationships.AsSlice())
				})
			}
		})
	}
}

func RecreateRelationshipsAfterDeleteWithFilter(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)
	ctx := context.Background()

	relationships := make([]*core.RelationTuple, 100)
	for i := 0; i < 100; i++ {
		relationships[i] = tuple.MustParse(fmt.Sprintf("document:%d#owner@user:first", i))
	}

	writeRelationships := func() error {
		_, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, relationships...)
		return err
	}

	deleteRelationships := func() error {
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			delLimit := uint64(100)
			_, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
				OptionalRelation: "owner",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:       "user",
					OptionalSubjectId: "first",
				},
			}, options.WithDeleteLimit(&delLimit))
			return err
		})
		return err
	}

	// Write the initial relationships.
	require.NoError(writeRelationships())

	// Delete the relationships.
	require.NoError(deleteRelationships())

	// Write the same relationships again.
	require.NoError(writeRelationships())
}

// QueryRelationshipsWithVariousFiltersTest tests various relationship filters for query relationships.
func QueryRelationshipsWithVariousFiltersTest(t *testing.T, tester DatastoreTester) {
	tcs := []struct {
		name          string
		filter        datastore.RelationshipsFilter
		relationships []string
		expected      []string
	}{
		{
			name: "resource type",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:second#viewer@user:tom",
				"folder:secondfolder#viewer@user:tom",
				"folder:someotherfolder#viewer@user:tom",
			},
			expected: []string{"document:first#viewer@user:tom", "document:second#viewer@user:tom"},
		},
		{
			name: "resource id",
			filter: datastore.RelationshipsFilter{
				OptionalResourceIds: []string{"first"},
			},
			relationships: []string{"document:first#viewer@user:tom", "document:second#viewer@user:tom"},
			expected:      []string{"document:first#viewer@user:tom"},
		},
		{
			name: "resource id prefix",
			filter: datastore.RelationshipsFilter{
				OptionalResourceIDPrefix: "first",
			},
			relationships: []string{"document:first#viewer@user:tom", "document:second#viewer@user:tom"},
			expected:      []string{"document:first#viewer@user:tom"},
		},
		{
			name: "resource id prefix with underscore",
			filter: datastore.RelationshipsFilter{
				OptionalResourceIDPrefix: "first_",
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:first_foo#viewer@user:tom",
				"document:first_bar#viewer@user:tom",
				"document:firstmeh#viewer@user:tom",
				"document:second#viewer@user:tom",
			},
			expected: []string{"document:first_foo#viewer@user:tom", "document:first_bar#viewer@user:tom"},
		},
		{
			name: "resource id prefix with multiple underscores",
			filter: datastore.RelationshipsFilter{
				OptionalResourceIDPrefix: "first_f_",
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:first_f_oo#viewer@user:tom",
				"document:first_bar#viewer@user:tom",
				"document:firstmeh#viewer@user:tom",
				"document:second#viewer@user:tom",
			},
			expected: []string{"document:first_f_oo#viewer@user:tom"},
		},
		{
			name: "resource id different prefix",
			filter: datastore.RelationshipsFilter{
				OptionalResourceIDPrefix: "s",
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:second#viewer@user:tom",
				"folder:secondfolder#viewer@user:tom",
				"folder:someotherfolder#viewer@user:tom",
			},
			expected: []string{
				"document:second#viewer@user:tom",
				"folder:secondfolder#viewer@user:tom",
				"folder:someotherfolder#viewer@user:tom",
			},
		},
		{
			name: "resource id different longer prefix",
			filter: datastore.RelationshipsFilter{
				OptionalResourceIDPrefix: "se",
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:second#viewer@user:tom",
				"folder:secondfolder#viewer@user:tom",
				"folder:someotherfolder#viewer@user:tom",
			},
			expected: []string{
				"document:second#viewer@user:tom",
				"folder:secondfolder#viewer@user:tom",
			},
		},
		{
			name: "resource type and resource id different prefix",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "folder",
				OptionalResourceIDPrefix: "s",
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:second#viewer@user:tom",
				"folder:secondfolder#viewer@user:tom",
				"folder:someotherfolder#viewer@user:tom",
			},
			expected: []string{
				"folder:secondfolder#viewer@user:tom",
				"folder:someotherfolder#viewer@user:tom",
			},
		},
		{
			name: "full resource",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "folder",
				OptionalResourceIDPrefix: "s",
				OptionalResourceRelation: "viewer",
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:second#viewer@user:tom",
				"folder:secondfolder#viewer@user:tom",
				"folder:someotherfolder#viewer@user:tom",
			},
			expected: []string{
				"folder:secondfolder#viewer@user:tom",
				"folder:someotherfolder#viewer@user:tom",
			},
		},
		{
			name: "full resource mismatch",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "folder",
				OptionalResourceIDPrefix: "s",
				OptionalResourceRelation: "someotherelation",
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:second#viewer@user:tom",
				"folder:secondfolder#viewer@user:tom",
				"folder:someotherfolder#viewer@user:tom",
			},
			expected: []string{},
		},
		{
			name: "subject type",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
					},
				},
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:second#viewer@anotheruser:tom",
				"folder:secondfolder#viewer@anotheruser:tom",
				"folder:someotherfolder#viewer@user:tom",
			},
			expected: []string{
				"document:first#viewer@user:tom",
				"folder:someotherfolder#viewer@user:tom",
			},
		},
		{
			name: "subject ids",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectIds: []string{"tom"},
					},
				},
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:second#viewer@anotheruser:fred",
				"folder:secondfolder#viewer@anotheruser:fred",
				"folder:someotherfolder#viewer@user:tom",
			},
			expected: []string{
				"document:first#viewer@user:tom",
				"folder:someotherfolder#viewer@user:tom",
			},
		},
		{
			name: "multiple subject ids",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectIds: []string{"tom", "fred"},
					},
				},
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:second#viewer@anotheruser:fred",
				"folder:secondfolder#viewer@anotheruser:sarah",
				"folder:someotherfolder#viewer@user:tom",
			},
			expected: []string{
				"document:first#viewer@user:tom",
				"document:second#viewer@anotheruser:fred",
				"folder:someotherfolder#viewer@user:tom",
			},
		},
		{
			name: "non ellipsis subject relation",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						RelationFilter: datastore.SubjectRelationFilter{
							NonEllipsisRelation: "something",
						},
					},
				},
			},
			relationships: []string{
				"document:first#viewer@user:tom#...",
				"document:second#viewer@anotheruser:fred#something",
				"folder:secondfolder#viewer@anotheruser:sarah",
				"folder:someotherfolder#viewer@user:tom",
			},
			expected: []string{
				"document:second#viewer@anotheruser:fred#something",
			},
		},
		{
			name: "specific subject relation and ellipsis",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						RelationFilter: datastore.SubjectRelationFilter{
							NonEllipsisRelation:     "something",
							IncludeEllipsisRelation: true,
						},
					},
				},
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:second#viewer@anotheruser:fred#something",
				"folder:secondfolder#viewer@anotheruser:sarah",
				"folder:someotherfolder#viewer@user:tom",
			},
			expected: []string{
				"document:first#viewer@user:tom",
				"document:second#viewer@anotheruser:fred#something",
				"folder:secondfolder#viewer@anotheruser:sarah",
				"folder:someotherfolder#viewer@user:tom",
			},
		},
		{
			name: "specific subject relation and no non-ellipsis",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						RelationFilter: datastore.SubjectRelationFilter{
							NonEllipsisRelation:      "something",
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:second#viewer@anotheruser:fred#something",
				"folder:secondfolder#viewer@anotheruser:sarah",
				"folder:someotherfolder#viewer@user:tom",
			},
			expected: []string{
				"document:second#viewer@anotheruser:fred#something",
			},
		},
		{
			name: "only ellipsis",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						RelationFilter: datastore.SubjectRelationFilter{
							IncludeEllipsisRelation: true,
						},
					},
				},
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:second#viewer@anotheruser:fred#something",
				"folder:secondfolder#viewer@anotheruser:sarah",
				"folder:someotherfolder#viewer@user:tom",
			},
			expected: []string{
				"document:first#viewer@user:tom",
				"folder:secondfolder#viewer@anotheruser:sarah",
				"folder:someotherfolder#viewer@user:tom",
			},
		},
		{
			name: "multiple subject filters",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						RelationFilter: datastore.SubjectRelationFilter{
							NonEllipsisRelation: "something",
						},
					},
					{
						OptionalSubjectIds: []string{"tom"},
					},
				},
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:second#viewer@anotheruser:fred#something",
				"folder:secondfolder#viewer@anotheruser:sarah",
				"folder:someotherfolder#viewer@user:tom",
			},
			expected: []string{
				"document:first#viewer@user:tom",
				"document:second#viewer@anotheruser:fred#something",
				"folder:someotherfolder#viewer@user:tom",
			},
		},
		{
			name: "multiple subject filters and resource ID prefix",
			filter: datastore.RelationshipsFilter{
				OptionalResourceIDPrefix: "s",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						RelationFilter: datastore.SubjectRelationFilter{
							NonEllipsisRelation: "something",
						},
					},
					{
						OptionalSubjectIds: []string{"tom"},
					},
				},
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:second#viewer@anotheruser:fred#something",
				"folder:secondfolder#viewer@anotheruser:sarah",
				"folder:someotherfolder#viewer@user:tom",
			},
			expected: []string{
				"document:second#viewer@anotheruser:fred#something",
				"folder:someotherfolder#viewer@user:tom",
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
			require.NoError(err)

			ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)
			ctx := context.Background()

			for _, rel := range tc.relationships {
				tpl := tuple.MustParse(rel)
				_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, tpl)
				require.NoError(err)
			}

			headRev, err := ds.HeadRevision(ctx)
			require.NoError(err)

			reader := ds.SnapshotReader(headRev)
			iter, err := reader.QueryRelationships(ctx, tc.filter)
			require.NoError(err)

			var results []string
			for {
				tpl := iter.Next()
				if tpl == nil {
					err := iter.Err()
					require.NoError(err)
					break
				}

				results = append(results, tuple.MustString(tpl))
			}
			iter.Close()

			require.ElementsMatch(tc.expected, results)
		})
	}
}

// TypedTouchAlreadyExistingTest tests touching a relationship twice, when valid type information is provided.
func TypedTouchAlreadyExistingTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	tpl1 := tuple.Parse("document:foo#viewer@user:tom")

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, tpl1)
	require.NoError(err)
	ensureTuples(ctx, require, ds, tpl1)

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, tpl1)
	require.NoError(err)
	ensureTuples(ctx, require, ds, tpl1)
}

// TypedTouchAlreadyExistingWithCaveatTest tests touching a relationship twice, when valid type information is provided.
func TypedTouchAlreadyExistingWithCaveatTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	ctpl1 := tuple.Parse("document:foo#caveated_viewer@user:tom[test:{\"foo\":\"bar\"}]")

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, ctpl1)
	require.NoError(err)
	ensureTuples(ctx, require, ds, ctpl1)

	ctpl1Updated := tuple.Parse("document:foo#caveated_viewer@user:tom[test:{\"foo\":\"baz\"}]")

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, ctpl1Updated)
	require.NoError(err)
	ensureTuples(ctx, require, ds, ctpl1Updated)
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

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		it, err := rwt.QueryRelationships(ctx, datastore.RelationshipsFilter{
			OptionalResourceType: "document",
		})
		require.NoError(err)
		it.Close()

		it, err = rwt.QueryRelationships(ctx, datastore.RelationshipsFilter{
			OptionalResourceType: "folder",
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
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			iter, err := rwt.QueryRelationships(ctx, datastore.RelationshipsFilter{
				OptionalResourceType: testResourceNamespace,
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

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
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

func BulkDeleteRelationshipsTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)
	ctx := context.Background()

	// Write a bunch of relationships.
	t.Log(time.Now(), "starting write")
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		_, err := rwt.BulkLoad(ctx, testfixtures.NewBulkTupleGenerator(testResourceNamespace, testReaderRelation, testUserNamespace, 1000, t))
		if err != nil {
			return err
		}

		_, err = rwt.BulkLoad(ctx, testfixtures.NewBulkTupleGenerator(testResourceNamespace, testEditorRelation, testUserNamespace, 1000, t))
		if err != nil {
			return err
		}

		return nil
	})
	require.NoError(err)

	// Issue a deletion for the first set of relationships.
	t.Log(time.Now(), "starting delete")
	deleteCount := 0
	deletedRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		t.Log(time.Now(), "deleting")
		deleteCount++
		_, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
			ResourceType:     testResourceNamespace,
			OptionalRelation: testReaderRelation,
		})
		return err
	})
	require.NoError(err)
	require.Equal(1, deleteCount)
	t.Log(time.Now(), "finished delete")

	// Ensure the relationships were removed.
	t.Log(time.Now(), "starting check")
	reader := ds.SnapshotReader(deletedRev)
	iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType:     testResourceNamespace,
		OptionalResourceRelation: testReaderRelation,
	})
	require.NoError(err)
	defer iter.Close()

	require.Nil(iter.Next(), "expected no results")
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
			OptionalResourceType:     tpl.ResourceAndRelation.Namespace,
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

func countTuples(ctx context.Context, require *require.Assertions, ds datastore.Datastore, resourceType string) int {
	headRev, err := ds.HeadRevision(ctx)
	require.NoError(err)

	reader := ds.SnapshotReader(headRev)

	iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: resourceType,
	})
	require.NoError(err)
	defer iter.Close()

	counter := 0
	for {
		rel := iter.Next()
		if rel == nil {
			break
		}

		counter++
	}

	return counter
}
