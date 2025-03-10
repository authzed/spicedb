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
	"github.com/ccoveille/go-safecast"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
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

	for _, numRels := range testCases {
		numRels := numRels
		t.Run(strconv.Itoa(numRels), func(t *testing.T) {
			require := require.New(t)

			ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
			require.NoError(err)
			defer ds.Close()

			ctx := context.Background()

			ok, err := ds.ReadyState(ctx)
			require.NoError(err)
			require.True(ok.IsReady)

			setupDatastore(ds, require)

			tRequire := testfixtures.RelationshipChecker{Require: require, DS: ds}

			var testRels []tuple.Relationship
			for i := 0; i < numRels; i++ {
				resourceName := fmt.Sprintf("resource%d", i)
				userName := fmt.Sprintf("user%d", i)

				newRel := makeTestRel(resourceName, userName)
				testRels = append(testRels, newRel)
			}

			lastRevision, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, testRels...)
			require.NoError(err)

			for _, toCheck := range testRels {
				tRequire.RelationshipExists(ctx, toCheck, lastRevision)
			}

			// Write a duplicate relationship to make sure the datastore rejects it
			_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, testRels...)
			require.Error(err)

			dsReader := ds.SnapshotReader(lastRevision)
			for _, relToFind := range testRels {
				relSubject := relToFind.Subject

				// Check that we can find the relationship a number of ways
				iter, err := dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
					OptionalResourceType: relToFind.Resource.ObjectType,
					OptionalResourceIds:  []string{relToFind.Resource.ObjectID},
				})
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, relToFind)

				// Check without a resource type.
				iter, err = dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
					OptionalResourceIds: []string{relToFind.Resource.ObjectID},
				})
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, relToFind)

				iter, err = dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
					OptionalResourceType:     relToFind.Resource.ObjectType,
					OptionalResourceIds:      []string{relToFind.Resource.ObjectID},
					OptionalResourceRelation: relToFind.Resource.Relation,
				})
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, relToFind)

				iter, err = dsReader.ReverseQueryRelationships(
					ctx,
					onrToSubjectsFilter(relSubject),
					options.WithResRelation(&options.ResourceRelation{
						Namespace: relToFind.Resource.ObjectType,
						Relation:  relToFind.Resource.Relation,
					}),
				)
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, relToFind)

				iter, err = dsReader.ReverseQueryRelationships(
					ctx,
					onrToSubjectsFilter(relSubject),
					options.WithResRelation(&options.ResourceRelation{
						Namespace: relToFind.Resource.ObjectType,
						Relation:  relToFind.Resource.Relation,
					}),
					options.WithLimitForReverse(options.LimitOne),
				)
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter, relToFind)

				// Check that we fail to find the relationship with the wrong filters
				iter, err = dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
					OptionalResourceType:     relToFind.Resource.ObjectType,
					OptionalResourceIds:      []string{relToFind.Resource.ObjectID},
					OptionalResourceRelation: "fake",
				})
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter)

				incorrectUserset := relSubject.WithRelation("fake")

				iter, err = dsReader.ReverseQueryRelationships(
					ctx,
					onrToSubjectsFilter(incorrectUserset),
					options.WithResRelation(&options.ResourceRelation{
						Namespace: relToFind.Resource.ObjectType,
						Relation:  relToFind.Resource.Relation,
					}),
				)
				require.NoError(err)
				tRequire.VerifyIteratorResults(iter)
			}

			// Check a query that returns a number of relationships
			iter, err := dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
				OptionalResourceType: testResourceNamespace,
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testRels...)

			// Filter it down to a single relationship with a userset
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
			tRequire.VerifyIteratorResults(iter, testRels[0])

			// Check for larger reverse queries.
			iter, err = dsReader.ReverseQueryRelationships(ctx, datastore.SubjectsFilter{
				SubjectType: testUserNamespace,
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testRels...)

			// Check limit.
			if len(testRels) > 1 {
				limit, _ := safecast.ToUint64(len(testRels) - 1)
				iter, err := dsReader.ReverseQueryRelationships(ctx, datastore.SubjectsFilter{
					SubjectType: testUserNamespace,
				}, options.WithLimitForReverse(&limit))
				require.NoError(err)

				tRequire.VerifyIteratorCount(iter, len(testRels)-1)
			}

			// Check that we can find the group of relationships too
			iter, err = dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
				OptionalResourceType: testRels[0].Resource.ObjectType,
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testRels...)

			iter, err = dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
				OptionalResourceType:     testRels[0].Resource.ObjectType,
				OptionalResourceRelation: testRels[0].Resource.Relation,
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter, testRels...)

			// Try some bad queries
			iter, err = dsReader.QueryRelationships(ctx, datastore.RelationshipsFilter{
				OptionalResourceType: testRels[0].Resource.ObjectType,
				OptionalResourceIds:  []string{"fakeobectid"},
			})
			require.NoError(err)
			tRequire.VerifyIteratorResults(iter)

			// Delete the first relationship.
			deletedAt, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationDelete, testRels[0])
			require.NoError(err)

			// Delete it AGAIN (idempotent delete) and make sure there's no error
			_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationDelete, testRels[0])
			require.NoError(err)

			// Verify it can still be read at the old revision
			tRequire.RelationshipExists(ctx, testRels[0], lastRevision)

			// Verify that it does not show up at the new revision
			tRequire.NoRelationshipExists(ctx, testRels[0], deletedAt)
			alreadyDeletedIter, err := ds.SnapshotReader(deletedAt).QueryRelationships(
				ctx,
				datastore.RelationshipsFilter{
					OptionalResourceType: testRels[0].Resource.ObjectType,
				},
			)
			require.NoError(err)
			tRequire.VerifyIteratorResults(alreadyDeletedIter, testRels[1:]...)

			// Write it back
			returnedAt, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, testRels[0])
			require.NoError(err)
			tRequire.RelationshipExists(ctx, testRels[0], returnedAt)

			// Delete with DeleteRelationship
			deletedAt, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				_, _, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
					ResourceType: testResourceNamespace,
				})
				require.NoError(err)
				return err
			})
			require.NoError(err)
			tRequire.NoRelationshipExists(ctx, testRels[0], deletedAt)
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

			rel := makeTestRel(tc, tc)

			// Write the test relationship
			_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
					{
						Operation:    tuple.UpdateOperationCreate,
						Relationship: rel,
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

			found, err := datastore.IteratorToSlice(iter)
			require.NoError(err)
			require.Len(found, 1)

			first := found[0]
			require.NotNil(first)
			require.Equal(tc, first.Resource.ObjectID)
			require.Equal(tc, first.Subject.ObjectID)
		})
	}
}

// DeleteRelationshipsTest tests whether or not the requirements for deleting
// relationships hold for a particular datastore.
func DeleteRelationshipsTest(t *testing.T, tester DatastoreTester) {
	var testRels []tuple.Relationship
	for i := 0; i < 10; i++ {
		newRel := makeTestRel(fmt.Sprintf("resource%d", i), fmt.Sprintf("user%d", i%2))
		testRels = append(testRels, newRel)
	}
	testRels[len(testRels)-1].Resource.Relation = "writer"

	table := []struct {
		name                    string
		inputRels               []tuple.Relationship
		filter                  *v1.RelationshipFilter
		expectedExistingRels    []tuple.Relationship
		expectedNonExistingRels []tuple.Relationship
	}{
		{
			"resourceID",
			testRels,
			&v1.RelationshipFilter{
				ResourceType:       testResourceNamespace,
				OptionalResourceId: "resource0",
			},
			testRels[1:],
			testRels[:1],
		},
		{
			"only resourceID",
			testRels,
			&v1.RelationshipFilter{
				OptionalResourceId: "resource0",
			},
			testRels[1:],
			testRels[:1],
		},
		{
			"only relation",
			testRels,
			&v1.RelationshipFilter{
				OptionalRelation: "writer",
			},
			testRels[:len(testRels)-1],
			[]tuple.Relationship{testRels[len(testRels)-1]},
		},
		{
			"relation",
			testRels,
			&v1.RelationshipFilter{
				ResourceType:     testResourceNamespace,
				OptionalRelation: "writer",
			},
			testRels[:len(testRels)-1],
			[]tuple.Relationship{testRels[len(testRels)-1]},
		},
		{
			"subjectID",
			testRels,
			&v1.RelationshipFilter{
				ResourceType:          testResourceNamespace,
				OptionalSubjectFilter: &v1.SubjectFilter{SubjectType: testUserNamespace, OptionalSubjectId: "user0"},
			},
			[]tuple.Relationship{testRels[1], testRels[3], testRels[5], testRels[7], testRels[9]},
			[]tuple.Relationship{testRels[0], testRels[2], testRels[4], testRels[6], testRels[8]},
		},
		{
			"subjectID without resource type",
			testRels,
			&v1.RelationshipFilter{
				OptionalSubjectFilter: &v1.SubjectFilter{SubjectType: testUserNamespace, OptionalSubjectId: "user0"},
			},
			[]tuple.Relationship{testRels[1], testRels[3], testRels[5], testRels[7], testRels[9]},
			[]tuple.Relationship{testRels[0], testRels[2], testRels[4], testRels[6], testRels[8]},
		},
		{
			"subjectRelation",
			testRels,
			&v1.RelationshipFilter{
				ResourceType:          testResourceNamespace,
				OptionalSubjectFilter: &v1.SubjectFilter{SubjectType: testUserNamespace, OptionalRelation: &v1.SubjectFilter_RelationFilter{Relation: ""}},
			},
			nil,
			testRels,
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

			tRequire := testfixtures.RelationshipChecker{Require: require, DS: ds}

			toTouch := make([]tuple.RelationshipUpdate, 0, len(tt.inputRels))
			for _, tpl := range tt.inputRels {
				toTouch = append(toTouch, tuple.Touch(tpl))
			}

			_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				return rwt.WriteRelationships(ctx, toTouch)
			})
			require.NoError(err)

			deletedAt, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				_, _, err := rwt.DeleteRelationships(ctx, tt.filter)
				require.NoError(err)
				return err
			})
			require.NoError(err)

			for _, tpl := range tt.expectedExistingRels {
				tRequire.RelationshipExists(ctx, tpl, deletedAt)
			}

			for _, tpl := range tt.expectedNonExistingRels {
				tRequire.NoRelationshipExists(ctx, tpl, deletedAt)
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

		revisionErr := datastore.InvalidRevisionError{}
		require.True(errors.As(err, &revisionErr))

		newRel := makeTestRel("one", "one")
		firstWrite, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, newRel)
		require.NoError(err)

		// Check that we can read at the just written revision
		err = ds.CheckRevision(ctx, firstWrite)
		require.NoError(err)

		// Wait the duration required to allow the revision to expire
		time.Sleep(testGCDuration * 2)

		// Write another relationship which will allow the first revision to expire
		nextWrite, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, newRel)
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
		err := rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
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
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("document:foo#viewer@user:tom#...")),
		})
	})
	require.NoError(err)

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		// Delete the relationship.
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Delete(tuple.MustParse("document:foo#viewer@user:tom#...")),
		})
	})
	require.NoError(err)

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		// Delete the relationship again.
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
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

	tpl := makeTestRel("foo", "tom")
	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, tpl)
	require.NoError(err)

	ensureRelationships(ctx, require, ds, tpl)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationDelete, tpl)
	require.NoError(err)

	ensureNotRelationships(ctx, require, ds, tpl)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, tpl)
	require.NoError(err)

	ensureRelationships(ctx, require, ds, tpl)
}

// CreateAlreadyExistingTest tests creating a relationship twice.
func CreateAlreadyExistingTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	tpl1 := makeTestRel("foo", "tom")
	tpl2 := makeTestRel("foo", "sarah")
	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, tpl1, tpl2)
	require.NoError(err)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, tpl1)
	require.ErrorAs(err, &common.CreateRelationshipExistsError{})
	require.Contains(err.Error(), "could not CREATE relationship ")
	grpcutil.RequireStatus(t, codes.AlreadyExists, err)

	f := func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		_, err := rwt.BulkLoad(ctx, testfixtures.NewBulkRelationshipGenerator(testResourceNamespace, testReaderRelation, testUserNamespace, 1, t))
		return err
	}
	_, _ = ds.ReadWriteTx(ctx, f)
	_, err = ds.ReadWriteTx(ctx, f)
	require.Error(err)
	grpcutil.RequireStatus(t, codes.AlreadyExists, err)
}

// TouchAlreadyExistingTest tests touching a relationship twice.
func TouchAlreadyExistingTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	tpl1 := makeTestRel("foo", "tom")
	tpl2 := makeTestRel("foo", "sarah")

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, tpl1, tpl2)
	require.NoError(err)

	ensureRelationships(ctx, require, ds, tpl1, tpl2)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, tpl1, tpl2)
	require.NoError(err)

	ensureRelationships(ctx, require, ds, tpl1, tpl2)

	tpl3 := makeTestRel("foo", "fred")
	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, tpl1, tpl3)
	require.NoError(err)

	ensureRelationships(ctx, require, ds, tpl1, tpl2, tpl3)
}

// CreateDeleteTouchTest tests writing a relationship, deleting it, and then touching it.
func CreateDeleteTouchTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	tpl1 := makeTestRel("foo", "tom")
	tpl2 := makeTestRel("foo", "sarah")

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, tpl1, tpl2)
	require.NoError(err)

	ensureRelationships(ctx, require, ds, tpl1, tpl2)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationDelete, tpl1, tpl2)
	require.NoError(err)

	ensureNotRelationships(ctx, require, ds, tpl1, tpl2)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, tpl1, tpl2)
	require.NoError(err)

	ensureRelationships(ctx, require, ds, tpl1, tpl2)
}

// DeleteOneThousandIndividualInOneCallTest tests deleting 1000 relationships, individually.
func DeleteOneThousandIndividualInOneCallTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	// Write the 1000 relationships.
	relationships := make([]tuple.Relationship, 0, 1000)
	for i := 0; i < 1000; i++ {
		tpl := makeTestRel("foo", fmt.Sprintf("user%d", i))
		relationships = append(relationships, tpl)
	}

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, relationships...)
	require.NoError(err)
	ensureRelationships(ctx, require, ds, relationships...)

	// Add an extra relationship.
	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, makeTestRel("foo", "extra"))
	require.NoError(err)
	ensureRelationships(ctx, require, ds, makeTestRel("foo", "extra"))

	// Delete the first 1000 relationships.
	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationDelete, relationships...)
	require.NoError(err)
	ensureNotRelationships(ctx, require, ds, relationships...)

	// Ensure the extra relationship is still present.
	ensureRelationships(ctx, require, ds, makeTestRel("foo", "extra"))
}

// DeleteWithInvalidPrefixTest tests deleting relationships with an invalid object prefix.
func DeleteWithInvalidPrefixTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)
	ctx := context.Background()

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		_, _, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
			OptionalResourceIdPrefix: "hithere%",
		})
		return err
	})
	require.Error(err)
	require.ErrorContains(err, "value does not match regex")
}

// MixedWriteOperationsTest tests a WriteRelationships call with mixed operations.
func MixedWriteOperationsTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)
	ctx := context.Background()

	// Write the 100 relationships.
	rels := make([]tuple.Relationship, 0, 100)
	for i := 0; i < 100; i++ {
		rels = append(rels, tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR("document", "somedoc", "viewer"),
				Subject:  tuple.ONR("user", "user-"+strconv.Itoa(i), tuple.Ellipsis),
			},
		})
	}

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, rels...)
	require.NoError(err)
	ensureRelationships(ctx, require, ds, rels...)

	// Create a WriteRelationships call with a few CREATEs, TOUCHes and DELETEs.
	updates := make([]tuple.RelationshipUpdate, 0, 30)
	expectedRels := make([]tuple.Relationship, 0, 105)

	// Add a CREATE for 10 new relationships.
	for i := 0; i < 10; i++ {
		newRel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR("document", "somedoc", "viewer"),
				Subject:  tuple.ONR("user", "user-"+strconv.Itoa(i+100), tuple.Ellipsis),
			},
		}
		expectedRels = append(expectedRels, newRel)
		updates = append(updates, tuple.Create(newRel))
	}

	// Add a TOUCH for 5 existing relationships.
	for i := 0; i < 5; i++ {
		updates = append(updates, tuple.Touch(tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR("document", "somedoc", "viewer"),
				Subject:  tuple.ONR("user", "user-"+strconv.Itoa(i+50), tuple.Ellipsis),
			},
		}))
	}

	// Add a TOUCH for 5 new relationships.
	for i := 0; i < 5; i++ {
		newRel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR("document", "somedoc", "viewer"),
				Subject:  tuple.ONR("user", "user-"+strconv.Itoa(i+110), tuple.Ellipsis),
			},
		}
		expectedRels = append(expectedRels, newRel)
		updates = append(updates, tuple.Touch(newRel))
	}

	// DELETE the first 10 relationships.
	deletedRels := make([]tuple.Relationship, 0, 10)
	for i := 0; i < 10; i++ {
		rel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR("document", "somedoc", "viewer"),
				Subject:  tuple.ONR("user", "user-"+strconv.Itoa(i), tuple.Ellipsis),
			},
		}
		deletedRels = append(deletedRels, rel)
		updates = append(updates, tuple.Delete(rel))
	}

	for i := 10; i < 100; i++ {
		expectedRels = append(expectedRels, rels[i])
	}

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, updates)
	})
	require.NoError(err)

	// Ensure the expected relationships all exist.
	ensureRelationships(ctx, require, ds, expectedRels...)
	ensureNotRelationships(ctx, require, ds, deletedRels...)
}

// DeleteWithLimitTest tests deleting relationships with a limit.
func DeleteWithLimitTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)
	ctx := context.Background()

	// Write the 1000 relationships.
	rels := make([]tuple.Relationship, 0, 1000)
	for i := 0; i < 1000; i++ {
		rels = append(rels, makeTestRel("foo", fmt.Sprintf("user%d", i)))
	}

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, rels...)
	require.NoError(err)
	ensureRelationships(ctx, require, ds, rels...)

	// Delete 100 rels.
	var deleteLimit uint64 = 100
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		numDeleted, limitReached, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
			ResourceType: testResourceNamespace,
		}, options.WithDeleteLimit(&deleteLimit))
		require.NoError(err)
		require.True(limitReached)
		require.Equal(deleteLimit, numDeleted)
		return nil
	})
	require.NoError(err)

	// Ensure 900 rels remain.
	found := countRels(ctx, require, ds, testResourceNamespace)
	require.Equal(900, found)

	// Delete the remainder.
	deleteLimit = 1000
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		numDeleted, limitReached, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
			ResourceType: testResourceNamespace,
		}, options.WithDeleteLimit(&deleteLimit))
		require.NoError(err)
		require.False(limitReached)
		require.Equal(uint64(900), numDeleted)
		return nil
	})
	require.NoError(err)

	found = countRels(ctx, require, ds, testResourceNamespace)
	require.Equal(0, found)
}

// DeleteCaveatedTupleTest tests deleting a relationship with a caveat.
func DeleteCaveatedTupleTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	tpl, err := tuple.Parse("test/resource:someresource#viewer@test/user:someuser[somecaveat]")
	require.NoError(err)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, tpl)
	require.NoError(err)
	ensureRelationships(ctx, require, ds, tpl)

	// Delete the tuple.
	withoutCaveat, err := tuple.Parse("test/resource:someresource#viewer@test/user:someuser")
	require.NoError(err)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationDelete, withoutCaveat)
	require.NoError(err)
	ensureNotRelationships(ctx, require, ds, tpl, withoutCaveat)
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
						_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, tpl)
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
						_, _, err := rwt.DeleteRelationships(ctx, tc.filter, options.WithDeleteLimit(delLimit))
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

					found, err := datastore.IteratorToSlice(iter)
					require.NoError(err)
					require.Empty(found, "got relationships: %v", found)

					// Ensure the expected relationships were deleted.
					resourceTypes := mapz.NewSet[string]()
					for _, rel := range tc.relationships {
						tpl := tuple.MustParse(rel)
						resourceTypes.Add(tpl.Resource.ObjectType)
					}

					allRemainingRelationships := mapz.NewSet[string]()
					for _, resourceType := range resourceTypes.AsSlice() {
						iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
							OptionalResourceType: resourceType,
						})
						require.NoError(err)

						for rel, err := range iter {
							require.NoError(err)
							allRemainingRelationships.Add(tuple.MustString(rel))
						}
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

						for rel, err := range iter {
							require.NoError(err)
							allInitialRelationships.Add(tuple.MustString(rel))
						}
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

	relationships := make([]tuple.Relationship, 100)
	for i := 0; i < 100; i++ {
		relationships[i] = tuple.MustParse(fmt.Sprintf("document:%d#owner@user:first", i))
	}

	writeRelationships := func() error {
		_, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, relationships...)
		return err
	}

	deleteRelationships := func() error {
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			delLimit := uint64(100)
			_, _, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
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
		name              string
		filter            datastore.RelationshipsFilter
		withoutCaveats    bool
		withoutExpiration bool
		relationships     []string
		expected          []string
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
			name: "nonmatching resource type",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "other",
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
		{
			name: "resource type with caveats",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
			},
			relationships: []string{
				"document:first#viewer@user:tom[firstcaveat]",
				"document:second#viewer@user:tom[secondcaveat]",
				"folder:secondfolder#viewer@user:tom",
				"folder:someotherfolder#viewer@user:tom",
			},
			expected: []string{"document:first#viewer@user:tom[firstcaveat]", "document:second#viewer@user:tom[secondcaveat]"},
		},
		{
			name: "resource type with caveats and context",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
			},
			relationships: []string{
				"document:first#viewer@user:tom[firstcaveat:{\"foo\":\"bar\"}]",
				"document:second#viewer@user:tom[secondcaveat]",
				"document:third#viewer@user:tom[secondcaveat:{\"bar\":\"baz\"}]",
				"folder:secondfolder#viewer@user:tom",
				"folder:someotherfolder#viewer@user:tom",
			},
			expected: []string{
				"document:first#viewer@user:tom[firstcaveat:{\"foo\":\"bar\"}]",
				"document:second#viewer@user:tom[secondcaveat]",
				"document:third#viewer@user:tom[secondcaveat:{\"bar\":\"baz\"}]",
			},
		},
		{
			name: "relationship expiration",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:first#expiring_viewer@user:fred[expiration:2021-01-01T00:00:00Z]",
				"document:first#expiring_viewer@user:sarah[expiration:2900-01-02T01:02:03Z]",
			},
			expected: []string{
				"document:first#viewer@user:tom",
				"document:first#expiring_viewer@user:sarah[expiration:2900-01-02T01:02:03Z]",
			},
		},
		{
			name: "relationship expiration filtered by expiration required",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalExpirationOption: datastore.ExpirationFilterOptionHasExpiration,
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:first#expiring_viewer@user:fred[expiration:2021-01-01T00:00:00Z]",
				"document:first#expiring_viewer@user:sarah[expiration:2900-01-02T01:02:03Z]",
			},
			expected: []string{
				"document:first#expiring_viewer@user:sarah[expiration:2900-01-02T01:02:03Z]",
			},
		},
		{
			name: "relationship expiration filtered by expiration disallowed",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalExpirationOption: datastore.ExpirationFilterOptionNoExpiration,
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:first#expiring_viewer@user:fred[expiration:2021-01-01T00:00:00Z]",
				"document:first#expiring_viewer@user:sarah[expiration:2900-01-02T01:02:03Z]",
			},
			expected: []string{
				"document:first#viewer@user:tom",
			},
		},
		{
			name: "no caveats and no expiration",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:first#viewer@user:fred",
				"document:first#viewer@user:sarah",
			},
			expected: []string{
				"document:first#viewer@user:tom",
				"document:first#viewer@user:fred",
				"document:first#viewer@user:sarah",
			},
			withoutCaveats:    true,
			withoutExpiration: true,
		},
		{
			name: "multiple subject IDs with subject type",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"tom", "fred"},
					},
				},
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:first#viewer@user:fred",
				"document:second#viewer@anotheruser:fred#something",
				"folder:secondfolder#viewer@anotheruser:sarah",
				"folder:someotherfolder#viewer@user:tom",
			},
			expected: []string{
				"document:first#viewer@user:tom",
				"document:first#viewer@user:fred",
				"folder:someotherfolder#viewer@user:tom",
			},
		},
		{
			name: "relationships with expiration",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
			},
			relationships: []string{
				"document:first#expiring_viewer@user:tom[expiration:2020-01-01T00:00:00Z]",
				"document:first#expiring_viewer@user:fred[expiration:2321-01-01T00:00:00Z]",
			},
			expected: []string{
				"document:first#expiring_viewer@user:fred[expiration:2321-01-01T00:00:00Z]",
			},
		},
		{
			name: "multiple subject filters with multiple ids",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"tom", "fred"},
					},
					{
						OptionalSubjectType: "anotheruser",
						OptionalSubjectIds:  []string{"tom", "jerry"},
					},
				},
			},
			relationships: []string{
				"document:first#viewer@user:tom",
				"document:first#viewer@user:fred",
				"document:second#viewer@anotheruser:fred#something",
				"folder:secondfolder#viewer@anotheruser:tom",
				"folder:secondfolder#viewer@anotheruser:sarah",
				"folder:secondfolder#viewer@anotheruser:jerry",
				"folder:someotherfolder#viewer@user:tom",
			},
			expected: []string{
				"document:first#viewer@user:tom",
				"document:first#viewer@user:fred",
				"folder:someotherfolder#viewer@user:tom",
				"folder:secondfolder#viewer@anotheruser:tom",
				"folder:secondfolder#viewer@anotheruser:jerry",
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
				_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, tpl)
				require.NoError(err)
			}

			headRev, err := ds.HeadRevision(ctx)
			require.NoError(err)

			reader := ds.SnapshotReader(headRev)
			iter, err := reader.QueryRelationships(ctx, tc.filter, options.WithSkipCaveats(tc.withoutCaveats), options.WithSkipExpiration(tc.withoutExpiration))
			require.NoError(err)

			var results []string
			for rel, err := range iter {
				require.NoError(err)
				results = append(results, tuple.MustString(rel))
			}

			require.ElementsMatch(tc.expected, results)
		})
	}
}

// RelationshipExpirationTest tests expiration on relationships.
func RelationshipExpirationTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	// Create a relationship that expires in the future.
	rel1, err := tuple.Parse("document:foo#expiring_viewer@user:tom[expiration:2900-01-01T00:00:00Z]")
	require.NoError(err)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, rel1)
	require.NoError(err)
	ensureRelationships(ctx, require, ds, rel1)
	ensureReverseRelationships(ctx, require, ds, rel1)

	// Touch the relationship to a different expiration.
	rel2, err := tuple.Parse("document:foo#expiring_viewer@user:tom[expiration:2800-01-01T00:00:00Z]")
	require.NoError(err)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, rel2)
	require.NoError(err)
	ensureRelationships(ctx, require, ds, rel2)
	ensureReverseRelationships(ctx, require, ds, rel2)

	// Touch the relationship to a time in the past.
	rel3, err := tuple.Parse("document:foo#expiring_viewer@user:tom[expiration:2000-01-01T00:00:00Z]")
	require.NoError(err)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, rel3)
	require.NoError(err)
	ensureNotRelationships(ctx, require, ds, rel3)
	ensureNotReverseRelationships(ctx, require, ds, rel3)

	// Try to recreate the relationship, which should fail even though the expiration has past.
	relrecreate, err := tuple.Parse("document:foo#expiring_viewer@user:tom[expiration:3000-01-01T00:00:00Z]")
	require.NoError(err)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, relrecreate)
	require.Error(err)
	require.ErrorContains(err, "already existed")

	// Touch the relationship back to the future.
	rel4, err := tuple.Parse("document:foo#expiring_viewer@user:tom[expiration:3000-01-01T00:00:00Z]")
	require.NoError(err)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, rel4)
	require.NoError(err)
	ensureRelationships(ctx, require, ds, rel4)
	ensureReverseRelationships(ctx, require, ds, rel4)
}

// TypedTouchAlreadyExistingTest tests touching a relationship twice, when valid type information is provided.
func TypedTouchAlreadyExistingTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	tpl1, err := tuple.Parse("document:foo#viewer@user:tom")
	require.NoError(err)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, tpl1)
	require.NoError(err)
	ensureRelationships(ctx, require, ds, tpl1)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, tpl1)
	require.NoError(err)
	ensureRelationships(ctx, require, ds, tpl1)
}

// TypedTouchAlreadyExistingWithCaveatTest tests touching a relationship twice, when valid type information is provided.
func TypedTouchAlreadyExistingWithCaveatTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	ctpl1, err := tuple.Parse("document:foo#caveated_viewer@user:tom[test:{\"foo\":\"bar\"}]")
	require.NoError(err)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, ctpl1)
	require.NoError(err)
	ensureRelationships(ctx, require, ds, ctpl1)

	ctpl1Updated, err := tuple.Parse("document:foo#caveated_viewer@user:tom[test:{\"foo\":\"baz\"}]")
	require.NoError(err)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, ctpl1Updated)
	require.NoError(err)
	ensureRelationships(ctx, require, ds, ctpl1Updated)
}

// CreateTouchDeleteTouchTest tests writing a relationship, touching it, deleting it, and then touching it.
func CreateTouchDeleteTouchTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	tpl1 := makeTestRel("foo", "tom")
	tpl2 := makeTestRel("foo", "sarah")

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, tpl1, tpl2)
	require.NoError(err)

	ensureRelationships(ctx, require, ds, tpl1, tpl2)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, tpl1, tpl2)
	require.NoError(err)

	ensureRelationships(ctx, require, ds, tpl1, tpl2)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationDelete, tpl1, tpl2)
	require.NoError(err)

	ensureNotRelationships(ctx, require, ds, tpl1, tpl2)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, tpl1, tpl2)
	require.NoError(err)

	ensureRelationships(ctx, require, ds, tpl1, tpl2)
}

// TouchAlreadyExistingCaveatedTest tests touching a relationship twice.
func TouchAlreadyExistingCaveatedTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	tpl1 := tuple.MustWithCaveat(makeTestRel("foo", "tom"), "formercaveat")
	tpl2 := makeTestRel("foo", "sarah")
	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, tpl1, tpl2)
	require.NoError(err)

	ensureRelationships(ctx, require, ds, tpl1, tpl2)

	ctpl1 := tuple.MustWithCaveat(makeTestRel("foo", "tom"), "somecaveat")
	tpl3 := makeTestRel("foo", "fred")

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, ctpl1, tpl3)
	require.NoError(err)

	ensureRelationships(ctx, require, ds, tpl2, tpl3, ctpl1)
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

		for range it {
			break
		}

		it, err = rwt.QueryRelationships(ctx, datastore.RelationshipsFilter{
			OptionalResourceType: "folder",
		})
		require.NoError(err)

		for range it {
			break
		}

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
			if err != nil {
				return err
			}

			for range iter {
				break
			}

			// We do NOT assert the error here because serialization problems can manifest as errors
			// on the individual writes.
			rtu := tuple.Touch(makeTestRel("new_resource", "new_user"))
			err = rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{rtu})

			waitToStartCloser.Do(func() {
				close(waitToStart)
			})
			<-waitToFinish
			return err
		})
		return err
	})

	<-waitToStart

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		defer waitToFinishCloser.Do(func() {
			close(waitToFinish)
		})

		rtu := tuple.Touch(makeTestRel("another_resource", "another_user"))
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{rtu})
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
		_, err := rwt.BulkLoad(ctx, testfixtures.NewBulkRelationshipGenerator(testResourceNamespace, testReaderRelation, testUserNamespace, 1000, t))
		if err != nil {
			return err
		}

		_, err = rwt.BulkLoad(ctx, testfixtures.NewBulkRelationshipGenerator(testResourceNamespace, testEditorRelation, testUserNamespace, 1000, t))
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
		_, _, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
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

	found, err := datastore.IteratorToSlice(iter)
	require.NoError(err)
	require.Empty(found)
}

func onrToSubjectsFilter(onr tuple.ObjectAndRelation) datastore.SubjectsFilter {
	return datastore.SubjectsFilter{
		SubjectType:        onr.ObjectType,
		OptionalSubjectIds: []string{onr.ObjectID},
		RelationFilter:     datastore.SubjectRelationFilter{}.WithNonEllipsisRelation(onr.Relation),
	}
}

func ensureReverseRelationships(ctx context.Context, require *require.Assertions, ds datastore.Datastore, rels ...tuple.Relationship) {
	ensureReverseRelationshipsStatus(ctx, require, ds, rels, true)
}

func ensureNotReverseRelationships(ctx context.Context, require *require.Assertions, ds datastore.Datastore, rels ...tuple.Relationship) {
	ensureReverseRelationshipsStatus(ctx, require, ds, rels, false)
}

func ensureReverseRelationshipsStatus(ctx context.Context, require *require.Assertions, ds datastore.Datastore, rels []tuple.Relationship, mustExist bool) {
	headRev, err := ds.HeadRevision(ctx)
	require.NoError(err)

	reader := ds.SnapshotReader(headRev)

	for _, rel := range rels {
		filter := datastore.SubjectRelationFilter{
			NonEllipsisRelation: rel.Subject.Relation,
		}
		if rel.Subject.Relation == tuple.Ellipsis {
			filter.IncludeEllipsisRelation = true
			filter.NonEllipsisRelation = ""
		}

		iter, err := reader.ReverseQueryRelationships(ctx, datastore.SubjectsFilter{
			SubjectType:        rel.Subject.ObjectType,
			OptionalSubjectIds: []string{rel.Subject.ObjectID},
			RelationFilter:     filter,
		})
		require.NoError(err)

		found, err := datastore.IteratorToSlice(iter)
		require.NoError(err)

		if mustExist {
			require.NotEmpty(found, "expected relationship %s", tuple.MustString(rel))
		} else {
			require.Empty(found, "expected relationship %s to not exist", tuple.MustString(rel))
		}

		if mustExist {
			require.Equal(1, len(found))
			require.Equal(tuple.MustString(rel), tuple.MustString(found[0]))
		}
	}
}

func ensureRelationships(ctx context.Context, require *require.Assertions, ds datastore.Datastore, rels ...tuple.Relationship) {
	ensureRelationshipsStatus(ctx, require, ds, rels, true)
}

func ensureNotRelationships(ctx context.Context, require *require.Assertions, ds datastore.Datastore, rels ...tuple.Relationship) {
	ensureRelationshipsStatus(ctx, require, ds, rels, false)
}

func ensureRelationshipsStatus(ctx context.Context, require *require.Assertions, ds datastore.Datastore, rels []tuple.Relationship, mustExist bool) {
	headRev, err := ds.HeadRevision(ctx)
	require.NoError(err)

	reader := ds.SnapshotReader(headRev)

	for _, rel := range rels {
		iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
			OptionalResourceType:     rel.Resource.ObjectType,
			OptionalResourceIds:      []string{rel.Resource.ObjectID},
			OptionalResourceRelation: rel.Resource.Relation,
			OptionalSubjectsSelectors: []datastore.SubjectsSelector{
				{
					OptionalSubjectType: rel.Subject.ObjectType,
					OptionalSubjectIds:  []string{rel.Subject.ObjectID},
				},
			},
		})
		require.NoError(err)

		found, err := datastore.IteratorToSlice(iter)
		require.NoError(err)

		if mustExist {
			require.NotEmpty(found, "expected relationship %s", tuple.MustString(rel))
		} else {
			require.Empty(found, "expected relationship %s to not exist", tuple.MustString(rel))
		}

		if mustExist {
			require.Equal(1, len(found))
			require.Equal(tuple.MustString(rel), tuple.MustString(found[0]))
		}
	}
}

func countRels(ctx context.Context, require *require.Assertions, ds datastore.Datastore, resourceType string) int {
	headRev, err := ds.HeadRevision(ctx)
	require.NoError(err)

	reader := ds.SnapshotReader(headRev)

	iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: resourceType,
	})
	require.NoError(err)

	counter := 0
	for _, err := range iter {
		require.NoError(err)
		counter++
	}

	return counter
}
