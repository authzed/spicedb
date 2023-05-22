package test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-set/strset"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const waitForChangesTimeout = 5 * time.Second

// WatchTest tests whether or not the requirements for watching changes hold
// for a particular datastore.
func WatchTest(t *testing.T, tester DatastoreTester) {
	testCases := []struct {
		numTuples        int
		expectFallBehind bool
	}{
		{
			numTuples:        1,
			expectFallBehind: false,
		},
		{
			numTuples:        2,
			expectFallBehind: false,
		},
		{
			numTuples:        256,
			expectFallBehind: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(strconv.Itoa(tc.numTuples), func(t *testing.T) {
			require := require.New(t)

			ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 16)
			require.NoError(err)

			setupDatastore(ds, require)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			lowestRevision, err := ds.HeadRevision(ctx)
			require.NoError(err)

			changes, errchan := ds.Watch(ctx, lowestRevision)
			require.Zero(len(errchan))

			var testUpdates [][]*core.RelationTupleUpdate
			var bulkDeletes []*core.RelationTupleUpdate
			for i := 0; i < tc.numTuples; i++ {
				newRelationship := makeTestTuple(fmt.Sprintf("relation%d", i), "test_user")
				newUpdate := tuple.Touch(newRelationship)
				batch := []*core.RelationTupleUpdate{newUpdate}
				testUpdates = append(testUpdates, batch)
				_, err := common.UpdateTuplesInDatastore(ctx, ds, newUpdate)
				require.NoError(err)

				if i != 0 {
					bulkDeletes = append(bulkDeletes, tuple.Delete(newRelationship))
				}
			}

			updateUpdate := tuple.Touch(tuple.MustWithCaveat(makeTestTuple("relation0", "test_user"), "somecaveat"))
			createUpdate := tuple.Touch(makeTestTuple("another_relation", "somestuff"))

			batch := []*core.RelationTupleUpdate{updateUpdate, createUpdate}
			_, err = common.UpdateTuplesInDatastore(ctx, ds, batch...)
			require.NoError(err)

			deleteUpdate := tuple.Delete(makeTestTuple("relation0", "test_user"))
			_, err = common.UpdateTuplesInDatastore(ctx, ds, deleteUpdate)
			require.NoError(err)

			testUpdates = append(testUpdates, batch, []*core.RelationTupleUpdate{deleteUpdate})

			_, err = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
				err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
					ResourceType:     testResourceNamespace,
					OptionalRelation: testReaderRelation,
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       testUserNamespace,
						OptionalSubjectId: "test_user",
					},
				})
				require.NoError(err)
				return err
			})
			require.NoError(err)

			if len(bulkDeletes) > 0 {
				testUpdates = append(testUpdates, bulkDeletes)
			}

			verifyUpdates(require, testUpdates, changes, errchan, tc.expectFallBehind)

			// Test the catch-up case
			changes, errchan = ds.Watch(ctx, lowestRevision)
			verifyUpdates(require, testUpdates, changes, errchan, tc.expectFallBehind)
		})
	}
}

func verifyUpdates(
	require *require.Assertions,
	testUpdates [][]*core.RelationTupleUpdate,
	changes <-chan *datastore.RevisionChanges,
	errchan <-chan error,
	expectDisconnect bool,
) {
	for _, expected := range testUpdates {
		changeWait := time.NewTimer(waitForChangesTimeout)
		select {
		case change, ok := <-changes:
			if !ok {
				require.True(expectDisconnect, "unexpected disconnect")
				errWait := time.NewTimer(waitForChangesTimeout)
				select {
				case err := <-errchan:
					require.True(errors.As(err, &datastore.ErrWatchDisconnected{}))
					return
				case <-errWait.C:
					require.Fail("Timed out waiting for ErrWatchDisconnected")
				}
				return
			}

			expectedChangeSet := setOfChanges(expected)
			actualChangeSet := setOfChanges(change.Changes)

			missingExpected := strset.Difference(expectedChangeSet, actualChangeSet)
			unexpected := strset.Difference(actualChangeSet, expectedChangeSet)

			require.True(missingExpected.IsEmpty(), "expected changes missing: %s", missingExpected)
			require.True(unexpected.IsEmpty(), "unexpected changes: %s", unexpected)

			time.Sleep(1 * time.Millisecond)
		case <-changeWait.C:
			require.Fail("Timed out", "waiting for changes: %s", expected)
		}
	}

	require.False(expectDisconnect, "all changes verified without expected disconnect")
}

func setOfChanges(changes []*core.RelationTupleUpdate) *strset.Set {
	changeSet := strset.NewWithSize(len(changes))
	for _, change := range changes {
		changeSet.Add(fmt.Sprintf("OPERATION_%s(%s)", change.Operation, tuple.StringWithoutCaveat(change.Tuple)))
	}
	return changeSet
}

// WatchCancelTest tests whether or not the requirements for cancelling watches
// hold for a particular datastore.
func WatchCancelTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	startWatchRevision := setupDatastore(ds, require)

	ctx, cancel := context.WithCancel(context.Background())
	changes, errchan := ds.Watch(ctx, startWatchRevision)
	require.Zero(len(errchan))

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, makeTestTuple("test", "test"))
	require.NoError(err)

	cancel()

	for {
		changeWait := time.NewTimer(waitForChangesTimeout)
		select {
		case created, ok := <-changes:
			if ok {
				foundDiff := cmp.Diff(
					[]*core.RelationTupleUpdate{tuple.Touch(makeTestTuple("test", "test"))},
					created.Changes,
					protocmp.Transform(),
				)
				require.Empty(foundDiff)
				require.True(created.Revision.GreaterThan(datastore.NoRevision))
			} else {
				errWait := time.NewTimer(waitForChangesTimeout)
				require.Zero(created)
				select {
				case err := <-errchan:
					require.True(errors.As(err, &datastore.ErrWatchCanceled{}))
					return
				case <-errWait.C:
					require.Fail("Timed out waiting for ErrWatchCanceled")
				}
				return
			}
		case <-changeWait.C:
			require.Fail("deadline exceeded waiting for cancellation")
		}
	}
}

func WatchWithTouchTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 16)
	require.NoError(err)

	setupDatastore(ds, require)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lowestRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	// TOUCH a relationship and ensure watch sees it.
	changes, errchan := ds.Watch(ctx, lowestRevision)
	require.Zero(len(errchan))

	afterTouchRevision, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH,
		tuple.Parse("document:firstdoc#viewer@user:tom"),
		tuple.Parse("document:firstdoc#viewer@user:sarah"),
		tuple.Parse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)
	require.NoError(err)

	ensureTuples(ctx, require, ds,
		tuple.Parse("document:firstdoc#viewer@user:tom"),
		tuple.Parse("document:firstdoc#viewer@user:sarah"),
		tuple.Parse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)

	verifyUpdates(require, [][]*core.RelationTupleUpdate{
		{
			tuple.Touch(tuple.Parse("document:firstdoc#viewer@user:tom")),
			tuple.Touch(tuple.Parse("document:firstdoc#viewer@user:sarah")),
			tuple.Touch(tuple.Parse("document:firstdoc#viewer@user:fred[thirdcaveat]")),
		},
	},
		changes,
		errchan,
		false,
	)

	// TOUCH the relationship again with no changes and ensure it does *not* appear in the watch.
	changes, errchan = ds.Watch(ctx, afterTouchRevision)
	require.Zero(len(errchan))

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, tuple.Parse("document:firstdoc#viewer@user:tom"))
	require.NoError(err)

	ensureTuples(ctx, require, ds,
		tuple.Parse("document:firstdoc#viewer@user:tom"),
		tuple.Parse("document:firstdoc#viewer@user:sarah"),
		tuple.Parse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)

	verifyNoUpdates(require,
		changes,
		errchan,
		false,
	)

	// TOUCH the relationship again with a caveat name change and ensure it does appear in the watch.
	changes, errchan = ds.Watch(ctx, afterTouchRevision)
	require.Zero(len(errchan))

	afterNameChange, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, tuple.Parse("document:firstdoc#viewer@user:tom[somecaveat]"))
	require.NoError(err)

	ensureTuples(ctx, require, ds,
		tuple.Parse("document:firstdoc#viewer@user:tom[somecaveat]"),
		tuple.Parse("document:firstdoc#viewer@user:sarah"),
		tuple.Parse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)

	verifyUpdates(require, [][]*core.RelationTupleUpdate{
		{tuple.Touch(tuple.Parse("document:firstdoc#viewer@user:tom[somecaveat]"))},
	},
		changes,
		errchan,
		false,
	)

	// TOUCH the relationship again with a caveat context change and ensure it does appear in the watch.
	changes, errchan = ds.Watch(ctx, afterNameChange)
	require.Zero(len(errchan))

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, tuple.Parse("document:firstdoc#viewer@user:tom[somecaveat:{\"somecondition\": 42}]"))
	require.NoError(err)

	ensureTuples(ctx, require, ds,
		tuple.Parse("document:firstdoc#viewer@user:tom[somecaveat:{\"somecondition\": 42}]"),
		tuple.Parse("document:firstdoc#viewer@user:sarah"),
		tuple.Parse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)

	verifyUpdates(require, [][]*core.RelationTupleUpdate{
		{tuple.Touch(tuple.Parse("document:firstdoc#viewer@user:tom[somecaveat:{\"somecondition\": 42}]"))},
	},
		changes,
		errchan,
		false,
	)
}

func verifyNoUpdates(
	require *require.Assertions,
	changes <-chan *datastore.RevisionChanges,
	errchan <-chan error,
	expectDisconnect bool,
) {
	changeWait := time.NewTimer(waitForChangesTimeout)
	select {
	case changes, ok := <-changes:
		if !ok {
			require.True(expectDisconnect, "unexpected disconnect")
			errWait := time.NewTimer(waitForChangesTimeout)
			select {
			case err := <-errchan:
				require.True(errors.As(err, &datastore.ErrWatchDisconnected{}))
				return
			case <-errWait.C:
				require.Fail("Timed out waiting for ErrWatchDisconnected")
			}
			return
		}

		require.Equal(0, len(changes.Changes), "expected no changes")
	case <-changeWait.C:
		return
	}
}
