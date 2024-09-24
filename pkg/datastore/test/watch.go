package test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-set/strset"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const waitForChangesTimeout = 5 * time.Second

// WatchTest tests whether or not the requirements for watching changes hold
// for a particular datastore.
func WatchTest(t *testing.T, tester DatastoreTester) {
	testCases := []struct {
		numTuples        int
		bufferTimeout    time.Duration
		expectFallBehind bool
	}{
		{
			numTuples:        1,
			bufferTimeout:    1 * time.Minute,
			expectFallBehind: false,
		},
		{
			numTuples:        2,
			bufferTimeout:    1 * time.Minute,
			expectFallBehind: false,
		},
		{
			numTuples:        256,
			bufferTimeout:    1 * time.Minute,
			expectFallBehind: false,
		},
		{
			numTuples:        256,
			bufferTimeout:    1 * time.Nanosecond,
			expectFallBehind: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("%d-%v", tc.numTuples, tc.expectFallBehind), func(t *testing.T) {
			require := require.New(t)

			ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 16)
			require.NoError(err)

			setupDatastore(ds, require)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			lowestRevision, err := ds.HeadRevision(ctx)
			require.NoError(err)

			opts := datastore.WatchOptions{
				Content:                 datastore.WatchRelationships,
				WatchBufferLength:       128,
				WatchBufferWriteTimeout: tc.bufferTimeout,
			}
			changes, errchan := ds.Watch(ctx, lowestRevision, opts)
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

			_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				_, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
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

			VerifyUpdates(require, testUpdates, changes, errchan, tc.expectFallBehind)

			// Test the catch-up case
			changes, errchan = ds.Watch(ctx, lowestRevision, opts)
			VerifyUpdates(require, testUpdates, changes, errchan, tc.expectFallBehind)
		})
	}
}

func VerifyUpdates(
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
			actualChangeSet := setOfChanges(change.RelationshipChanges)

			missingExpected := strset.Difference(expectedChangeSet, actualChangeSet)
			unexpected := strset.Difference(actualChangeSet, expectedChangeSet)

			require.True(missingExpected.IsEmpty(), "expected changes missing: %s", missingExpected)
			require.True(unexpected.IsEmpty(), "unexpected changes: %s", unexpected)

			time.Sleep(1 * time.Millisecond)
		case <-changeWait.C:
			require.Fail("Timed out", "waited for changes: %s", expected)
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
	changes, errchan := ds.Watch(ctx, startWatchRevision, datastore.WatchJustRelationships())
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
					created.RelationshipChanges,
					protocmp.Transform(),
				)
				require.Empty(foundDiff)
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
	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchJustRelationships())
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

	VerifyUpdates(require, [][]*core.RelationTupleUpdate{
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
	changes, errchan = ds.Watch(ctx, afterTouchRevision, datastore.WatchJustRelationships())
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
	changes, errchan = ds.Watch(ctx, afterTouchRevision, datastore.WatchJustRelationships())
	require.Zero(len(errchan))

	afterNameChange, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, tuple.Parse("document:firstdoc#viewer@user:tom[somecaveat]"))
	require.NoError(err)

	ensureTuples(ctx, require, ds,
		tuple.Parse("document:firstdoc#viewer@user:tom[somecaveat]"),
		tuple.Parse("document:firstdoc#viewer@user:sarah"),
		tuple.Parse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)

	VerifyUpdates(require, [][]*core.RelationTupleUpdate{
		{tuple.Touch(tuple.Parse("document:firstdoc#viewer@user:tom[somecaveat]"))},
	},
		changes,
		errchan,
		false,
	)

	// TOUCH the relationship again with a caveat context change and ensure it does appear in the watch.
	changes, errchan = ds.Watch(ctx, afterNameChange, datastore.WatchJustRelationships())
	require.Zero(len(errchan))

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, tuple.Parse("document:firstdoc#viewer@user:tom[somecaveat:{\"somecondition\": 42}]"))
	require.NoError(err)

	ensureTuples(ctx, require, ds,
		tuple.Parse("document:firstdoc#viewer@user:tom[somecaveat:{\"somecondition\": 42}]"),
		tuple.Parse("document:firstdoc#viewer@user:sarah"),
		tuple.Parse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)

	VerifyUpdates(require, [][]*core.RelationTupleUpdate{
		{tuple.Touch(tuple.Parse("document:firstdoc#viewer@user:tom[somecaveat:{\"somecondition\": 42}]"))},
	},
		changes,
		errchan,
		false,
	)
}

func WatchWithDeleteTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 16)
	require.NoError(err)

	setupDatastore(ds, require)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lowestRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	// TOUCH a relationship and ensure watch sees it.
	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchJustRelationships())
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

	VerifyUpdates(require, [][]*core.RelationTupleUpdate{
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

	// DELETE the relationship
	changes, errchan = ds.Watch(ctx, afterTouchRevision, datastore.WatchJustRelationships())
	require.Zero(len(errchan))

	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_DELETE, tuple.Parse("document:firstdoc#viewer@user:tom"))
	require.NoError(err)

	ensureTuples(ctx, require, ds,
		tuple.Parse("document:firstdoc#viewer@user:sarah"),
		tuple.Parse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)

	VerifyUpdates(require, [][]*core.RelationTupleUpdate{
		{tuple.Delete(tuple.Parse("document:firstdoc#viewer@user:tom"))},
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

		require.Equal(0, len(changes.RelationshipChanges), "expected no changes")
	case <-changeWait.C:
		return
	}
}

func WatchSchemaTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 16)
	require.NoError(err)

	setupDatastore(ds, require)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lowestRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchJustSchema())
	require.Zero(len(errchan))

	// Write an updated schema and ensure the changes are returned.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.WriteNamespaces(ctx, &core.NamespaceDefinition{
			Name: "somenewnamespace",
		})
		if err != nil {
			return err
		}

		return rwt.WriteCaveats(ctx, []*core.CaveatDefinition{
			{
				Name: "somenewcaveat",
			},
		})
	})
	require.NoError(err)

	verifyMixedUpdates(require, [][]string{
		{
			"changed:somenewnamespace",
			"changed:somenewcaveat",
		},
	}, changes, errchan, false)

	// Delete some namespaces and caveats.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.DeleteNamespaces(ctx, "somenewnamespace")
		if err != nil {
			return err
		}

		return rwt.DeleteCaveats(ctx, []string{"somenewcaveat"})
	})
	require.NoError(err)

	verifyMixedUpdates(require, [][]string{
		{
			"deleted-ns:somenewnamespace",
			"deleted-caveat:somenewcaveat",
		},
	}, changes, errchan, false)
}

func WatchAllTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 16)
	require.NoError(err)

	setupDatastore(ds, require)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lowestRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchOptions{
		Content: datastore.WatchRelationships | datastore.WatchSchema,
	})
	require.Zero(len(errchan))

	// Write an updated schema.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.WriteNamespaces(ctx, &core.NamespaceDefinition{
			Name: "somenewnamespace",
		})
		if err != nil {
			return err
		}

		return rwt.WriteCaveats(ctx, []*core.CaveatDefinition{
			{
				Name: "somenewcaveat",
			},
		})
	})
	require.NoError(err)

	verifyMixedUpdates(require, [][]string{
		{
			"changed:somenewnamespace",
			"changed:somenewcaveat",
		},
	}, changes, errchan, false)

	// Write some relationships.
	_, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH,
		tuple.Parse("document:firstdoc#viewer@user:tom"),
		tuple.Parse("document:firstdoc#viewer@user:sarah"),
		tuple.Parse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)
	require.NoError(err)

	verifyMixedUpdates(require, [][]string{
		{
			"rel:OPERATION_TOUCH(document:firstdoc#viewer@user:fred)",
			"rel:OPERATION_TOUCH(document:firstdoc#viewer@user:sarah)",
			"rel:OPERATION_TOUCH(document:firstdoc#viewer@user:tom)",
		},
	}, changes, errchan, false)

	// Delete some namespaces and caveats.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.DeleteNamespaces(ctx, "somenewnamespace")
		if err != nil {
			return err
		}

		return rwt.DeleteCaveats(ctx, []string{"somenewcaveat"})
	})
	require.NoError(err)

	verifyMixedUpdates(require, [][]string{
		{
			"deleted-ns:somenewnamespace",
			"deleted-caveat:somenewcaveat",
		},
	}, changes, errchan, false)
}

func verifyMixedUpdates(
	require *require.Assertions,
	expectedUpdates [][]string,
	changes <-chan *datastore.RevisionChanges,
	errchan <-chan error,
	expectDisconnect bool,
) {
	for _, expected := range expectedUpdates {
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

			foundChanges := mapz.NewSet[string]()
			for _, changedDef := range change.ChangedDefinitions {
				foundChanges.Insert("changed:" + changedDef.GetName())
			}
			for _, deleted := range change.DeletedNamespaces {
				foundChanges.Insert("deleted-ns:" + deleted)
			}
			for _, deleted := range change.DeletedCaveats {
				foundChanges.Insert("deleted-caveat:" + deleted)
			}

			for _, update := range change.RelationshipChanges {
				foundChanges.Insert("rel:" + fmt.Sprintf("OPERATION_%s(%s)", update.Operation, tuple.StringWithoutCaveat(update.Tuple)))
			}

			found := foundChanges.AsSlice()

			sort.Strings(expected)
			sort.Strings(found)

			require.Equal(expected, found, "found unexpected changes")
			time.Sleep(1 * time.Millisecond)
		case <-changeWait.C:
			require.Fail("Timed out", "waited for changes: %s", expected)
		}
	}

	require.False(expectDisconnect, "all changes verified without expected disconnect")
}

func WatchCheckpointsTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 16)
	require.NoError(err)

	setupDatastore(ds, require)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lowestRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchOptions{
		Content:            datastore.WatchCheckpoints | datastore.WatchRelationships,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Zero(len(errchan))

	afterTouchRevision, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH,
		tuple.Parse("document:firstdoc#viewer@user:tom"),
	)
	require.NoError(err)
	verifyCheckpointUpdate(require, afterTouchRevision, changes)
}

func verifyCheckpointUpdate(
	require *require.Assertions,
	expectedRevision datastore.Revision,
	changes <-chan *datastore.RevisionChanges,
) {
	changeWait := time.NewTimer(waitForChangesTimeout)
	for {
		select {
		case change, ok := <-changes:
			require.True(ok)
			if change.IsCheckpoint {
				require.True(change.Revision.Equal(change.Revision) || change.Revision.GreaterThan(expectedRevision))
				return
			}
		case <-changeWait.C:
			require.Fail("Timed out", "waited for checkpoint")
		}
	}
}
