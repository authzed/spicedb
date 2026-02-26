package test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const waitForChangesTimeout = 20 * time.Second // FIXME, for reasons unknown, tests against Spanner emulator time out if this is lower

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
		t.Run(fmt.Sprintf("%d-%v", tc.numTuples, tc.expectFallBehind), func(t *testing.T) {
			require := require.New(t)

			ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 16)
			require.NoError(err)

			setupDatastore(ds, require)

			lowestRevision, err := ds.HeadRevision(t.Context())
			require.NoError(err)

			opts := datastore.WatchOptions{
				Content:                 datastore.WatchRelationships,
				WatchBufferLength:       50,
				WatchBufferWriteTimeout: tc.bufferTimeout,
			}
			changes, errchan := ds.Watch(t.Context(), lowestRevision, opts)
			require.Empty(errchan)

			var testUpdates [][]tuple.RelationshipUpdate
			var bulkDeletes []tuple.RelationshipUpdate
			for i := 0; i < tc.numTuples; i++ {
				newRelationship := makeTestRel(fmt.Sprintf("relation%d", i), "test_user")

				newUpdate := tuple.Touch(newRelationship)
				testUpdates = append(testUpdates, []tuple.RelationshipUpdate{newUpdate})

				_, err := common.UpdateRelationshipsInDatastore(t.Context(), ds, newUpdate)
				require.NoError(err)

				if i != 0 {
					bulkDeletes = append(bulkDeletes, tuple.Delete(newRelationship))
				}
			}

			updateUpdate := tuple.Touch(tuple.MustWithCaveat(makeTestRel("relation0", "test_user"), "somecaveat"))
			createUpdate := tuple.Touch(makeTestRel("another_relation", "somestuff"))

			_, err = common.UpdateRelationshipsInDatastore(t.Context(), ds, updateUpdate, createUpdate)
			require.NoError(err)

			deleteUpdate := tuple.Delete(makeTestRel("relation0", "test_user"))
			_, err = common.UpdateRelationshipsInDatastore(t.Context(), ds, deleteUpdate)
			require.NoError(err)

			testUpdates = append(testUpdates, []tuple.RelationshipUpdate{updateUpdate, createUpdate}, []tuple.RelationshipUpdate{deleteUpdate})

			_, err = ds.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				_, _, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
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
			changes, errchan = ds.Watch(t.Context(), lowestRevision, opts)
			VerifyUpdates(require, testUpdates, changes, errchan, tc.expectFallBehind)
		})
	}
}

func VerifyUpdates(
	require *require.Assertions,
	testUpdates [][]tuple.RelationshipUpdate,
	changes <-chan datastore.RevisionChanges,
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
					require.ErrorAs(err, &datastore.WatchDisconnectedError{})
					return
				case <-errWait.C:
					require.Fail("Timed out waiting for WatchDisconnectedError")
				}
				return
			}

			expectedChangeSet := setOfChanges(expected)
			actualChangeSet := setOfChanges(change.RelationshipChanges)

			missingExpected := expectedChangeSet.Difference(actualChangeSet)
			unexpected := actualChangeSet.Difference(expectedChangeSet)

			require.True(missingExpected.IsEmpty(), "expected changes missing: %s", missingExpected)
			require.True(unexpected.IsEmpty(), "unexpected changes: %s", unexpected)

			time.Sleep(1 * time.Millisecond)
		case <-changeWait.C:
			require.Fail("Timed out", "waited for changes: %s", expected)
		}
	}

	require.False(expectDisconnect, "all changes verified without expected disconnect")
}

func VerifyUpdatesWithMetadata(
	require *require.Assertions,
	testUpdates []updateWithMetadata,
	changes <-chan datastore.RevisionChanges,
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
					require.ErrorAs(err, &datastore.WatchDisconnectedError{})
					return
				case <-errWait.C:
					require.Fail("Timed out waiting for WatchDisconnectedError")
				}
				return
			}

			expectedChangeSet := setOfChanges(expected.updates)
			actualChangeSet := setOfChanges(change.RelationshipChanges)

			missingExpected := expectedChangeSet.Difference(actualChangeSet)
			unexpected := actualChangeSet.Difference(expectedChangeSet)

			require.True(missingExpected.IsEmpty(), "expected changes missing: %s", missingExpected)
			require.True(unexpected.IsEmpty(), "unexpected changes: %s", unexpected)

			if len(expected.metadata) == 0 {
				require.Empty(change.Metadatas, "expected no metadata, but found some: %v", change.Metadatas)
			} else {
				require.NotNil(change.Metadatas, "expected metadata, but found none")
				require.Len(change.Metadatas, 1, "expected single metadata entry, but found: %v", change.Metadatas)
			}

			time.Sleep(1 * time.Millisecond)
		case <-changeWait.C:
			require.Fail("Timed out", "waited for changes: %s", expected)
		}
	}

	require.False(expectDisconnect, "all changes verified without expected disconnect")
}

func setOfChanges(changes []tuple.RelationshipUpdate) *mapz.Set[string] {
	changeSet := mapz.NewSet[string]()
	for _, change := range changes {
		changeSet.Add(change.DebugString())
	}
	return changeSet
}

// WatchCancelTest tests whether or not the requirements for cancelling watches
// hold for a particular datastore.
func WatchCancelTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	startWatchRevision := setupDatastore(ds, require)

	ctx, cancel := context.WithCancel(t.Context())
	changes, errchan := ds.Watch(ctx, startWatchRevision, datastore.WatchJustRelationships())
	require.Empty(errchan)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, makeTestRel("test", "test"))
	require.NoError(err)

	cancel()

	for {
		changeWait := time.NewTimer(waitForChangesTimeout)
		select {
		case created, ok := <-changes:
			if ok {
				foundDiff := cmp.Diff(
					[]tuple.RelationshipUpdate{tuple.Touch(makeTestRel("test", "test"))},
					created.RelationshipChanges,
					protocmp.Transform(),
				)
				require.Empty(foundDiff)
			} else {
				errWait := time.NewTimer(waitForChangesTimeout)
				require.Zero(created)
				select {
				case err := <-errchan:
					require.ErrorAs(err, &datastore.WatchCanceledError{})
					return
				case <-errWait.C:
					require.Fail("Timed out waiting for WatchCanceledError")
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

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 16)
	require.NoError(err)

	setupDatastore(ds, require)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	lowestRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	// TOUCH a relationship and ensure watch sees it.
	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchJustRelationships())
	require.Empty(errchan)

	afterTouchRevision, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch,
		tuple.MustParse("document:firstdoc#viewer@user:tom"),
		tuple.MustParse("document:firstdoc#viewer@user:sarah"),
		tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)
	require.NoError(err)

	ensureRelationships(ctx, require, ds,
		tuple.MustParse("document:firstdoc#viewer@user:tom"),
		tuple.MustParse("document:firstdoc#viewer@user:sarah"),
		tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)

	VerifyUpdates(require, [][]tuple.RelationshipUpdate{
		{
			tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:tom")),
			tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:sarah")),
			tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]")),
		},
	},
		changes,
		errchan,
		false,
	)

	// TOUCH the relationship again with no changes and ensure it does *not* appear in the watch.
	changes, errchan = ds.Watch(ctx, afterTouchRevision, datastore.WatchJustRelationships())
	require.Empty(errchan)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, tuple.MustParse("document:firstdoc#viewer@user:tom"))
	require.NoError(err)

	ensureRelationships(ctx, require, ds,
		tuple.MustParse("document:firstdoc#viewer@user:tom"),
		tuple.MustParse("document:firstdoc#viewer@user:sarah"),
		tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)

	verifyNoUpdates(require,
		changes,
		errchan,
		false,
	)

	// TOUCH the relationship again with a caveat name change and ensure it does appear in the watch.
	changes, errchan = ds.Watch(ctx, afterTouchRevision, datastore.WatchJustRelationships())
	require.Empty(errchan)

	afterNameChange, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, tuple.MustParse("document:firstdoc#viewer@user:tom[somecaveat]"))
	require.NoError(err)

	ensureRelationships(ctx, require, ds,
		tuple.MustParse("document:firstdoc#viewer@user:tom[somecaveat]"),
		tuple.MustParse("document:firstdoc#viewer@user:sarah"),
		tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)

	VerifyUpdates(require, [][]tuple.RelationshipUpdate{
		{tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:tom[somecaveat]"))},
	},
		changes,
		errchan,
		false,
	)

	// TOUCH the relationship again with a caveat context change and ensure it does appear in the watch.
	changes, errchan = ds.Watch(ctx, afterNameChange, datastore.WatchJustRelationships())
	require.Empty(errchan)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, tuple.MustParse("document:firstdoc#viewer@user:tom[somecaveat:{\"somecondition\": 42}]"))
	require.NoError(err)

	ensureRelationships(ctx, require, ds,
		tuple.MustParse("document:firstdoc#viewer@user:tom[somecaveat:{\"somecondition\": 42}]"),
		tuple.MustParse("document:firstdoc#viewer@user:sarah"),
		tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)

	VerifyUpdates(require, [][]tuple.RelationshipUpdate{
		{tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:tom[somecaveat:{\"somecondition\": 42}]"))},
	},
		changes,
		errchan,
		false,
	)
}

func WatchWithExpirationTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 16)
	require.NoError(err)

	setupDatastore(ds, require)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	lowestRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchJustRelationships())
	require.Empty(errchan)

	metadata, err := structpb.NewStruct(map[string]any{"somekey": "somevalue"})
	require.NoError(err)

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("document:firstdoc#viewer@user:tom[expiration:2321-01-01T00:00:00Z]")),
		})
	}, options.WithMetadata(metadata))
	require.NoError(err)

	VerifyUpdates(require, [][]tuple.RelationshipUpdate{
		{tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:tom[expiration:2321-01-01T00:00:00Z]"))},
	},
		changes,
		errchan,
		false,
	)
}

type updateWithMetadata struct {
	updates  []tuple.RelationshipUpdate
	metadata map[string]any
}

func WatchWithMetadataTest(t *testing.T, tester DatastoreTester) {
	if strings.Contains(t.Name(), "Spanner") {
		t.Skip("Spanner emulator does not support transaction tags. See https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/issues/280")
	}

	require := require.New(t)

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 16)
	require.NoError(err)

	setupDatastore(ds, require)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	lowestRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchJustRelationships())
	require.Empty(errchan)

	metadata, err := structpb.NewStruct(map[string]any{"somekey": "somevalue"})
	require.NoError(err)

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("document:firstdoc#viewer@user:tom")),
		})
	}, options.WithMetadata(metadata))
	require.NoError(err)

	VerifyUpdatesWithMetadata(require, []updateWithMetadata{
		{
			updates:  []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:tom"))},
			metadata: map[string]any{"somekey": "somevalue"},
		},
	},
		changes,
		errchan,
		false,
	)
}

func WatchWithDeleteTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 16)
	require.NoError(err)

	setupDatastore(ds, require)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	lowestRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	// TOUCH a relationship and ensure watch sees it.
	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchJustRelationships())
	require.Empty(errchan)

	afterTouchRevision, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch,
		tuple.MustParse("document:firstdoc#viewer@user:tom"),
		tuple.MustParse("document:firstdoc#viewer@user:sarah"),
		tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)
	require.NoError(err)

	ensureRelationships(ctx, require, ds,
		tuple.MustParse("document:firstdoc#viewer@user:tom"),
		tuple.MustParse("document:firstdoc#viewer@user:sarah"),
		tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)

	VerifyUpdates(require, [][]tuple.RelationshipUpdate{
		{
			tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:tom")),
			tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:sarah")),
			tuple.Touch(tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]")),
		},
	},
		changes,
		errchan,
		false,
	)

	// DELETE the relationship
	changes, errchan = ds.Watch(ctx, afterTouchRevision, datastore.WatchJustRelationships())
	require.Empty(errchan)

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationDelete, tuple.MustParse("document:firstdoc#viewer@user:tom"))
	require.NoError(err)

	ensureRelationships(ctx, require, ds,
		tuple.MustParse("document:firstdoc#viewer@user:sarah"),
		tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)

	VerifyUpdates(require, [][]tuple.RelationshipUpdate{
		{tuple.Delete(tuple.MustParse("document:firstdoc#viewer@user:tom"))},
	},
		changes,
		errchan,
		false,
	)
}

func verifyNoUpdates(
	require *require.Assertions,
	changes <-chan datastore.RevisionChanges,
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
				require.ErrorAs(err, &datastore.WatchDisconnectedError{})
				return
			case <-errWait.C:
				require.Fail("Timed out waiting for WatchDisconnectedError")
			}
			return
		}

		require.Empty(changes.RelationshipChanges, "expected no changes")
	case <-changeWait.C:
		return
	}
}

func WatchSchemaTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 16)
	require.NoError(err)

	setupDatastore(ds, require)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	lowestRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchJustSchema())
	require.Empty(errchan)

	// Addition
	// Write an updated schema and ensure the changes are returned.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.LegacyWriteNamespaces(ctx, &core.NamespaceDefinition{
			Name: "somenewnamespace",
		})
		if err != nil {
			return err
		}

		return rwt.LegacyWriteCaveats(ctx, []*core.CaveatDefinition{
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

	// Changed
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.LegacyWriteNamespaces(ctx, &core.NamespaceDefinition{
			Name: "somenewnamespace",
			Relation: []*core.Relation{
				{
					Name: "anotherrelation",
				},
			},
		})
		if err != nil {
			return err
		}

		return rwt.LegacyWriteCaveats(ctx, []*core.CaveatDefinition{
			{
				Name:                 "somenewcaveat",
				SerializedExpression: []byte("123"),
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

	// Removed
	// Delete some namespaces and caveats.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.LegacyDeleteNamespaces(ctx, []string{"somenewnamespace"}, datastore.DeleteNamespacesAndRelationships)
		if err != nil {
			return err
		}

		return rwt.LegacyDeleteCaveats(ctx, []string{"somenewcaveat"})
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

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 16)
	require.NoError(err)

	setupDatastore(ds, require)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	lowestRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchOptions{
		Content: datastore.WatchRelationships | datastore.WatchSchema,
	})
	require.Empty(errchan)

	// Write an updated schema.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.LegacyWriteNamespaces(ctx, &core.NamespaceDefinition{
			Name: "somenewnamespace",
		})
		if err != nil {
			return err
		}

		return rwt.LegacyWriteCaveats(ctx, []*core.CaveatDefinition{
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
	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch,
		tuple.MustParse("document:firstdoc#viewer@user:tom"),
		tuple.MustParse("document:firstdoc#viewer@user:sarah"),
		tuple.MustParse("document:firstdoc#viewer@user:fred[thirdcaveat]"),
	)
	require.NoError(err)

	verifyMixedUpdates(require, [][]string{
		{
			"rel:TOUCH(document:firstdoc#viewer@user:fred)",
			"rel:TOUCH(document:firstdoc#viewer@user:sarah)",
			"rel:TOUCH(document:firstdoc#viewer@user:tom)",
		},
	}, changes, errchan, false)

	// Delete some namespaces and caveats.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.LegacyDeleteNamespaces(ctx, []string{"somenewnamespace"}, datastore.DeleteNamespacesAndRelationships)
		if err != nil {
			return err
		}

		return rwt.LegacyDeleteCaveats(ctx, []string{"somenewcaveat"})
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
	changes <-chan datastore.RevisionChanges,
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
					require.ErrorAs(err, &datastore.WatchDisconnectedError{})
					return
				case <-errWait.C:
					require.Fail("Timed out waiting for WatchDisconnectedError")
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
				foundChanges.Insert("rel:" + update.DebugString())
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

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 16)
	require.NoError(err)

	setupDatastore(ds, require)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	lowestRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchOptions{
		Content:            datastore.WatchCheckpoints | datastore.WatchRelationships | datastore.WatchSchema,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(errchan)

	afterTouchRevision, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch,
		tuple.MustParse("document:firstdoc#viewer@user:tom"),
	)
	require.NoError(err)

	verifyCheckpointUpdate(require, afterTouchRevision, changes)

	afterTouchRevision, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(ctx, &core.NamespaceDefinition{Name: "doesnotexist"})
	})
	require.NoError(err)

	verifyCheckpointUpdate(require, afterTouchRevision, changes)
}

func WatchEmissionStrategyTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 16)
	require.NoError(err)

	setupDatastore(ds, require)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	features, err := ds.Features(ctx)
	require.NoError(err)

	expectsWatchError := (features.WatchEmitsImmediately.Status != datastore.FeatureSupported)

	lowestRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchOptions{
		Content:            datastore.WatchCheckpoints | datastore.WatchRelationships,
		CheckpointInterval: 100 * time.Millisecond,
		EmissionStrategy:   datastore.EmitImmediatelyStrategy,
	})
	if expectsWatchError {
		require.NotEmpty(errchan)
		err := <-errchan
		require.ErrorContains(err, "emit immediately strategy is unsupported")
		return
	}
	require.Empty(errchan)

	// since changes are streamed immediately, we expect changes to be streamed as independent change events,
	// whereas with the default emission strategy it would be accumulated and normalized. For examples, the default
	// strategy would accumulate everything into a single Change sent over the channel, like an added relationship
	// and its associated transaction metadata. The emit immediately strategy will emit both
	// the rel touch and tx metadata as independent changes as it won't accumulate and normalize it.
	testTuple := tuple.MustParse("document:firstdoc#viewer@user:" + uuid.NewString())
	testMetadata, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(err)
	targetRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Touch(testTuple),
		})
	}, options.WithMetadata(testMetadata))
	require.NoError(err)

	var relTouchEmitted, metadataEmitted, checkpointEmitted bool
	changeWait := time.NewTimer(waitForChangesTimeout)
	var changeCount int
	for {
		select {
		case change, ok := <-changes:
			require.True(ok)
			for _, relChange := range change.RelationshipChanges {
				if relChange.Relationship.WithoutIntegrity() == testTuple && relChange.Operation == tuple.UpdateOperationTouch {
					relTouchEmitted = true
					changeCount++
					require.True(targetRev.Equal(change.Revision))
				}

				continue // we expect each change to come in individual change event
			}

			if len(change.Metadatas) == 1 {
				require.Contains(change.Metadatas[0].AsMap(), "foo")
				metadataEmitted = true
				changeCount++
				require.True(targetRev.Equal(change.Revision))
				continue
			}

			if change.IsCheckpoint {
				if change.Revision.Equal(targetRev) || change.Revision.GreaterThan(targetRev) {
					require.True(metadataEmitted, "expected tx metadata before checkpoint")
					require.True(relTouchEmitted, "expected relationship touch before checkpoint")
					require.GreaterOrEqual(changeCount, 2, "expected at least 2 changes over the channel")
					return
				}
			}
		case <-changeWait.C:
			require.Fail("Timed out", "waited for checkpoint")
		}

		if relTouchEmitted && metadataEmitted && checkpointEmitted {
			return
		}
	}
}

func verifyCheckpointUpdate(
	require *require.Assertions,
	expectedRevision datastore.Revision,
	changes <-chan datastore.RevisionChanges,
) {
	var relChangeEmitted, schemaChangeEmitted bool
	changeWait := time.NewTimer(waitForChangesTimeout)
	for {
		select {
		case change, ok := <-changes:
			require.True(ok)
			if len(change.ChangedDefinitions) > 0 {
				schemaChangeEmitted = true
			}
			if len(change.RelationshipChanges) > 0 {
				relChangeEmitted = true
			}
			if change.IsCheckpoint {
				if change.Revision.Equal(expectedRevision) || change.Revision.GreaterThan(expectedRevision) {
					require.True(relChangeEmitted || schemaChangeEmitted, "expected relationship/schema changes before checkpoint")
					return
				}

				// we received a past revision checkpoint, ignore
			}
		case <-changeWait.C:
			require.Fail("Timed out", "waited for checkpoint")
		}
	}
}
