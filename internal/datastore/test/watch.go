package test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/scylladb/go-set/strset"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	v0 "github.com/authzed/spicedb/internal/proto/authzed/api/v0"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestWatch(t *testing.T, tester DatastoreTester) {
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
		t.Run(strconv.Itoa(tc.numTuples), func(t *testing.T) {
			require := require.New(t)

			ds, err := tester.New(0, veryLargeGCWindow, 16)
			require.NoError(err)

			setupDatastore(ds, require)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			lowestRevision, err := ds.SyncRevision(ctx)
			require.NoError(err)

			changes, errchan := ds.Watch(ctx, lowestRevision)
			require.Zero(len(errchan))

			var testUpdates [][]*v0.RelationTupleUpdate
			for i := 0; i < tc.numTuples; i++ {
				newUpdate := tuple.Touch(
					makeTestTuple(fmt.Sprintf("relation%d", i), fmt.Sprintf("user%d", i)),
				)
				batch := []*v0.RelationTupleUpdate{newUpdate}
				testUpdates = append(testUpdates, batch)
				_, err := ds.WriteTuples(ctx, nil, batch)
				require.NoError(err)
			}

			updateUpdate := tuple.Touch(makeTestTuple("relation0", "user0"))
			createUpdate := tuple.Touch(makeTestTuple("another_relation", "somestuff"))
			batch := []*v0.RelationTupleUpdate{updateUpdate, createUpdate}
			_, err = ds.WriteTuples(ctx, nil, batch)
			require.NoError(err)

			deleteUpdate := tuple.Delete(makeTestTuple("relation0", "user0"))
			_, err = ds.WriteTuples(ctx, nil, []*v0.RelationTupleUpdate{deleteUpdate})
			require.NoError(err)

			testUpdates = append(testUpdates, batch, []*v0.RelationTupleUpdate{deleteUpdate})

			verifyUpdates(require, testUpdates, changes, errchan, tc.expectFallBehind)

			// Test the catch-up case
			changes, errchan = ds.Watch(ctx, lowestRevision)
			verifyUpdates(require, testUpdates, changes, errchan, tc.expectFallBehind)
		})
	}
}

func verifyUpdates(
	require *require.Assertions,
	testUpdates [][]*v0.RelationTupleUpdate,
	changes <-chan *datastore.RevisionChanges,
	errchan <-chan error,
	expectDisconnect bool,
) {
	for _, expected := range testUpdates {
		changeWait := time.NewTimer(5 * time.Second)
		select {
		case change, ok := <-changes:
			if !ok {
				require.True(expectDisconnect)
				errWait := time.NewTimer(2 * time.Second)
				select {
				case err := <-errchan:
					require.True(errors.As(err, &datastore.ErrWatchDisconnected{}))
					return
				case <-errWait.C:
					require.Fail("Timed out")
				}
				return
			}

			expectedChangeSet := setOfChanges(expected)
			actualChangeSet := setOfChanges(change.Changes)
			require.True(expectedChangeSet.IsEqual(actualChangeSet))
		case <-changeWait.C:
			require.Fail("Timed out")
		}
	}

	require.False(expectDisconnect)
}

func updateString(update *v0.RelationTupleUpdate) string {
	return fmt.Sprintf("%s(%s)", update.Operation, tuple.String(update.Tuple))
}

func setOfChanges(changes []*v0.RelationTupleUpdate) *strset.Set {
	changeSet := strset.NewWithSize(len(changes))
	for _, oneChange := range changes {
		changeSet.Add(updateString(oneChange))
	}
	return changeSet
}

func TestWatchCancel(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(err)

	startWatchRevision := setupDatastore(ds, require)

	ctx, cancel := context.WithCancel(context.Background())
	changes, errchan := ds.Watch(ctx, startWatchRevision)
	require.Zero(len(errchan))

	_, err = ds.WriteTuples(ctx, nil, []*v0.RelationTupleUpdate{
		tuple.Create(makeTestTuple("test", "test")),
	})
	require.NoError(err)

	cancel()

	for {
		changeWait := time.NewTimer(250 * time.Millisecond)
		select {
		case created, ok := <-changes:
			if ok {
				require.Equal(
					[]*v0.RelationTupleUpdate{tuple.Create(makeTestTuple("test", "test"))},
					created.Changes,
				)
				require.True(created.Revision.GreaterThan(datastore.NoRevision))
			} else {
				errWait := time.NewTimer(100 * time.Millisecond)
				require.Zero(created)
				select {
				case err := <-errchan:
					require.True(errors.As(err, &datastore.ErrWatchCanceled{}))
					return
				case <-errWait.C:
					require.Fail("Timed out")
				}
				return
			}
		case <-changeWait.C:
			require.Fail("deadline exceeded waiting for cancellation")
		}
	}
}
