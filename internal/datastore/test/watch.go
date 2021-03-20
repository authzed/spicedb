package test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
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

			ds, err := tester.New()
			require.NoError(err)

			ctx := context.Background()
			changes, errchan := ds.Watch(ctx, 0)
			require.Zero(len(errchan))

			var testUpdates []*pb.RelationTupleUpdate
			lowestRevision := ^uint64(0)
			for i := 0; i < tc.numTuples; i++ {
				newUpdate := testfixtures.C(makeTestTuple(fmt.Sprintf("relation%d", i), fmt.Sprintf("user%d", i)))
				testUpdates = append(testUpdates, newUpdate)
				newRevision, err := ds.WriteTuples(
					testfixtures.NoPreconditions,
					[]*pb.RelationTupleUpdate{newUpdate},
				)
				require.NoError(err)

				if newRevision < lowestRevision {
					lowestRevision = newRevision
				}
			}

			verifyUpdates(require, testUpdates, changes, errchan, tc.expectFallBehind)

			// Test the catch-up case
			changes, errchan = ds.Watch(ctx, lowestRevision-1)
			verifyUpdates(require, testUpdates, changes, errchan, tc.expectFallBehind)
		})
	}
}

func verifyUpdates(
	require *require.Assertions,
	testUpdates []*pb.RelationTupleUpdate,
	changes <-chan *datastore.RevisionChanges,
	errchan <-chan error,
	expectDisconnect bool,
) {
	for _, expected := range testUpdates {
		changeWait := time.NewTimer(250 * time.Millisecond)
		select {
		case change, ok := <-changes:
			if !ok {
				require.True(expectDisconnect)
				errWait := time.NewTimer(100 * time.Millisecond)
				select {
				case err := <-errchan:
					require.Equal(datastore.ErrWatchDisconnected, err)
					return
				case <-errWait.C:
					require.Fail("Timed out")
				}
				return
			}
			require.Equal([]*pb.RelationTupleUpdate{expected}, change.Changes)
		case <-changeWait.C:
			require.Fail("Timed out")
		}
	}

	require.False(expectDisconnect)
}

func TestWatchCancel(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New()
	require.NoError(err)

	ctx, cancel := context.WithCancel(context.Background())
	changes, errchan := ds.Watch(ctx, 0)
	require.Zero(len(errchan))

	_, err = ds.WriteTuples(
		testfixtures.NoPreconditions,
		[]*pb.RelationTupleUpdate{testfixtures.C(makeTestTuple("test", "test"))},
	)
	require.NoError(err)

	cancel()

	for {
		changeWait := time.NewTimer(250 * time.Millisecond)
		select {
		case created, ok := <-changes:
			if ok {
				require.Equal(
					[]*pb.RelationTupleUpdate{testfixtures.C(makeTestTuple("test", "test"))},
					created.Changes,
				)
				require.Equal(uint64(1), created.Revision)
			} else {
				errWait := time.NewTimer(100 * time.Millisecond)
				require.Zero(created)
				select {
				case err := <-errchan:
					require.Equal(datastore.ErrWatchCanceled, err)
					return
				case <-errWait.C:
					require.Fail("Timed out")
				}
				return
			}
		case <-changeWait.C:
			require.Fail("deadline exceeded waiting to cancellation")
		}
	}
}
