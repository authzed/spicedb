package spanner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func at(seconds int) time.Time {
	return time.Unix(int64(seconds), 0).UTC()
}

func TestCheckpointTrackerHoldsBackToSlowestPartition(t *testing.T) {
	t.Parallel()

	tracker := newCheckpointTracker()

	// The stream starts as two partitions, both at the same point.
	tracker.replacedBy("", []string{"a", "b"}, at(10))

	checkpoint, ok := tracker.nextCheckpoint()
	require.True(t, ok)
	require.Equal(t, at(10), checkpoint)

	// One partition races ahead. The checkpoint must not follow it, because the
	// other partition may still be holding changes of the same age.
	tracker.readUpTo("a", at(30))
	_, ok = tracker.nextCheckpoint()
	require.False(t, ok, "checkpoint moved ahead of a partition that has not caught up")

	// Once the other catches up, the checkpoint moves to where both have reached.
	tracker.readUpTo("b", at(20))
	checkpoint, ok = tracker.nextCheckpoint()
	require.True(t, ok)
	require.Equal(t, at(20), checkpoint)
}

func TestCheckpointTrackerAdvancesOnHeartbeatsAlone(t *testing.T) {
	t.Parallel()

	tracker := newCheckpointTracker()
	tracker.replacedBy("", []string{"a", "b"}, at(10))

	_, ok := tracker.nextCheckpoint()
	require.True(t, ok)

	// No changes at all, only the heartbeats Spanner sends while a partition is
	// quiet. The checkpoint still moves forward.
	tracker.readUpTo("a", at(20))
	tracker.readUpTo("b", at(21))

	checkpoint, ok := tracker.nextCheckpoint()
	require.True(t, ok)
	require.Equal(t, at(20), checkpoint)
}

func TestCheckpointTrackerNeverGoesBackwards(t *testing.T) {
	t.Parallel()

	tracker := newCheckpointTracker()
	tracker.replacedBy("", []string{"a"}, at(10))
	tracker.readUpTo("a", at(30))

	checkpoint, ok := tracker.nextCheckpoint()
	require.True(t, ok)
	require.Equal(t, at(30), checkpoint)

	// Nothing new has been read, so there is nothing to report.
	_, ok = tracker.nextCheckpoint()
	require.False(t, ok)

	// An out-of-order or repeated timestamp cannot pull the checkpoint back.
	tracker.readUpTo("a", at(5))
	_, ok = tracker.nextCheckpoint()
	require.False(t, ok)
}

func TestCheckpointTrackerFollowsPartitionSplit(t *testing.T) {
	t.Parallel()

	tracker := newCheckpointTracker()
	tracker.replacedBy("", []string{"a"}, at(10))
	tracker.readUpTo("a", at(20))

	checkpoint, ok := tracker.nextCheckpoint()
	require.True(t, ok)
	require.Equal(t, at(20), checkpoint)

	// "a" is split into two. It is done, and the two replacing it start at 30.
	tracker.replacedBy("a", []string{"b", "c"}, at(30))

	checkpoint, ok = tracker.nextCheckpoint()
	require.True(t, ok)
	require.Equal(t, at(30), checkpoint)

	// The partition that was replaced no longer holds anything back.
	tracker.readUpTo("b", at(40))
	tracker.readUpTo("c", at(50))

	checkpoint, ok = tracker.nextCheckpoint()
	require.True(t, ok)
	require.Equal(t, at(40), checkpoint)
}

func TestCheckpointTrackerFollowsPartitionMerge(t *testing.T) {
	t.Parallel()

	tracker := newCheckpointTracker()
	tracker.replacedBy("", []string{"a", "b"}, at(10))
	tracker.readUpTo("a", at(20))
	tracker.readUpTo("b", at(20))

	checkpoint, ok := tracker.nextCheckpoint()
	require.True(t, ok)
	require.Equal(t, at(20), checkpoint)

	// "a" and "b" merge into "c", which each of them announces separately. Until
	// both have announced it, the one still going holds the checkpoint back.
	tracker.replacedBy("a", []string{"c"}, at(30))
	_, ok = tracker.nextCheckpoint()
	require.False(t, ok, "checkpoint moved past a partition that is still being read")

	tracker.replacedBy("b", []string{"c"}, at(30))
	checkpoint, ok = tracker.nextCheckpoint()
	require.True(t, ok)
	require.Equal(t, at(30), checkpoint)
}

func TestCheckpointTrackerReportsNothingBeforeAnyRecord(t *testing.T) {
	t.Parallel()

	tracker := newCheckpointTracker()
	_, ok := tracker.nextCheckpoint()
	require.False(t, ok)
}
