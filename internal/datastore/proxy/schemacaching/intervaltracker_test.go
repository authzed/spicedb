package schemacaching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revision"
)

func rev(value string) datastore.Revision {
	dd := revision.DecimalDecoder{}
	rev, _ := dd.RevisionFromString(value)
	return rev
}

func TestIntervalTrackerBasic(t *testing.T) {
	tracker := newIntervalTracker[string]()

	// Perform lookup on an empty tracker.
	_, found := tracker.lookup(rev("1"), rev("0"))
	require.False(t, found)

	// Add an entry at revision.
	tracker.add("first", rev("1"))
	validate(t, tracker)

	// Ensure the entry is found at its own revision.
	value, found := tracker.lookup(rev("1"), rev("2"))
	require.True(t, found)
	require.Equal(t, "first", value)

	// Ensure the entry is found after that revision based on the tracking revision.
	value, found = tracker.lookup(rev("2"), rev("2"))
	require.True(t, found)
	require.Equal(t, "first", value)

	// Ensure the entry is not found if the tracking revision is less than that specified.
	_, found = tracker.lookup(rev("3"), rev("2"))
	require.False(t, found)

	// Add another entry at revision 4.
	tracker.add("second", rev("4"))
	validate(t, tracker)

	// Ensure that revisions 1-3 find the first.
	for _, rv := range []string{"1", "1.1", "2", "2.6", "3", "3.9", "3.999"} {
		value, found = tracker.lookup(rev(rv), rev("5"))
		require.True(t, found)
		require.Equal(t, "first", value)

		value, found = tracker.lookup(rev(rv), rev("12"))
		require.True(t, found)
		require.Equal(t, "first", value)
	}

	// Ensure that revision 4+ finds the second.
	value, found = tracker.lookup(rev("4"), rev("5"))
	require.True(t, found)
	require.Equal(t, "second", value)

	value, found = tracker.lookup(rev("5"), rev("5"))
	require.True(t, found)
	require.Equal(t, "second", value)

	// Ensure the entry is not found if the tracking revision is less than that specified.
	_, found = tracker.lookup(rev("5.1"), rev("5"))
	require.False(t, found)
}

func TestIntervalTrackerBeginningGap(t *testing.T) {
	tracker := newIntervalTracker[string]()

	// Add an entry at revision 4.
	tracker.add("first", rev("4"))
	validate(t, tracker)

	// Ensure the value is found at revision 4.
	value, found := tracker.lookup(rev("4"), rev("4"))
	require.True(t, found)
	require.Equal(t, "first", value)

	// Ensure the value is *not* found at revision 4.1 with max tracked 4.
	_, found = tracker.lookup(rev("4.1"), rev("4"))
	require.False(t, found)

	// Ensure the value is found at revision 4.1 when maxed tracked is 5.
	value, found = tracker.lookup(rev("4"), rev("5"))
	require.True(t, found)
	require.Equal(t, "first", value)

	// Make sure a request for revision 1-3 (exclusive) with the tracking revision less (since that "update" hasn't "arrived" yet)
	for _, rv := range []string{"1", "1.1", "2", "2.6", "2.999"} {
		_, found = tracker.lookup(rev(rv), rev("3"))
		require.False(t, found)
	}
}

func TestIntervalTrackerOutOfOrderInsertion(t *testing.T) {
	tracker := newIntervalTracker[string]()

	// Add an entry at revision 1.
	require.True(t, tracker.add("first", rev("1")))
	validate(t, tracker)

	// Add an entry at revision 2.
	tracker.add("second", rev("2"))
	validate(t, tracker)

	// Add an entry at revision 4.
	tracker.add("four", rev("4"))
	validate(t, tracker)

	// Add an entry at revision 3.
	require.False(t, tracker.add("third", rev("3")))
	validate(t, tracker)

	// Make sure a request for revision 1-2 (exclusive) refer to 'first'.
	for _, rv := range []string{"1", "1.1", "1.999"} {
		value, found := tracker.lookup(rev(rv), rev("4.1"))
		require.True(t, found)
		require.Equal(t, "first", value)
	}

	// Make sure a request for revision 2-4 (exclusive) refer to 'second'.
	for _, rv := range []string{"2", "2.5", "3.999"} {
		value, found := tracker.lookup(rev(rv), rev("4.1"))
		require.True(t, found)
		require.Equal(t, "second", value)
	}

	// Make sure a request for revision 4+ refers to 'four'
	for _, rv := range []string{"4", "4.01", "4.05", "4.1"} {
		value, found := tracker.lookup(rev(rv), rev("4.1"))
		require.True(t, found)
		require.Equal(t, "four", value)
	}

	// Add an entry at revision 8.
	tracker.add("eight", rev("8"))
	validate(t, tracker)

	// Make sure a request for revision 4-8 (exclusive) refers to 'four'
	for _, rv := range []string{"4", "4.01", "4.05", "4.1", "7.999"} {
		value, found := tracker.lookup(rev(rv), rev("8.1"))
		require.True(t, found)
		require.Equal(t, "four", value)
	}

	// Make sure a request for revision 8+ refers to 'eight'
	for _, rv := range []string{"8", "8.01", "8.05", "8.1"} {
		value, found := tracker.lookup(rev(rv), rev("8.1"))
		require.True(t, found)
		require.Equal(t, "eight", value)
	}
}

func TestIntervalTrackerGC(t *testing.T) {
	tracker := newIntervalTracker[string]()

	// Add an entry at revision 2.
	tracker.add("second", rev("2"))
	validate(t, tracker)

	// Add an entry at revision 4.
	tracker.add("four", rev("4"))
	validate(t, tracker)

	// Add an entry at revision 1.
	require.False(t, tracker.add("first", rev("1")))
	validate(t, tracker)

	// Wait 10ms
	time.Sleep(10 * time.Millisecond)

	// GC anything older than 5s, which shouldn't change anything.
	result := tracker.removeStaleIntervals(5 * time.Second)
	require.False(t, false, result)
	require.Equal(t, 2, len(tracker.sortedEntries))

	// GC anything older than 5ms. There should still be a single entry because it is unbounded.
	result = tracker.removeStaleIntervals(5 * time.Millisecond)
	require.False(t, false, result)
	require.Equal(t, 1, len(tracker.sortedEntries))
}

func TestIntervalTrackerAnotherBasicTest(t *testing.T) {
	tracker := newIntervalTracker[string]()

	// Add an entry at revision.
	tracker.add("first", rev("1"))
	validate(t, tracker)

	// Ensure the entry is found at its own revision.
	value, found := tracker.lookup(rev("1"), rev("1"))
	require.True(t, found)
	require.Equal(t, "first", value)

	// Add another entry at revision 2.
	tracker.add("second", rev("2"))
	validate(t, tracker)

	// Ensure the entry is found at its own revision.
	value, found = tracker.lookup(rev("2"), rev("2"))
	require.True(t, found)
	require.Equal(t, "second", value)

	// Ensure entry 1 is found at its own revision.
	value, found = tracker.lookup(rev("1"), rev("2"))
	require.True(t, found)
	require.Equal(t, "first", value)

	// Ensure entry 1 is found at a parent revision.
	value, found = tracker.lookup(rev("1"), rev("3"))
	require.True(t, found)
	require.Equal(t, "first", value)

	// Ensure entry 2 is found at a parent revision.
	value, found = tracker.lookup(rev("2"), rev("3"))
	require.True(t, found)
	require.Equal(t, "second", value)

	value, found = tracker.lookup(rev("3"), rev("3"))
	require.True(t, found)
	require.Equal(t, "second", value)

	// Check to ensure not found for revision 3.
	_, found = tracker.lookup(rev("3"), rev("2"))
	require.False(t, found)

	// Ensure entry 2 is found even if last revision is lower.
	value, found = tracker.lookup(rev("2"), rev("1"))
	require.True(t, found)
	require.Equal(t, "second", value)
}

func TestIntervalTrackerWithNoLastRevision(t *testing.T) {
	tracker := newIntervalTracker[string]()

	// Add an entry at revision.
	tracker.add("first", rev("1"))
	validate(t, tracker)

	// Ensure the entry is found at its own revision.
	value, found := tracker.lookup(rev("1"), rev("1"))
	require.True(t, found)
	require.Equal(t, "first", value)

	// Ensure the entry is found at its own revision.
	value, found = tracker.lookup(rev("1"), nil)
	require.True(t, found)
	require.Equal(t, "first", value)

	// Add another entry at revision 2.
	tracker.add("second", rev("2"))
	validate(t, tracker)

	// Ensure the entry is found at its own revision.
	value, found = tracker.lookup(rev("2"), nil)
	require.True(t, found)
	require.Equal(t, "second", value)

	// Ensure another revision is not found.
	_, found = tracker.lookup(rev("3"), nil)
	require.False(t, found)

	// Ensure another revision is not found.
	_, found = tracker.lookup(rev("0"), nil)
	require.False(t, found)
}

func TestIntervalTrackerRealWorldUsage(t *testing.T) {
	tracker := newIntervalTracker[string]()
	tracker.add("notfound1", rev("1"))
	validate(t, tracker)

	tracker.add("real2", rev("2"))
	validate(t, tracker)

	tracker.add("real2-again", rev("3"))
	validate(t, tracker)

	tracker.add("notfound5", rev("5"))
	validate(t, tracker)

	value, found := tracker.lookup(rev("5"), rev("5"))
	require.True(t, found)
	require.Equal(t, "notfound5", value)

	value, found = tracker.lookup(rev("5"), rev("3.5"))
	require.True(t, found)
	require.Equal(t, "notfound5", value)

	value, found = tracker.lookup(rev("2"), rev("5"))
	require.True(t, found)
	require.Equal(t, "real2", value)

	value, found = tracker.lookup(rev("2"), rev("3.5"))
	require.True(t, found)
	require.Equal(t, "real2", value)

	value, found = tracker.lookup(rev("3.5"), rev("5"))
	require.True(t, found)
	require.Equal(t, "real2-again", value)

	value, found = tracker.lookup(rev("3.5"), rev("3.5"))
	require.True(t, found)
	require.Equal(t, "real2-again", value)
}

func validate(t *testing.T, tracker *intervalTracker[string]) {
	for index, entry := range tracker.sortedEntries {
		if index > 0 {
			require.NotNil(t, entry.endingRevisionOrNil, "found nil ending revision for entry %d", index)
			require.True(t, entry.endingRevisionOrNil.LessThan(tracker.sortedEntries[index-1].startingRevision) || entry.endingRevisionOrNil.Equal(tracker.sortedEntries[index-1].startingRevision), "found entry %v->%v after entry %v->%v", entry.startingRevision, entry.endingRevisionOrNil, tracker.sortedEntries[index-1].startingRevision, tracker.sortedEntries[index-1].endingRevisionOrNil)
			require.True(t, entry.startingRevision.LessThan(entry.endingRevisionOrNil) || entry.startingRevision.Equal(entry.endingRevisionOrNil))
		}
	}
}
