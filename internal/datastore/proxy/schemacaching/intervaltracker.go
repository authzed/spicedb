package schemacaching

import (
	"slices"
	"sync"
	"time"

	"github.com/authzed/spicedb/pkg/datastore"
)

// intervalTracker is a specialized type for tracking a value over a set of
// revision-based time intervals.
type intervalTracker[T any] struct {
	// sortedEntries are the entries in the interval tracker, sorted with the *latest* being *first*.
	sortedEntries []intervalTrackerEntry[T]

	// lock is the sync lock for the tracker.
	lock sync.RWMutex
}

// intervalTrackerEntry is a single entry in the interval tracker, over a
// period of revisions.
type intervalTrackerEntry[T any] struct {
	// created is the point at which the entry was created.
	created time.Time

	// value is the value for the tracked interval.
	value T

	// startingRevision at which the interval begins.
	startingRevision datastore.Revision

	// endingRevision is the *exclusive* revision at which the interval ends. If not specified,
	// then the interval is open until the last checkpoint revision.
	endingRevisionOrNil datastore.Revision
}

// newIntervalTracker creates a new interval tracker.
func newIntervalTracker[T any]() *intervalTracker[T] {
	return &intervalTracker[T]{
		sortedEntries: make([]intervalTrackerEntry[T], 0, 1),
	}
}

// removeStaleIntervals removes all fully-defined intervals that were created at least window-time ago.
// Returns true if *all* intervals were removed, and the tracker is now empty.
func (it *intervalTracker[T]) removeStaleIntervals(window time.Duration) bool {
	threshold := time.Now().Add(-window)

	it.lock.Lock()
	defer it.lock.Unlock()

	it.sortedEntries = slices.DeleteFunc(it.sortedEntries, func(entry intervalTrackerEntry[T]) bool {
		// The open-ended entry always remains.
		if entry.endingRevisionOrNil == nil {
			return false
		}

		return entry.created.Before(threshold)
	})

	return len(it.sortedEntries) == 0
}

// lookup performs lookup of the value in the tracker at the specified revision. lastCheckpointRevision is the
// bound to use for the ending revision for the unbounded entry in the tracker (if any).
func (it *intervalTracker[T]) lookup(revision datastore.Revision, lastCheckpointRevision datastore.Revision) (T, bool) {
	it.lock.RLock()
	defer it.lock.RUnlock()

	// NOTE: The sortedEntries slice is sorted from latest to least recent, which is opposite that expected
	// by BinarySearchFunc, so all the returned values below are "inverted".
	index, ok := slices.BinarySearchFunc(
		it.sortedEntries,
		revision,
		func(entry intervalTrackerEntry[T], rev datastore.Revision) int {
			// If the entry's starting revision exactly matches the revision, then we know we've found
			// the correct entry.
			if entry.startingRevision.Equal(rev) {
				return 0
			}

			// If the entry starts after the revision, then it precedes the revision in the slice.
			if entry.startingRevision.GreaterThan(rev) {
				return -1
			}

			// Check if the revision is found within the entry.
			if entry.endingRevisionOrNil != nil {
				// If the revision is less than the ending revision (exclusively), then we've found
				// the correct entry.
				if rev.LessThan(entry.endingRevisionOrNil) {
					return 0
				}

				// Otherwise, the revision is later that ending the entry, which means the revision
				// precedes the entry in the slice.
				return 1
			}

			// If the last checkpoint revision is nil, then the entry's ending revision is closed to
			// anything beyond the entry's starting revision.
			endingRevisionInclusive := lastCheckpointRevision
			if lastCheckpointRevision == nil {
				endingRevisionInclusive = entry.startingRevision
			}

			// If the entry has no ending revision, then the supplied last checkpoint revision is the *inclusive*
			// revision for the ending.
			if rev.LessThan(endingRevisionInclusive) || rev.Equal(endingRevisionInclusive) {
				return 0
			}

			if rev.GreaterThan(endingRevisionInclusive) {
				return -1
			}

			return 1
		})
	if !ok {
		return *new(T), false
	}

	return it.sortedEntries[index].value, true
}

// add adds an entry into the tracker, indicating it becomes alive at the given revision.
// Returns whether the entry was successfully added. An entry can only be added if it is
// the latest entry: any attempt to add an entry at a revision before the latest found will
// return false and no-op.
func (it *intervalTracker[T]) add(entry T, revision datastore.Revision) bool {
	now := time.Now()

	it.lock.Lock()
	defer it.lock.Unlock()

	if len(it.sortedEntries) == 0 {
		it.sortedEntries = append(it.sortedEntries, intervalTrackerEntry[T]{
			created:             now,
			value:               entry,
			startingRevision:    revision,
			endingRevisionOrNil: nil,
		})
		return true
	}

	if revision.LessThan(it.sortedEntries[0].startingRevision) {
		return false
	}

	// If given the same revision as the top entry (which can happen from some datastores), ignore.
	if revision.Equal(it.sortedEntries[0].startingRevision) {
		return true
	}

	it.sortedEntries[0].endingRevisionOrNil = revision
	it.sortedEntries = append([]intervalTrackerEntry[T]{
		{
			created:             now,
			value:               entry,
			startingRevision:    revision,
			endingRevisionOrNil: nil,
		},
	}, it.sortedEntries...)
	return true
}
