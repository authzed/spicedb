package graph

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var ErrLimitReached = fmt.Errorf("limit has been reached")

// limitTracker is a helper struct for tracking the limit requested by a caller and decrementing
// that limit as results are published.
type limitTracker struct {
	hasLimit     bool
	currentLimit uint32
}

// newLimitTracker creates a new limit tracker, returning the tracker.
func newLimitTracker(optionalLimit uint32) *limitTracker {
	return &limitTracker{
		currentLimit: optionalLimit,
		hasLimit:     optionalLimit > 0,
	}
}

// clone creates a copy of the limitTracker, inheriting the current limit.
func (lt *limitTracker) clone() *limitTracker {
	return &limitTracker{
		currentLimit: lt.currentLimit,
		hasLimit:     lt.hasLimit,
	}
}

// prepareForPublishing asks the limit tracker to remove an element from the limit requested,
// returning whether that element can be published.
//
// Example usage:
//
//	okay := limits.prepareForPublishing()
//	if okay { ... publish ... }
func (lt *limitTracker) prepareForPublishing() bool {
	// if there is no limit defined, then the count is always allowed.
	if !lt.hasLimit {
		return true
	}

	// if the limit has been reached, allow no further items to be published.
	if lt.currentLimit == 0 {
		return false
	}

	// otherwise, remove the element from the limit.
	lt.currentLimit--
	return true
}

// markAlreadyPublished marks that the given count of results has already been published. If the count is
// greater than the limit, returns a spiceerror.
func (lt *limitTracker) markAlreadyPublished(count uint32) error {
	if !lt.hasLimit {
		return nil
	}

	if count > lt.currentLimit {
		return spiceerrors.MustBugf("given published count of %d exceeds the remaining limit of %d", count, lt.currentLimit)
	}

	lt.currentLimit -= count
	if lt.currentLimit == 0 {
		return nil
	}

	return nil
}

// hasExhaustedLimit returns true if the limit has been reached and all items allowable have been
// published.
func (lt *limitTracker) hasExhaustedLimit() bool {
	return lt.hasLimit && lt.currentLimit == 0
}
