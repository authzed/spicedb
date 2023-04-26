package graph

import (
	"context"
	"fmt"
)

// limitTracker is a helper struct for tracking the limit requested by a caller and decrementing
// that limit as results are published.
type limitTracker struct {
	hasLimit     bool
	currentLimit uint32
	cancel       func()
}

// newLimitTracker creates a new limit tracker, returning the tracker as well as a context that
// will be automatically canceled once the limit has been reached.
func newLimitTracker(ctx context.Context, optionalLimit uint32) (*limitTracker, context.Context) {
	withCancel, cancel := context.WithCancel(ctx)
	return &limitTracker{
		currentLimit: optionalLimit,
		hasLimit:     optionalLimit > 0,
		cancel:       cancel,
	}, withCancel
}

// mustPrepareForPublishing asks the limit tracker to remove the count from the limit requested,
// returning the number of values of that can be published, as well as a function that should be
// invoked after publishing to cancel the context if the limit has been reached.
//
// Example usage:
//
//	count, done := limits.mustPrepareForPublishing(42)
//	defer done()
//
//	publish(items[0:count])
func (lt *limitTracker) mustPrepareForPublishing(count int) (int, func()) {
	if count <= 0 {
		panic(fmt.Sprintf("got zero or negative count: %d", count))
	}

	// if there is no limit defined, then the count is always allowed.
	if !lt.hasLimit {
		return count, func() {}
	}

	// if the limit has been reached, allow no further items to be published.
	if lt.currentLimit == 0 {
		return 0, func() {}
	}

	// if the count is greater than the limit remaining, only return the limit remaining and mark
	// the context as canceled once the publish has completed.
	if int(lt.currentLimit) < count {
		existingLimit := int(lt.currentLimit)
		lt.currentLimit = 0
		return existingLimit, lt.cancel
	}

	// otherwise, remove the count from the limit.
	lt.currentLimit = lt.currentLimit - uint32(count)
	return count, func() {}
}

// hasExhaustedLimit returns true if the limit has been reached and all items allowable have been
// published.
func (lt *limitTracker) hasExhaustedLimit() bool {
	return lt.hasLimit && lt.currentLimit == 0
}
