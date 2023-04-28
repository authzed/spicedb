package graph

import (
	"context"
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

// prepareForPublishing asks the limit tracker to remove an element from the limit requested,
// returning whether that element can be published, as well as a function that should be
// invoked after publishing to cancel the context if the limit has been reached.
//
// Example usage:
//
//	okay, done := limits.prepareForPublishing()
//	defer done()
//
//	if okay {
//		publish(item)
//	}
func (lt *limitTracker) prepareForPublishing() (bool, func()) {
	// if there is no limit defined, then the count is always allowed.
	if !lt.hasLimit {
		return true, func() {}
	}

	// if the limit has been reached, allow no further items to be published.
	if lt.currentLimit == 0 {
		return false, func() {}
	}

	if lt.currentLimit == 1 {
		lt.currentLimit = 0
		return true, lt.cancel
	}

	// otherwise, remove the element from the limit.
	lt.currentLimit -= 1
	return true, func() {}
}

// hasExhaustedLimit returns true if the limit has been reached and all items allowable have been
// published.
func (lt *limitTracker) hasExhaustedLimit() bool {
	return lt.hasLimit && lt.currentLimit == 0
}
