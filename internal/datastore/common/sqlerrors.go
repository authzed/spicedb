package common

import (
	"context"
	"errors"
	"strings"
)

// IsCancellationError determines if an error returned by pgx has been caused by context cancellation.
func IsCancellationError(err error) bool {
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		err.Error() == "conn closed" { // conns are sometimes closed async upon cancellation
		return true
	}
	return false
}

// IsResettableError returns whether the given error is a resettable error.
func IsResettableError(err error) bool {
	// detect when an error is likely due to a node taken out of service
	if strings.Contains(err.Error(), "broken pipe") ||
		strings.Contains(err.Error(), "unexpected EOF") ||
		strings.Contains(err.Error(), "conn closed") ||
		strings.Contains(err.Error(), "connection refused") ||
		strings.Contains(err.Error(), "connection reset by peer") {
		return true
	}

	return false
}
