package cursorediterator

import (
	"iter"
	"testing"

	"github.com/stretchr/testify/require"
)

// collectAll collects all items and errors from an iterator sequence.
// Returns separate slices for items and errors.
func collectAll[T any](result iter.Seq2[T, error]) ([]T, []error) {
	items := make([]T, 0)
	errs := make([]error, 0)
	for item, err := range result {
		items = append(items, item)
		errs = append(errs, err)
	}

	return items, errs
}

// collectNoError collects all items from an iterator sequence, requiring no errors.
// Fails the test immediately if any error is encountered.
func collectNoError[T any](t *testing.T, result iter.Seq2[T, error]) []T {
	items := make([]T, 0)
	for item, err := range result {
		require.NoError(t, err)
		items = append(items, item)
	}

	return items
}

// collectUntilError collects items from an iterator sequence until the first error.
// Returns the collected items and the first error encountered (if any).
func collectUntilError[T any](result iter.Seq2[T, error]) ([]T, error) {
	items := make([]T, 0)
	for item, err := range result {
		if err != nil {
			return items, err
		}
		items = append(items, item)
	}

	return items, nil
}

// collectFirst collects up to n items from an iterator sequence, requiring no errors.
// Stops after collecting n items or encountering an error.
func collectFirst[T any](t *testing.T, result iter.Seq2[T, error], n int) []T {
	items := make([]T, 0)
	count := 0
	for item, err := range result {
		require.NoError(t, err)
		items = append(items, item)
		count++
		if count >= n {
			break
		}
	}

	return items
}

// simpleIntSequence creates a simple iterator that yields integers from startIndexInclusive to endIndexExclusive.
// The iterator yields each integer and returns nil for errors.
func simpleIntSequence(startIndexInclusive int, endIndexExclusive int) iter.Seq2[int, error] {
	return func(yield func(int, error) bool) {
		for i := startIndexInclusive; i < endIndexExclusive; i++ {
			if !yield(i, nil) {
				return
			}
		}
	}
}
