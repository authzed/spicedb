package graph

import (
	"fmt"
	"iter"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/digests"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// Fake item that implements UniquelyKeyed
type fakeUniqueItem struct {
	key   string
	error error
}

func (f *fakeUniqueItem) UniqueKey() (string, error) {
	return f.key, f.error
}

func TestEstimatedConcurrencyLimit_OptionalLimitZero(t *testing.T) {
	// Setup
	crr := &CursoredLookupResources3{
		digestMap: digests.NewDigestMap(),
	}
	refs := lr3refs{
		req: ValidatedLookupResources3Request{
			DispatchLookupResources3Request: &v1.DispatchLookupResources3Request{
				OptionalLimit: 0, // No limit set
			},
		},
		concurrencyLimit: 8,
	}
	item := &fakeUniqueItem{key: "test-key", error: nil}

	// Test
	var receivedConcurrencyLimit uint16
	fn := func(computedConcurrencyLimit uint16) iter.Seq2[result, error] {
		receivedConcurrencyLimit = computedConcurrencyLimit
		return func(yield func(result, error) bool) {
			// Empty iterator for this test
		}
	}

	// Execute
	iterator := estimatedConcurrencyLimit(crr, refs, item, fn)

	// Consume the iterator to trigger the function call
	for range iterator {
		break
	}

	// Verify that the full concurrency limit was used
	require.Equal(t, uint16(8), receivedConcurrencyLimit)
}

func TestEstimatedConcurrencyLimit_UniqueKeyError(t *testing.T) {
	// Setup
	crr := &CursoredLookupResources3{
		digestMap: digests.NewDigestMap(),
	}
	refs := lr3refs{
		req: ValidatedLookupResources3Request{
			DispatchLookupResources3Request: &v1.DispatchLookupResources3Request{
				OptionalLimit: 100,
			},
		},
		concurrencyLimit: 4,
	}
	expectedErr := fmt.Errorf("test error")
	item := &fakeUniqueItem{key: "", error: expectedErr}

	fn := func(computedConcurrencyLimit uint16) iter.Seq2[result, error] {
		t.Fatal("Function should not be called when UniqueKey fails")
		return nil
	}

	// Execute
	iterator := estimatedConcurrencyLimit(crr, refs, item, fn)

	// Verify error is returned
	var actualErr error
	for _, err := range iterator {
		actualErr = err
		break
	}

	require.Error(t, actualErr)
	require.Contains(t, actualErr.Error(), "failed to get unique key for item")
	require.Contains(t, actualErr.Error(), "test error")
}

func TestEstimatedConcurrencyLimit_NoDigestAvailable(t *testing.T) {
	// Setup
	crr := &CursoredLookupResources3{
		digestMap: digests.NewDigestMap(),
	}
	refs := lr3refs{
		req: ValidatedLookupResources3Request{
			DispatchLookupResources3Request: &v1.DispatchLookupResources3Request{
				OptionalLimit: 100,
			},
		},
		concurrencyLimit: 8,
	}
	item := &fakeUniqueItem{key: "non-existent-key", error: nil}

	// Test
	var receivedConcurrencyLimit uint16
	fn := func(computedConcurrencyLimit uint16) iter.Seq2[result, error] {
		receivedConcurrencyLimit = computedConcurrencyLimit
		return func(yield func(result, error) bool) {
			// Empty iterator for this test
		}
	}

	// Execute
	iterator := estimatedConcurrencyLimit(crr, refs, item, fn)

	// Consume the iterator
	for range iterator {
		break
	}

	// Verify default concurrency limit of 2 was used
	require.Equal(t, uint16(2), receivedConcurrencyLimit)
}

func TestEstimatedConcurrencyLimit_CDFBelowThreshold(t *testing.T) {
	// Setup
	crr := &CursoredLookupResources3{
		digestMap: digests.NewDigestMap(),
	}
	refs := lr3refs{
		req: ValidatedLookupResources3Request{
			DispatchLookupResources3Request: &v1.DispatchLookupResources3Request{
				OptionalLimit: 100,
			},
		},
		concurrencyLimit: 8,
	}
	item := &fakeUniqueItem{key: "test-key", error: nil}

	// Pre-populate digest map with data that will result in CDF <= 0.1
	// Add many high values so that the CDF for limit 100 is low
	for i := 0; i < 100; i++ {
		crr.digestMap.Add("test-key", 200.0) // Values much higher than limit
	}

	// Test
	var receivedConcurrencyLimit uint16
	fn := func(computedConcurrencyLimit uint16) iter.Seq2[result, error] {
		receivedConcurrencyLimit = computedConcurrencyLimit
		return func(yield func(result, error) bool) {
			// Empty iterator for this test
		}
	}

	// Execute
	iterator := estimatedConcurrencyLimit(crr, refs, item, fn)

	// Consume the iterator
	for range iterator {
		break
	}

	// Verify concurrency limit of 1 was used (CDF <= threshold)
	require.Equal(t, uint16(1), receivedConcurrencyLimit)
}

func TestEstimatedConcurrencyLimit_CDFAboveThreshold(t *testing.T) {
	// Setup
	crr := &CursoredLookupResources3{
		digestMap: digests.NewDigestMap(),
	}
	refs := lr3refs{
		req: ValidatedLookupResources3Request{
			DispatchLookupResources3Request: &v1.DispatchLookupResources3Request{
				OptionalLimit: 100,
			},
		},
		concurrencyLimit: 8,
	}
	item := &fakeUniqueItem{key: "test-key", error: nil}

	// Pre-populate digest map with data that will result in CDF > 0.1
	// Add many low values so that the CDF for limit 100 is high
	for i := 0; i < 100; i++ {
		crr.digestMap.Add("test-key", 50.0) // Values lower than limit
	}

	// Test
	var receivedConcurrencyLimit uint16
	fn := func(computedConcurrencyLimit uint16) iter.Seq2[result, error] {
		receivedConcurrencyLimit = computedConcurrencyLimit
		return func(yield func(result, error) bool) {
			// Empty iterator for this test
		}
	}

	// Execute
	iterator := estimatedConcurrencyLimit(crr, refs, item, fn)

	// Consume the iterator
	for range iterator {
		break
	}

	// Verify default concurrency limit of 2 was used (CDF > threshold)
	require.Equal(t, uint16(2), receivedConcurrencyLimit)
}

func TestEstimatedConcurrencyLimit_CountingIteratorTracksResults(t *testing.T) {
	// Setup
	crr := &CursoredLookupResources3{
		digestMap: digests.NewDigestMap(),
	}
	refs := lr3refs{
		req: ValidatedLookupResources3Request{
			DispatchLookupResources3Request: &v1.DispatchLookupResources3Request{
				OptionalLimit: 100,
			},
		},
		concurrencyLimit: 8,
	}
	item := &fakeUniqueItem{key: "test-key", error: nil}

	// Test function that yields some results
	fn := func(computedConcurrencyLimit uint16) iter.Seq2[result, error] {
		return func(yield func(result, error) bool) {
			// Yield 3 results
			for i := 0; i < 3; i++ {
				pramItem := possibleResource{
					resourceID:    fmt.Sprintf("obj%d", i),
					forSubjectIDs: []string{"user1"},
				}
				resultItem := result{
					Item:   pramItem,
					Cursor: nil,
				}
				if !yield(resultItem, nil) {
					break
				}
			}
		}
	}

	// Execute
	iterator := estimatedConcurrencyLimit(crr, refs, item, fn)

	// Consume all results
	count := 0
	for _, err := range iterator {
		require.NoError(t, err)
		count++
	}

	// Verify we got 3 results
	require.Equal(t, 3, count)

	// Verify that the digest map was updated with the count
	cdf, ok := crr.digestMap.CDF("test-key", 100.0)
	require.True(t, ok, "Expected digest to be recorded")
	require.Greater(t, cdf, 0.0, "Expected CDF to be greater than 0")
}
