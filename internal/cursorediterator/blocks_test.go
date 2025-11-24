package cursorediterator

import (
	"context"
	"fmt"
	"iter"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCursoredWithIntegerHeader(t *testing.T) {
	ctx := t.Context()

	t.Run("empty Cursor starts at index 0", func(t *testing.T) {
		headerCalled := false
		var capturedStartIndex int

		header := func(ctx context.Context, startIndex int) iter.Seq2[int, error] {
			headerCalled = true
			capturedStartIndex = startIndex
			return simpleIntSequence(startIndex, startIndex+3) // yields 3 items
		}

		nextFunc := Empty[int]

		result := CursoredWithIntegerHeader(ctx, Cursor{}, header, nextFunc)
		items := collectNoError(t, result)

		require.True(t, headerCalled)
		require.Equal(t, 0, capturedStartIndex)
		require.Len(t, items, 3)

		// Check items and Cursors
		require.Equal(t, 0, items[0].Item)
		require.Equal(t, Cursor{"1"}, items[0].Cursor)
		require.Equal(t, 1, items[1].Item)
		require.Equal(t, Cursor{"2"}, items[1].Cursor)
		require.Equal(t, 2, items[2].Item)
		require.Equal(t, Cursor{"3"}, items[2].Cursor)
	})

	t.Run("cursor with integer head starts at correct index", func(t *testing.T) {
		headerCalled := false
		var capturedStartIndex int

		header := func(ctx context.Context, startIndex int) iter.Seq2[int, error] {
			headerCalled = true
			capturedStartIndex = startIndex
			return simpleIntSequence(startIndex, startIndex+2)
		}

		nextFunc := Empty[int]

		result := CursoredWithIntegerHeader(ctx, Cursor{"remaining", "5"}, header, nextFunc)
		items := collectNoError(t, result)

		require.True(t, headerCalled)
		require.Equal(t, 5, capturedStartIndex)
		require.Len(t, items, 2)

		require.Equal(t, 5, items[0].Item)
		require.Equal(t, Cursor{"6"}, items[0].Cursor)
		require.Equal(t, 6, items[1].Item)
		require.Equal(t, Cursor{"7"}, items[1].Cursor)
	})

	t.Run("completed header value (-1) skips header and calls next", func(t *testing.T) {
		headerCalled := false
		nextCalled := false
		var capturedRemainingCursor Cursor

		header := func(ctx context.Context, startIndex int) iter.Seq2[int, error] {
			headerCalled = true
			return simpleIntSequence(0, 0) // empty
		}

		nextFunc := func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
			nextCalled = true
			capturedRemainingCursor = c
			return func(yield func(ItemAndCursor[int], error) bool) {
				if !yield(ItemAndCursor[int]{Item: 100, Cursor: Cursor{"next-cursor"}}, nil) {
					return
				}
			}
		}

		result := CursoredWithIntegerHeader(ctx, Cursor{"remaining", "cursor", "-1"}, header, nextFunc)
		items := collectNoError(t, result)

		require.False(t, headerCalled)
		require.True(t, nextCalled)
		require.Equal(t, Cursor{"remaining", "cursor"}, capturedRemainingCursor)
		require.Len(t, items, 1)

		// Next items should have Cursor prefixed with -1
		require.Equal(t, 100, items[0].Item)
		require.Equal(t, Cursor{"next-cursor", "-1"}, items[0].Cursor)
	})

	t.Run("header followed by next", func(t *testing.T) {
		var headerStartIndex int
		var nextRemainingCursor Cursor

		header := func(ctx context.Context, startIndex int) iter.Seq2[int, error] {
			headerStartIndex = startIndex
			return simpleIntSequence(startIndex, startIndex+2)
		}

		nextFunc := func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
			nextRemainingCursor = c
			return func(yield func(ItemAndCursor[int], error) bool) {
				if !yield(ItemAndCursor[int]{Item: 200, Cursor: Cursor{"next1"}}, nil) {
					return
				}
				if !yield(ItemAndCursor[int]{Item: 201, Cursor: Cursor{"next2"}}, nil) {
					return
				}
			}
		}

		result := CursoredWithIntegerHeader(ctx, Cursor{"remaining", "3"}, header, nextFunc)
		items := collectNoError(t, result)

		require.Equal(t, 3, headerStartIndex)
		require.Equal(t, Cursor{"remaining"}, nextRemainingCursor)
		require.Len(t, items, 4)

		// Header items
		require.Equal(t, 3, items[0].Item)
		require.Equal(t, Cursor{"4"}, items[0].Cursor)
		require.Equal(t, 4, items[1].Item)
		require.Equal(t, Cursor{"5"}, items[1].Cursor)

		// Next items with -1 prefix
		require.Equal(t, 200, items[2].Item)
		require.Equal(t, Cursor{"next1", "-1"}, items[2].Cursor)
		require.Equal(t, 201, items[3].Item)
		require.Equal(t, Cursor{"next2", "-1"}, items[3].Cursor)
	})

	t.Run("invalid Cursor head returns error", func(t *testing.T) {
		header := func(ctx context.Context, startIndex int) iter.Seq2[int, error] {
			return simpleIntSequence(0, 1)
		}

		nextFunc := Empty[int]

		result := CursoredWithIntegerHeader(ctx, Cursor{"invalid-int"}, header, nextFunc)
		items, errs := collectAll(result)

		require.Len(t, items, 1) // yieldsError yields one default value
		require.Len(t, errs, 1)
		require.Error(t, errs[0])
		require.Contains(t, errs[0].Error(), "invalid syntax")
	})

	t.Run("header with range loop processes until error", func(t *testing.T) {
		headerError := fmt.Errorf("header error")

		header := func(ctx context.Context, startIndex int) iter.Seq2[int, error] {
			return func(yield func(int, error) bool) {
				if !yield(0, nil) { // first success
					return
				}
				if !yield(1, headerError) { // error is discarded by range loop
					return
				}
				if !yield(2, nil) { // continues processing
					return
				}
			}
		}

		nextFunc := Empty[int]

		result := CursoredWithIntegerHeader(ctx, Cursor{}, header, nextFunc)
		items, err := collectUntilError(result)
		require.Error(t, err)
		require.Contains(t, err.Error(), "header error")

		require.Len(t, items, 1)
		require.Equal(t, 0, items[0].Item)
	})

	t.Run("next function receives correct remaining Cursor", func(t *testing.T) {
		var capturedCursor Cursor

		header := func(ctx context.Context, startIndex int) iter.Seq2[int, error] {
			return simpleIntSequence(startIndex, startIndex+1)
		}

		nextFunc := func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
			capturedCursor = c
			return func(yield func(ItemAndCursor[int], error) bool) {
				if !yield(ItemAndCursor[int]{Item: 100, Cursor: Cursor{"custom"}}, nil) {
					return
				}
			}
		}

		result := CursoredWithIntegerHeader(ctx, Cursor{"extra", "data", "5"}, header, nextFunc)
		items := collectNoError(t, result)

		require.Equal(t, Cursor{"extra", "data"}, capturedCursor)
		require.Len(t, items, 2)

		// Header item
		require.Equal(t, 5, items[0].Item)
		require.Equal(t, Cursor{"6"}, items[0].Cursor)

		// Next item with -1 prefix
		require.Equal(t, 100, items[1].Item)
		require.Equal(t, Cursor{"custom", "-1"}, items[1].Cursor)
	})

	t.Run("empty header and empty next", func(t *testing.T) {
		header := func(ctx context.Context, startIndex int) iter.Seq2[int, error] {
			return simpleIntSequence(0, 0) // empty
		}

		nextFunc := Empty[int]

		result := CursoredWithIntegerHeader(ctx, Cursor{}, header, nextFunc)
		items := collectNoError(t, result)

		require.Len(t, items, 0)
	})

	t.Run("cursor index increments correctly", func(t *testing.T) {
		header := func(ctx context.Context, startIndex int) iter.Seq2[int, error] {
			return simpleIntSequence(startIndex, startIndex+5)
		}

		nextFunc := Empty[int]

		result := CursoredWithIntegerHeader(ctx, Cursor{"10"}, header, nextFunc)
		items := collectNoError(t, result)

		require.Len(t, items, 5)
		for i, item := range items {
			expectedValue := 10 + i
			expectedCursor := strconv.Itoa(expectedValue + 1)
			require.Equal(t, expectedValue, item.Item)
			require.Equal(t, Cursor{expectedCursor}, item.Cursor)
		}
	})

	t.Run("header error with consumer early termination", func(t *testing.T) {
		headerError := fmt.Errorf("header processing error")

		header := func(ctx context.Context, startIndex int) iter.Seq2[int, error] {
			return func(yield func(int, error) bool) {
				if !yield(100, nil) { // First success
					return
				}
				if !yield(0, headerError) { // Error that consumer will stop on
					return
				}
			}
		}

		nextFunc := Empty[int]

		result := CursoredWithIntegerHeader(ctx, Cursor{}, header, nextFunc)

		var items []ItemAndCursor[int]
		var errs []error

		for item, err := range result {
			if err != nil {
				errs = append(errs, err)
				break // Consumer stops after first error
			}
			items = append(items, item)
		}

		require.Len(t, items, 1)
		require.Equal(t, 100, items[0].Item)
		require.Len(t, errs, 1)
		require.Contains(t, errs[0].Error(), "header processing error")
	})
}

func TestCursoredParallelIterators(t *testing.T) {
	// Helper functions for creating test iterators
	simpleIterator := func(items []int, prefix string) Next[int] {
		return func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
			return func(yield func(ItemAndCursor[int], error) bool) {
				for i, item := range items {
					select {
					case <-ctx.Done():
						return
					default:
						cursorStr := fmt.Sprintf("%s-%d", prefix, i+1)
						if !yield(ItemAndCursor[int]{Item: item, Cursor: Cursor{cursorStr}}, nil) {
							return
						}
					}
				}
			}
		}
	}

	errorIterator := func(items []int, errorAt int, err error) Next[int] {
		return func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
			return func(yield func(ItemAndCursor[int], error) bool) {
				for i, item := range items {
					if i == errorAt {
						if !yield(ItemAndCursor[int]{}, err) {
							return
						}
						return
					}
					cursorStr := fmt.Sprintf("item-%d", i)
					if !yield(ItemAndCursor[int]{Item: item, Cursor: Cursor{cursorStr}}, nil) {
						return
					}
				}
			}
		}
	}

	concurrencyLevels := []uint16{1, 2, 4, 8}
	for _, concurrency := range concurrencyLevels {
		t.Run(fmt.Sprintf("concurrency %d", concurrency), func(t *testing.T) {
			tcs := []struct {
				name          string
				cursor        Cursor
				iterators     []Next[int]
				expected      []ItemAndCursor[int]
				expectedError string
			}{
				{
					name:   "empty Cursor executes all iterators",
					cursor: Cursor{},
					iterators: []Next[int]{
						simpleIterator([]int{1, 2}, "iter1"),
						simpleIterator([]int{10}, "iter2"),
						simpleIterator([]int{100, 101, 102}, "iter3"),
					},
					expected: []ItemAndCursor[int]{
						{Item: 1, Cursor: Cursor{"iter1-1", "0"}},
						{Item: 2, Cursor: Cursor{"iter1-2", "0"}},
						{Item: 10, Cursor: Cursor{"iter2-1", "1"}},
						{Item: 100, Cursor: Cursor{"iter3-1", "2"}},
						{Item: 101, Cursor: Cursor{"iter3-2", "2"}},
						{Item: 102, Cursor: Cursor{"iter3-3", "2"}},
					},
				},
				{
					name:   "cursor with starting index skips earlier iterators",
					cursor: Cursor{"remaining", "1"},
					iterators: []Next[int]{
						simpleIterator([]int{1}, "iter1"),
						simpleIterator([]int{10}, "iter2"),
						simpleIterator([]int{100}, "iter3"),
					},
					expected: []ItemAndCursor[int]{
						{Item: 10, Cursor: Cursor{"iter2-1", "1"}},
						{Item: 100, Cursor: Cursor{"iter3-1", "2"}},
					},
				},
				{
					name:   "multiple result Cursor with starting index skips earlier iterators",
					cursor: Cursor{"remaining", "1"},
					iterators: []Next[int]{
						simpleIterator([]int{1, 2, 3, 4, 5}, "iter1"),
						simpleIterator([]int{10, 20, 30, 40}, "iter2"),
						simpleIterator([]int{100, 101, 102}, "iter3"),
					},
					expected: []ItemAndCursor[int]{
						{Item: 10, Cursor: Cursor{"iter2-1", "1"}},
						{Item: 20, Cursor: Cursor{"iter2-2", "1"}},
						{Item: 30, Cursor: Cursor{"iter2-3", "1"}},
						{Item: 40, Cursor: Cursor{"iter2-4", "1"}},
						{Item: 100, Cursor: Cursor{"iter3-1", "2"}},
						{Item: 101, Cursor: Cursor{"iter3-2", "2"}},
						{Item: 102, Cursor: Cursor{"iter3-3", "2"}},
					},
				},
				{
					name:          "invalid Cursor head returns error",
					cursor:        Cursor{"invalid-int"},
					iterators:     []Next[int]{simpleIterator([]int{1}, "iter1")},
					expected:      []ItemAndCursor[int]{},
					expectedError: "strconv.Atoi: parsing \"invalid-int\": invalid syntax",
				},
				{
					name:   "iterator error stops processing",
					cursor: Cursor{},
					iterators: []Next[int]{
						errorIterator([]int{1, 2}, 1, fmt.Errorf("iterator error")),
						simpleIterator([]int{10}, "iter2"),
					},
					expected:      []ItemAndCursor[int]{{Item: 1, Cursor: Cursor{"item-0", "0"}}},
					expectedError: "iterator error",
				},
				{
					name:      "empty iterators list",
					cursor:    Cursor{},
					iterators: []Next[int]{},
					expected:  []ItemAndCursor[int]{},
				},
				{
					name:      "single iterator",
					cursor:    Cursor{},
					iterators: []Next[int]{simpleIterator([]int{42}, "single")},
					expected:  []ItemAndCursor[int]{{Item: 42, Cursor: Cursor{"single-1", "0"}}},
				},
			}

			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					ctx := t.Context()
					results := CursoredParallelIterators(ctx, tc.cursor, concurrency, tc.iterators...)

					if tc.expectedError != "" {
						items, err := collectUntilError(results)
						require.Error(t, err)
						require.Contains(t, err.Error(), tc.expectedError)
						require.Len(t, items, len(tc.expected))
						for i, expectedItem := range tc.expected {
							require.Equal(t, expectedItem, items[i])
						}
					} else {
						items := collectNoError(t, results)
						require.Len(t, items, len(tc.expected))
						for i, expectedItem := range tc.expected {
							require.Equal(t, expectedItem, items[i])
						}
					}
				})
			}
		})
	}

	// Test panic cases separately since they can't be easily tested in table-driven tests
	t.Run("panic cases", func(t *testing.T) {
		ctx := t.Context()
		concurrency := uint16(1)

		t.Run("starting index equal to length panics", func(t *testing.T) {
			iter1 := simpleIterator([]int{1}, "iter1")

			require.Panics(t, func() {
				results := CursoredParallelIterators(ctx, Cursor{"1"}, concurrency, iter1)
				for range results {
					// Consume iterator to trigger panic
				}
			})
		})

		t.Run("starting index greater than length panics", func(t *testing.T) {
			iter1 := simpleIterator([]int{1}, "iter1")

			require.Panics(t, func() {
				results := CursoredParallelIterators(ctx, Cursor{"2"}, concurrency, iter1)
				for range results {
					// Consume iterator to trigger panic
				}
			})
		})

		// Test context cancellation to ensure later iterators don't run
		t.Run("context cancellation prevents later iterators from running", func(t *testing.T) {
			// Track which iterators actually run
			iter1Called := false
			iter2Called := false
			iter3Called := false

			// Create iterators with timeouts to allow cancellation to take effect
			slowIterator1 := func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
				return func(yield func(ItemAndCursor[int], error) bool) {
					select {
					case <-ctx.Done():
						require.Fail(t, "iterator 1 should not run after context cancellation")
						return
					default:
						iter1Called = true
						if !yield(ItemAndCursor[int]{Item: 1, Cursor: Cursor{"iter1-done"}}, nil) {
							return
						}
					}
				}
			}

			slowIterator2 := func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
				time.Sleep(10 * time.Millisecond)
				return func(yield func(ItemAndCursor[int], error) bool) {
					// This iterator should not run if context is cancelled early
					select {
					case <-ctx.Done():
						return
					default:
						iter2Called = true
						if !yield(ItemAndCursor[int]{Item: 2, Cursor: Cursor{"iter2-done"}}, nil) {
							return
						}
					}
				}
			}

			slowIterator3 := func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
				time.Sleep(10 * time.Millisecond)
				return func(yield func(ItemAndCursor[int], error) bool) {
					// This iterator should not run if context is cancelled early
					select {
					case <-ctx.Done():
						return
					default:
						iter3Called = true
						if !yield(ItemAndCursor[int]{Item: 3, Cursor: Cursor{"iter3-done"}}, nil) {
							return
						}
					}
				}
			}

			results := CursoredParallelIterators(ctx, Cursor{}, concurrency, slowIterator1, slowIterator2, slowIterator3)

			var items []ItemAndCursor[int]
			for item, err := range results {
				require.NoError(t, err)
				items = append(items, item)
				break
			}

			time.Sleep(25 * time.Millisecond) // Allow time for other iterators to potentially run

			// Verify only the first iterator ran and produced results, as the break in the loop should cancel
			// the context before the others can run.
			require.True(t, iter1Called, "iterator 1 should have been called")

			require.False(t, iter2Called, "iterator 2 should not have been called due to cancellation")
			require.False(t, iter3Called, "iterator 3 should not have been called due to cancellation")

			require.Len(t, items, 1, "should only have received one item before cancellation")
			require.Equal(t, 1, items[0].Item)
			require.Equal(t, Cursor{"iter1-done", "0"}, items[0].Cursor)
		})

		t.Run("iterator error with consumer early termination", func(t *testing.T) {
			iteratorError := fmt.Errorf("iterator processing error")

			errorIterator := func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
				return func(yield func(ItemAndCursor[int], error) bool) {
					// Yield successful item first
					if !yield(ItemAndCursor[int]{Item: 100, Cursor: Cursor{"success"}}, nil) {
						return
					}
					// Yield error and expect consumer to stop
					if !yield(ItemAndCursor[int]{}, iteratorError) {
						return
					}
				}
			}

			results := CursoredParallelIterators(ctx, Cursor{}, uint16(1), errorIterator)

			var items []ItemAndCursor[int]
			var errs []error

			for item, err := range results {
				if err != nil {
					errs = append(errs, err)
					break // Consumer stops after first error
				}
				items = append(items, item)
			}

			require.Len(t, items, 1)
			require.Equal(t, 100, items[0].Item)
			require.Len(t, errs, 1)
			require.Contains(t, errs[0].Error(), "iterator processing error")
		})

		t.Run("context cancellation during iteration processing", func(t *testing.T) {
			ctxWithCancel, cancel := context.WithCancel(t.Context())

			slowIterator := func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
				return func(yield func(ItemAndCursor[int], error) bool) {
					// First item succeeds
					if !yield(ItemAndCursor[int]{Item: 1, Cursor: Cursor{"first"}}, nil) {
						return
					}

					// Cancel context before second item
					cancel()

					// This should detect cancellation
					select {
					case <-ctx.Done():
						return
					default:
						if !yield(ItemAndCursor[int]{Item: 2, Cursor: Cursor{"second"}}, nil) {
							return
						}
					}
				}
			}

			results := CursoredParallelIterators(ctxWithCancel, Cursor{}, uint16(1), slowIterator)
			items := collectNoError(t, results)

			// Should only get the first item before cancellation
			require.Len(t, items, 1)
			require.Equal(t, 1, items[0].Item)
		})
	})
}

func TestCursoredProducerMapperIterator(t *testing.T) {
	ctx := t.Context()

	concurrencyLevels := []uint16{1, 2, 4}
	for _, concurrency := range concurrencyLevels {
		t.Run(fmt.Sprintf("concurrency %d", concurrency), func(t *testing.T) {
			// Helper function to create a simple producer that yields chunks
			simpleProducer := func(chunks []Chunk[[]string, int]) func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
				return func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
					return func(yield func(ChunkOrHold[[]string, int], error) bool) {
						for i := startIndex; i < len(chunks); i++ {
							select {
							case <-ctx.Done():
								return
							default:
								if !yield(chunks[i], nil) {
									return
								}
							}
						}
					}
				}
			}

			// Cursor converter functions for int
			intFromString := strconv.Atoi

			intToString := func(i int) (string, error) {
				return strconv.Itoa(i), nil
			}

			// Helper function to create a simple mapper
			simpleMapper := func(prefix string) func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[int], error] {
				return func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[int], error] {
					return func(yield func(ItemAndCursor[int], error) bool) {
						for i, item := range chunk {
							cursorStr := fmt.Sprintf("%s-%d", prefix, i)
							value := len(item) // Convert string to int based on length
							if !yield(ItemAndCursor[int]{Item: value, Cursor: Cursor{cursorStr}}, nil) {
								return
							}
						}
					}
				}
			}

			t.Run("empty Cursor starts from beginning", func(t *testing.T) {
				chunks := []Chunk[[]string, int]{
					{CurrentChunk: []string{"ab", "cd"}, CurrentChunkCursor: 1},
					{CurrentChunk: []string{"efg"}, CurrentChunkCursor: 2},
				}

				producer := simpleProducer(chunks)
				mapper := simpleMapper("mapped")

				result := CursoredProducerMapperIterator(ctx, Cursor{}, concurrency, intFromString, intToString, producer, mapper)
				items := collectNoError(t, result)

				require.Len(t, items, 3)
				require.Equal(t, 2, items[0].Item) // len("ab")
				require.Equal(t, Cursor{"mapped-0", "1"}, items[0].Cursor)
				require.Equal(t, 2, items[1].Item) // len("cd")
				require.Equal(t, Cursor{"mapped-1", "1"}, items[1].Cursor)
				require.Equal(t, 3, items[2].Item) // len("efg")
				require.Equal(t, Cursor{"mapped-0", "2"}, items[2].Cursor)
			})

			t.Run("cursor with head value starts from correct position", func(t *testing.T) {
				chunks := []Chunk[[]string, int]{
					{CurrentChunk: []string{"ab", "cd"}, CurrentChunkCursor: 1},
					{CurrentChunk: []string{"efg"}, CurrentChunkCursor: 2},
					{CurrentChunk: []string{"hi"}, CurrentChunkCursor: 3},
				}

				producer := simpleProducer(chunks)
				mapper := simpleMapper("mapped")

				result := CursoredProducerMapperIterator(ctx, Cursor{"remaining", "1"}, concurrency, intFromString, intToString, producer, mapper)
				items := collectNoError(t, result)

				require.Len(t, items, 2)
				require.Equal(t, 3, items[0].Item) // len("efg")
				require.Equal(t, Cursor{"mapped-0", "2"}, items[0].Cursor)
				require.Equal(t, 2, items[1].Item) // len("hi")
				require.Equal(t, Cursor{"mapped-0", "3"}, items[1].Cursor)
			})

			t.Run("invalid Cursor head returns error", func(t *testing.T) {
				chunks := []Chunk[[]string, int]{
					{CurrentChunk: []string{"item"}, CurrentChunkCursor: 1},
				}

				producer := simpleProducer(chunks)
				mapper := simpleMapper("mapped")

				result := CursoredProducerMapperIterator(ctx, Cursor{"invalid-int"}, concurrency, intFromString, intToString, producer, mapper)
				items, errs := collectAll(result)

				require.Len(t, items, 1) // yieldsError yields one default value
				require.Len(t, errs, 1)
				require.Error(t, errs[0])
				require.Contains(t, errs[0].Error(), "invalid syntax")
			})

			t.Run("empty producer yields no items", func(t *testing.T) {
				emptyProducer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
					return func(yield func(ChunkOrHold[[]string, int], error) bool) {
						// Yield nothing
					}
				}

				mapper := simpleMapper("mapped")

				result := CursoredProducerMapperIterator(ctx, Cursor{}, concurrency, intFromString, intToString, emptyProducer, mapper)
				items := collectNoError(t, result)

				require.Len(t, items, 0)
			})

			t.Run("producer error handling", func(t *testing.T) {
				producerError := fmt.Errorf("producer failed")
				errorProducer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
					return func(yield func(ChunkOrHold[[]string, int], error) bool) {
						chunk := Chunk[[]string, int]{CurrentChunk: []string{"good"}, CurrentChunkCursor: 1}
						if !yield(chunk, nil) {
							return
						}
						if !yield(Chunk[[]string, int]{}, producerError) {
							return
						}
					}
				}

				mapper := simpleMapper("mapped")

				result := CursoredProducerMapperIterator(ctx, Cursor{}, concurrency, intFromString, intToString, errorProducer, mapper)
				items, err := collectUntilError(result)

				require.Error(t, err)
				require.Contains(t, err.Error(), "producer failed")
				require.Len(t, items, 1)
				require.Equal(t, 4, items[0].Item) // len("good")
			})

			// NOTE: this test exists because we saw a periodic flake with this test
			// where the errors would come out before the items due to a race between
			// goroutines in the internals. This runs the logic several times, which
			// should help prevent a similar regression.
			t.Run("multiple producer error handling", func(t *testing.T) {
				producerError := fmt.Errorf("producer failed")
				errorProducer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
					return func(yield func(ChunkOrHold[[]string, int], error) bool) {
						chunk := Chunk[[]string, int]{CurrentChunk: []string{"good"}, CurrentChunkCursor: 1}
						if !yield(chunk, nil) {
							return
						}
						if !yield(Chunk[[]string, int]{}, producerError) {
							return
						}
					}
				}

				mapper := simpleMapper("mapped")

				for range 20 {
					result := CursoredProducerMapperIterator(ctx, Cursor{}, concurrency, intFromString, intToString, errorProducer, mapper)
					items, err := collectUntilError(result)

					require.Error(t, err)
					require.Contains(t, err.Error(), "producer failed")
					require.Len(t, items, 1)
					require.Equal(t, 4, items[0].Item) // len("good")
				}
			})

			t.Run("mapper function error handling", func(t *testing.T) {
				chunks := []Chunk[[]string, int]{
					{CurrentChunk: []string{"item1", "item2"}, CurrentChunkCursor: 1},
				}

				producer := simpleProducer(chunks)

				mapperError := fmt.Errorf("mapper failed")
				errorMapper := func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[int], error] {
					return func(yield func(ItemAndCursor[int], error) bool) {
						// Yield first item successfully
						if !yield(ItemAndCursor[int]{Item: len(chunk[0]), Cursor: Cursor{"first"}}, nil) {
							return
						}
						// Yield error for second item
						if !yield(ItemAndCursor[int]{}, mapperError) {
							return
						}
					}
				}

				result := CursoredProducerMapperIterator(ctx, Cursor{}, concurrency, intFromString, intToString, producer, errorMapper)
				_, err := collectUntilError(result)

				require.Error(t, err)
				require.Contains(t, err.Error(), "mapper failed")
			})

			t.Run("cursor conversion error handling", func(t *testing.T) {
				chunks := []Chunk[[]string, int]{
					{CurrentChunk: []string{"item"}, CurrentChunkCursor: 1},
				}

				producer := simpleProducer(chunks)
				mapper := simpleMapper("mapped")

				conversionError := fmt.Errorf("conversion failed")
				errorConverter := func(i int) (string, error) {
					return "", conversionError
				}

				result := CursoredProducerMapperIterator(ctx, Cursor{}, concurrency, intFromString, errorConverter, producer, mapper)
				items, err := collectUntilError(result)

				require.Error(t, err)
				require.Contains(t, err.Error(), "conversion failed")
				require.Len(t, items, 0)
			})

			t.Run("context cancellation during producer execution", func(t *testing.T) {
				ctxWithCancel, cancel := context.WithCancel(t.Context())

				slowProducer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
					return func(yield func(ChunkOrHold[[]string, int], error) bool) {
						// First chunk succeeds
						chunk1 := Chunk[[]string, int]{CurrentChunk: []string{"item1"}, CurrentChunkCursor: 1}
						if !yield(chunk1, nil) {
							return
						}

						// Cancel context before second chunk
						time.Sleep(10 * time.Millisecond) // Allow time for processing before
						cancel()

						// This should be cancelled and not execute
						select {
						case <-ctx.Done():
							return
						default:
							chunk2 := Chunk[[]string, int]{CurrentChunk: []string{"item2"}, CurrentChunkCursor: 2}
							if !yield(chunk2, nil) {
								return
							}
						}
					}
				}

				mapper := simpleMapper("mapped")

				result := CursoredProducerMapperIterator(ctxWithCancel, Cursor{}, concurrency, intFromString, intToString, slowProducer, mapper)

				// For concurrency > 1, we expect context cancellation to eventually return the context error
				items, err := collectUntilError(result)

				if concurrency == 1 {
					// For concurrency=1, should only get the first item before cancellation
					require.NoError(t, err)
					require.Len(t, items, 1)
					require.Equal(t, 5, items[0].Item) // len("item1")
				} else {
					// For concurrency > 1, should get first item and then context canceled error
					require.Error(t, err)
					require.ErrorIs(t, err, context.Canceled)
					require.Len(t, items, 1)
					require.Equal(t, 5, items[0].Item) // len("item1")
				}
			})

			t.Run("context cancellation during chunk processing", func(t *testing.T) {
				ctxWithCancel, cancel := context.WithCancel(t.Context())
				t.Cleanup(cancel)

				slowProducer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
					return func(yield func(ChunkOrHold[[]string, int], error) bool) {
						// First chunk
						chunk1 := Chunk[[]string, int]{CurrentChunk: []string{"item1"}, CurrentChunkCursor: 1}
						if !yield(chunk1, nil) {
							return
						}

						// Give time for the first chunk to be processed and context to be cancelled
						time.Sleep(10 * time.Millisecond)

						// Check cancellation before second chunk
						select {
						case <-ctx.Done():
							return
						default:
							chunk2 := Chunk[[]string, int]{CurrentChunk: []string{"item2"}, CurrentChunkCursor: 2}
							if !yield(chunk2, nil) {
								return
							}
						}
					}
				}

				mapper := simpleMapper("mapped")

				result := CursoredProducerMapperIterator(ctxWithCancel, Cursor{}, concurrency, intFromString, intToString, slowProducer, mapper)

				var items []ItemAndCursor[int]
				var lastErr error
				for item, err := range result {
					if err != nil {
						lastErr = err
						break
					}
					items = append(items, item)

					// Cancel context after first item
					if len(items) == 1 {
						cancel()
					}
				}

				if concurrency == 1 {
					// For concurrency=1, should only get the first item before cancellation
					require.NoError(t, lastErr)
					require.Len(t, items, 1)
					require.Equal(t, 5, items[0].Item) // len("item1")
				} else {
					// For concurrency > 1, we should eventually get a context canceled error
					if lastErr != nil {
						require.ErrorIs(t, lastErr, context.Canceled)
					}
					// Should get at least the first item
					require.GreaterOrEqual(t, len(items), 1)
					if len(items) >= 1 {
						require.Equal(t, 5, items[0].Item) // len("item1")
					}
				}
			})

			t.Run("early termination with consumer stopping after error", func(t *testing.T) {
				chunks := []Chunk[[]string, int]{
					{CurrentChunk: []string{"item1", "item2"}, CurrentChunkCursor: 1},
				}

				producer := simpleProducer(chunks)

				mapperError := fmt.Errorf("mapper error")
				errorMapper := func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[int], error] {
					return func(yield func(ItemAndCursor[int], error) bool) {
						// Yield successful item first
						if !yield(ItemAndCursor[int]{Item: len(chunk[0]), Cursor: Cursor{"success"}}, nil) {
							return
						}
						// Yield error and expect consumer to stop
						if !yield(ItemAndCursor[int]{}, mapperError) {
							return
						}
					}
				}

				result := CursoredProducerMapperIterator(ctx, Cursor{}, concurrency, intFromString, intToString, producer, errorMapper)
				_, err := collectUntilError(result)
				require.Error(t, err)
				require.Contains(t, err.Error(), "mapper error")
			})

			t.Run("HoldForMappingComplete functionality", func(t *testing.T) {
				// Test that HoldForMappingComplete causes the producer to wait
				// for the mapper to complete its current chunk before continuing
				processingOrder := make([]string, 0)
				var mu sync.Mutex

				producer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
					return func(yield func(ChunkOrHold[[]string, int], error) bool) {
						// First yield a normal chunk
						chunk1 := Chunk[[]string, int]{CurrentChunk: []string{"item1"}, CurrentChunkCursor: 1}
						if !yield(chunk1, nil) {
							return
						}

						mu.Lock()
						processingOrder = append(processingOrder, "producer: yielded chunk1")
						mu.Unlock()

						// Then yield a hold signal
						hold := HoldForMappingComplete[[]string, int]{}
						if !yield(hold, nil) {
							return
						}

						mu.Lock()
						processingOrder = append(processingOrder, "producer: yielded hold")
						mu.Unlock()

						// Finally yield another chunk
						chunk2 := Chunk[[]string, int]{CurrentChunk: []string{"item2"}, CurrentChunkCursor: 2}
						if !yield(chunk2, nil) {
							return
						}

						mu.Lock()
						processingOrder = append(processingOrder, "producer: yielded chunk2")
						mu.Unlock()
					}
				}

				mapper := func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[int], error] {
					return func(yield func(ItemAndCursor[int], error) bool) {
						for i, item := range chunk {
							mu.Lock()
							processingOrder = append(processingOrder, fmt.Sprintf("mapper: processing %s", item))
							mu.Unlock()

							// Simulate some processing time
							time.Sleep(5 * time.Millisecond)

							cursorStr := fmt.Sprintf("mapped-%d", i)
							value := len(item)
							if !yield(ItemAndCursor[int]{Item: value, Cursor: Cursor{cursorStr}}, nil) {
								return
							}

							mu.Lock()
							processingOrder = append(processingOrder, fmt.Sprintf("mapper: completed %s", item))
							mu.Unlock()
						}
					}
				}

				result := CursoredProducerMapperIterator(ctx, Cursor{}, concurrency, intFromString, intToString, producer, mapper)
				items := collectNoError(t, result)

				// Verify the items were processed correctly
				require.Len(t, items, 2)
				require.Equal(t, 5, items[0].Item) // len("item1")
				require.Equal(t, 5, items[1].Item) // len("item2")

				// Verify the processing order shows that the hold functionality worked
				mu.Lock()
				order := make([]string, len(processingOrder))
				copy(order, processingOrder)
				mu.Unlock()

				require.Contains(t, order, "producer: yielded chunk1")
				require.Contains(t, order, "producer: yielded hold")
				require.Contains(t, order, "producer: yielded chunk2")
				require.Contains(t, order, "mapper: processing item1")
				require.Contains(t, order, "mapper: completed item1")
				require.Contains(t, order, "mapper: processing item2")
				require.Contains(t, order, "mapper: completed item2")
			})

			t.Run("producer receives remainingCursor", func(t *testing.T) {
				var capturedRemainingCursor Cursor

				producer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
					capturedRemainingCursor = remainingCursor
					return func(yield func(ChunkOrHold[[]string, int], error) bool) {
						chunk := Chunk[[]string, int]{CurrentChunk: []string{"test"}, CurrentChunkCursor: 1}
						if !yield(chunk, nil) {
							return
						}
					}
				}

				mapper := simpleMapper("mapped")

				// Test with a cursor that has additional parts beyond the head
				result := CursoredProducerMapperIterator(ctx, Cursor{"extra", "data", "0"}, concurrency, intFromString, intToString, producer, mapper)
				items := collectNoError(t, result)

				// Verify the producer received the remaining cursor
				require.Equal(t, Cursor{"extra", "data"}, capturedRemainingCursor)
				require.Len(t, items, 1)
				require.Equal(t, 4, items[0].Item) // len("test")
			})
		})
	}

	// Specific test for concurrency behavior
	t.Run("parallel execution behavior", func(t *testing.T) {
		// Helper function to create a simple producer that yields chunks
		simpleProducer := func(chunks []Chunk[[]string, int]) func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
			return func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
				return func(yield func(ChunkOrHold[[]string, int], error) bool) {
					for i := startIndex; i < len(chunks); i++ {
						select {
						case <-ctx.Done():
							return
						default:
							if !yield(chunks[i], nil) {
								return
							}
						}
					}
				}
			}
		}

		// Cursor converter functions for int
		intFromString := strconv.Atoi

		intToString := func(i int) (string, error) {
			return strconv.Itoa(i), nil
		}

		concurrencyLevels := []uint16{1, 2, 4}
		for _, concurrency := range concurrencyLevels {
			t.Run(fmt.Sprintf("concurrency %d", concurrency), func(t *testing.T) {
				// Track processing order with synchronized access
				var processingOrder []string
				var mu sync.Mutex

				chunks := []Chunk[[]string, int]{
					{CurrentChunk: []string{"chunk1-item1", "chunk1-item2"}, CurrentChunkCursor: 1},
					{CurrentChunk: []string{"chunk2-item1", "chunk2-item2"}, CurrentChunkCursor: 2},
					{CurrentChunk: []string{"chunk3-item1", "chunk3-item2"}, CurrentChunkCursor: 3},
					{CurrentChunk: []string{"chunk4-item1", "chunk4-item2"}, CurrentChunkCursor: 4},
				}

				producer := simpleProducer(chunks)

				mapper := func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[int], error] {
					return func(yield func(ItemAndCursor[int], error) bool) {
						chunkName := chunk[0][:6] // Get "chunk1", "chunk2", etc.

						mu.Lock()
						processingOrder = append(processingOrder, fmt.Sprintf("start-%s", chunkName))
						mu.Unlock()

						// Simulate some processing time
						time.Sleep(10 * time.Millisecond)

						for i, item := range chunk {
							cursorStr := fmt.Sprintf("mapped-%d", i)
							value := len(item)
							if !yield(ItemAndCursor[int]{Item: value, Cursor: Cursor{cursorStr}}, nil) {
								return
							}
						}

						mu.Lock()
						processingOrder = append(processingOrder, fmt.Sprintf("end-%s", chunkName))
						mu.Unlock()
					}
				}

				result := CursoredProducerMapperIterator(ctx, Cursor{}, concurrency, intFromString, intToString, producer, mapper)
				items := collectNoError(t, result)

				// Verify all items were processed
				require.Len(t, items, 8) // 4 chunks * 2 items each

				// Verify the processing order shows parallelism when concurrency > 1
				mu.Lock()
				order := make([]string, len(processingOrder))
				copy(order, processingOrder)
				mu.Unlock()

				// With concurrency > 1, we should see starts of multiple chunks before their ends
				if concurrency > 1 {
					startCount := 0
					endCount := 0
					foundParallelism := false

					for _, event := range order {
						if strings.HasPrefix(event, "start-") {
							startCount++
						} else if strings.HasPrefix(event, "end-") {
							endCount++
						}

						// If we have more starts than ends at any point, we have parallelism
						if startCount > endCount+1 {
							foundParallelism = true
							break
						}
					}

					if concurrency >= 2 && len(chunks) >= 2 {
						require.True(t, foundParallelism, "Expected to find evidence of parallel processing with concurrency %d", concurrency)
					}
				}

				// Verify all chunks were started and ended
				for i := 1; i <= len(chunks); i++ {
					chunkName := fmt.Sprintf("chunk%d", i)
					require.Contains(t, order, fmt.Sprintf("start-%s", chunkName))
					require.Contains(t, order, fmt.Sprintf("end-%s", chunkName))
				}
			})
		}
	})
}

func TestCursoredParallelIteratorsConcurrency1SpecialCase(t *testing.T) {
	ctx := t.Context()

	// Helper functions for creating test iterators
	simpleIterator := func(items []int, prefix string) Next[int] {
		return func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
			return func(yield func(ItemAndCursor[int], error) bool) {
				for i, item := range items {
					select {
					case <-ctx.Done():
						return
					default:
						cursorStr := fmt.Sprintf("%s-%d", prefix, i+1)
						if !yield(ItemAndCursor[int]{Item: item, Cursor: Cursor{cursorStr}}, nil) {
							return
						}
					}
				}
			}
		}
	}

	errorIterator := func(items []int, errorAt int, err error) Next[int] {
		return func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
			return func(yield func(ItemAndCursor[int], error) bool) {
				for i, item := range items {
					if i == errorAt {
						if !yield(ItemAndCursor[int]{}, err) {
							return
						}
						return
					}
					cursorStr := fmt.Sprintf("item-%d", i)
					if !yield(ItemAndCursor[int]{Item: item, Cursor: Cursor{cursorStr}}, nil) {
						return
					}
				}
			}
		}
	}

	t.Run("sequential execution with concurrency=1", func(t *testing.T) {
		executionOrder := make([]string, 0)
		var mu sync.Mutex

		// Create iterators that track execution order
		trackingIterator := func(items []int, name string) Next[int] {
			return func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
				return func(yield func(ItemAndCursor[int], error) bool) {
					mu.Lock()
					executionOrder = append(executionOrder, fmt.Sprintf("start-%s", name))
					mu.Unlock()

					for i, item := range items {
						cursorStr := fmt.Sprintf("%s-%d", name, i)
						if !yield(ItemAndCursor[int]{Item: item, Cursor: Cursor{cursorStr}}, nil) {
							return
						}
					}

					mu.Lock()
					executionOrder = append(executionOrder, fmt.Sprintf("end-%s", name))
					mu.Unlock()
				}
			}
		}

		iterators := []Next[int]{
			trackingIterator([]int{1, 2}, "iter1"),
			trackingIterator([]int{10, 20}, "iter2"),
			trackingIterator([]int{100}, "iter3"),
		}

		results := CursoredParallelIterators(ctx, Cursor{}, 1, iterators...)
		items := collectNoError(t, results)

		// Verify items are correct and in expected order
		expected := []ItemAndCursor[int]{
			{Item: 1, Cursor: Cursor{"iter1-0", "0"}},
			{Item: 2, Cursor: Cursor{"iter1-1", "0"}},
			{Item: 10, Cursor: Cursor{"iter2-0", "1"}},
			{Item: 20, Cursor: Cursor{"iter2-1", "1"}},
			{Item: 100, Cursor: Cursor{"iter3-0", "2"}},
		}
		require.Equal(t, expected, items)

		// Verify sequential execution order (no parallelism)
		mu.Lock()
		order := make([]string, len(executionOrder))
		copy(order, executionOrder)
		mu.Unlock()

		expectedOrder := []string{
			"start-iter1", "end-iter1",
			"start-iter2", "end-iter2",
			"start-iter3", "end-iter3",
		}
		require.Equal(t, expectedOrder, order)
	})

	t.Run("concurrency=1 handles errors correctly", func(t *testing.T) {
		iterators := []Next[int]{
			simpleIterator([]int{1, 2}, "iter1"),
			errorIterator([]int{10, 20}, 1, fmt.Errorf("test error")),
			simpleIterator([]int{100}, "iter3"), // Should not be reached
		}

		results := CursoredParallelIterators(ctx, Cursor{}, 1, iterators...)
		items, err := collectUntilError(results)

		// Should get items from iter1 and first item from iter2 before error
		require.Error(t, err)
		require.Contains(t, err.Error(), "test error")
		require.Len(t, items, 3)
		require.Equal(t, 1, items[0].Item)
		require.Equal(t, 2, items[1].Item)
		require.Equal(t, 10, items[2].Item)
	})

	t.Run("concurrency=1 respects context cancellation", func(t *testing.T) {
		ctxWithCancel, cancel := context.WithCancel(t.Context())

		cancellingIterator := func(items []int, name string) Next[int] {
			return func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
				return func(yield func(ItemAndCursor[int], error) bool) {
					for i, item := range items {
						if name == "iter2" && i == 1 {
							cancel() // Cancel after first item of iter2
						}

						select {
						case <-ctx.Done():
							return
						default:
							cursorStr := fmt.Sprintf("%s-%d", name, i)
							if !yield(ItemAndCursor[int]{Item: item, Cursor: Cursor{cursorStr}}, nil) {
								return
							}
						}
					}
				}
			}
		}

		iterators := []Next[int]{
			cancellingIterator([]int{1, 2}, "iter1"),
			cancellingIterator([]int{10, 20}, "iter2"),
			cancellingIterator([]int{100}, "iter3"), // Should not be reached
		}

		results := CursoredParallelIterators(ctxWithCancel, Cursor{}, 1, iterators...)
		items, err := collectUntilError(results)
		// Should detect context cancellation and return context error
		if err != nil {
			require.ErrorIs(t, err, context.Canceled)
		}
		// Should process some items before cancellation
		require.GreaterOrEqual(t, len(items), 1)
		if len(items) >= 1 {
			require.Equal(t, 1, items[0].Item)
		}
	})

	t.Run("concurrency=1 handles cursor correctly", func(t *testing.T) {
		var capturedCursors []Cursor

		cursorTrackingIterator := func(items []int, name string) Next[int] {
			return func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
				capturedCursors = append(capturedCursors, c)
				return func(yield func(ItemAndCursor[int], error) bool) {
					for i, item := range items {
						cursorStr := fmt.Sprintf("%s-%d", name, i)
						if !yield(ItemAndCursor[int]{Item: item, Cursor: Cursor{cursorStr}}, nil) {
							return
						}
					}
				}
			}
		}

		iterators := []Next[int]{
			cursorTrackingIterator([]int{1}, "iter1"),
			cursorTrackingIterator([]int{10}, "iter2"),
		}

		// Start from iterator 1, with remaining cursor
		results := CursoredParallelIterators(ctx, Cursor{"remaining", "data", "1"}, 1, iterators...)
		items := collectNoError(t, results)

		require.Len(t, items, 1)
		require.Equal(t, 10, items[0].Item)

		// Verify cursor handling: first iterator gets remaining cursor, subsequent get nil
		require.Len(t, capturedCursors, 1)
		require.Equal(t, Cursor{"remaining", "data"}, capturedCursors[0])
	})
}

func TestCursoredProducerMapperIteratorConcurrency1SpecialCase(t *testing.T) {
	ctx := t.Context()

	// Cursor converter functions for int
	intFromString := strconv.Atoi

	intToString := func(i int) (string, error) {
		return strconv.Itoa(i), nil
	}

	t.Run("sequential execution with concurrency=1", func(t *testing.T) {
		executionOrder := make([]string, 0)
		var mu sync.Mutex

		// Producer that yields chunks with tracking
		producer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
			return func(yield func(ChunkOrHold[[]string, int], error) bool) {
				chunks := []Chunk[[]string, int]{
					{CurrentChunk: []string{"chunk1-item1", "chunk1-item2"}, CurrentChunkCursor: 1},
					{CurrentChunk: []string{"chunk2-item1"}, CurrentChunkCursor: 2},
				}

				for i := startIndex; i < len(chunks); i++ {
					mu.Lock()
					executionOrder = append(executionOrder, fmt.Sprintf("producer-chunk%d", i+1))
					mu.Unlock()

					if !yield(chunks[i], nil) {
						return
					}
				}
			}
		}

		// Mapper that tracks execution
		mapper := func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[int], error] {
			return func(yield func(ItemAndCursor[int], error) bool) {
				chunkName := chunk[0][:6] // "chunk1" or "chunk2"

				mu.Lock()
				executionOrder = append(executionOrder, fmt.Sprintf("mapper-start-%s", chunkName))
				mu.Unlock()

				for i, item := range chunk {
					cursorStr := fmt.Sprintf("mapped-%d", i)
					value := len(item)
					if !yield(ItemAndCursor[int]{Item: value, Cursor: Cursor{cursorStr}}, nil) {
						return
					}
				}

				mu.Lock()
				executionOrder = append(executionOrder, fmt.Sprintf("mapper-end-%s", chunkName))
				mu.Unlock()
			}
		}

		result := CursoredProducerMapperIterator(ctx, Cursor{}, 1, intFromString, intToString, producer, mapper)
		items := collectNoError(t, result)

		// Verify items are correct
		require.Len(t, items, 3)
		require.Equal(t, 12, items[0].Item) // len("chunk1-item1")
		require.Equal(t, Cursor{"mapped-0", "1"}, items[0].Cursor)
		require.Equal(t, 12, items[1].Item) // len("chunk1-item2")
		require.Equal(t, Cursor{"mapped-1", "1"}, items[1].Cursor)
		require.Equal(t, 12, items[2].Item) // len("chunk2-item1")
		require.Equal(t, Cursor{"mapped-0", "2"}, items[2].Cursor)

		// Verify sequential execution order
		mu.Lock()
		order := make([]string, len(executionOrder))
		copy(order, executionOrder)
		mu.Unlock()

		expectedOrder := []string{
			"producer-chunk1", "mapper-start-chunk1", "mapper-end-chunk1",
			"producer-chunk2", "mapper-start-chunk2", "mapper-end-chunk2",
		}
		require.Equal(t, expectedOrder, order)
	})

	t.Run("concurrency=1 skips HoldForMappingComplete", func(t *testing.T) {
		// Producer that yields HoldForMappingComplete
		producer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
			return func(yield func(ChunkOrHold[[]string, int], error) bool) {
				chunk1 := Chunk[[]string, int]{CurrentChunk: []string{"item1"}, CurrentChunkCursor: 1}
				if !yield(chunk1, nil) {
					return
				}

				// This should be skipped in concurrency=1 mode
				hold := HoldForMappingComplete[[]string, int]{}
				if !yield(hold, nil) {
					return
				}

				chunk2 := Chunk[[]string, int]{CurrentChunk: []string{"item2"}, CurrentChunkCursor: 2}
				if !yield(chunk2, nil) {
					return
				}
			}
		}

		mapper := func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[int], error] {
			return func(yield func(ItemAndCursor[int], error) bool) {
				for i, item := range chunk {
					cursorStr := fmt.Sprintf("mapped-%d", i)
					value := len(item)
					if !yield(ItemAndCursor[int]{Item: value, Cursor: Cursor{cursorStr}}, nil) {
						return
					}
				}
			}
		}

		result := CursoredProducerMapperIterator(ctx, Cursor{}, 1, intFromString, intToString, producer, mapper)
		items := collectNoError(t, result)

		// Should get both chunks processed, HoldForMappingComplete should be skipped
		require.Len(t, items, 2)
		require.Equal(t, 5, items[0].Item) // len("item1")
		require.Equal(t, 5, items[1].Item) // len("item2")
	})

	t.Run("concurrency=1 handles errors correctly", func(t *testing.T) {
		producerError := fmt.Errorf("producer error")

		// Producer that yields one good chunk then an error
		producer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
			return func(yield func(ChunkOrHold[[]string, int], error) bool) {
				chunk1 := Chunk[[]string, int]{CurrentChunk: []string{"good"}, CurrentChunkCursor: 1}
				if !yield(chunk1, nil) {
					return
				}

				// Yield error
				if !yield(Chunk[[]string, int]{}, producerError) {
					return
				}
			}
		}

		mapper := func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[int], error] {
			return func(yield func(ItemAndCursor[int], error) bool) {
				for i, item := range chunk {
					cursorStr := fmt.Sprintf("mapped-%d", i)
					value := len(item)
					if !yield(ItemAndCursor[int]{Item: value, Cursor: Cursor{cursorStr}}, nil) {
						return
					}
				}
			}
		}

		result := CursoredProducerMapperIterator(ctx, Cursor{}, 1, intFromString, intToString, producer, mapper)
		items, err := collectUntilError(result)

		require.Error(t, err)
		require.Contains(t, err.Error(), "producer error")
		require.Len(t, items, 1)
		require.Equal(t, 4, items[0].Item) // len("good")
	})

	t.Run("concurrency=1 handles mapper errors correctly", func(t *testing.T) {
		producer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
			return func(yield func(ChunkOrHold[[]string, int], error) bool) {
				chunk := Chunk[[]string, int]{CurrentChunk: []string{"item1", "item2"}, CurrentChunkCursor: 1}
				if !yield(chunk, nil) {
					return
				}
			}
		}

		mapperError := fmt.Errorf("mapper error")
		mapper := func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[int], error] {
			return func(yield func(ItemAndCursor[int], error) bool) {
				// Process first item successfully
				if !yield(ItemAndCursor[int]{Item: len(chunk[0]), Cursor: Cursor{"mapped-0"}}, nil) {
					return
				}
				// Error on second item
				if !yield(ItemAndCursor[int]{}, mapperError) {
					return
				}
			}
		}

		result := CursoredProducerMapperIterator(ctx, Cursor{}, 1, intFromString, intToString, producer, mapper)
		items, err := collectUntilError(result)

		require.Error(t, err)
		require.Contains(t, err.Error(), "mapper error")
		require.Len(t, items, 1)
		require.Equal(t, 5, items[0].Item) // len("item1")
	})

	t.Run("concurrency=1 handles remaining cursor correctly", func(t *testing.T) {
		var capturedRemainingCursor Cursor

		producer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
			capturedRemainingCursor = remainingCursor
			return func(yield func(ChunkOrHold[[]string, int], error) bool) {
				chunk := Chunk[[]string, int]{CurrentChunk: []string{"test"}, CurrentChunkCursor: 1}
				if !yield(chunk, nil) {
					return
				}
			}
		}

		var mapperReceivedCursors []Cursor
		mapper := func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[int], error] {
			mapperReceivedCursors = append(mapperReceivedCursors, remainingCursor)
			return func(yield func(ItemAndCursor[int], error) bool) {
				if !yield(ItemAndCursor[int]{Item: len(chunk[0]), Cursor: Cursor{"mapped"}}, nil) {
					return
				}
			}
		}

		// Test with remaining cursor
		result := CursoredProducerMapperIterator(ctx, Cursor{"extra", "data", "0"}, 1, intFromString, intToString, producer, mapper)
		items := collectNoError(t, result)

		// Verify producer received remaining cursor
		require.Equal(t, Cursor{"extra", "data"}, capturedRemainingCursor)

		// Verify mapper received remaining cursor for first chunk only
		require.Len(t, mapperReceivedCursors, 1)
		require.Equal(t, Cursor{"extra", "data"}, mapperReceivedCursors[0])

		require.Len(t, items, 1)
		require.Equal(t, 4, items[0].Item) // len("test")
	})
}

// TestConcurrencyConsistency tests that the same input data produces the same output
// across different concurrency levels for blocks that support concurrency.
func TestConcurrencyConsistency(t *testing.T) {
	ctx := t.Context()

	// Shared concurrency levels to test across all functions
	concurrencyLevels := []uint16{0, 1, 2, 5, 10}

	t.Run("CursoredParallelIterators consistency", func(t *testing.T) {
		// Test data - same for all concurrency levels
		testCases := []struct {
			name      string
			cursor    Cursor
			iterators []Next[int]
			expected  []ItemAndCursor[int]
		}{
			{
				name:   "multiple iterators with different lengths",
				cursor: Cursor{},
				iterators: []Next[int]{
					simpleTestIterator([]int{1, 2, 3}, "iter1"),
					simpleTestIterator([]int{10, 20}, "iter2"),
					simpleTestIterator([]int{100, 101, 102, 103}, "iter3"),
				},
				expected: []ItemAndCursor[int]{
					{Item: 1, Cursor: Cursor{"iter1-1", "0"}},
					{Item: 2, Cursor: Cursor{"iter1-2", "0"}},
					{Item: 3, Cursor: Cursor{"iter1-3", "0"}},
					{Item: 10, Cursor: Cursor{"iter2-1", "1"}},
					{Item: 20, Cursor: Cursor{"iter2-2", "1"}},
					{Item: 100, Cursor: Cursor{"iter3-1", "2"}},
					{Item: 101, Cursor: Cursor{"iter3-2", "2"}},
					{Item: 102, Cursor: Cursor{"iter3-3", "2"}},
					{Item: 103, Cursor: Cursor{"iter3-4", "2"}},
				},
			},
			{
				name:   "with cursor starting index",
				cursor: Cursor{"remaining", "1"},
				iterators: []Next[int]{
					simpleTestIterator([]int{1, 2}, "iter1"),
					simpleTestIterator([]int{10, 20, 30}, "iter2"),
					simpleTestIterator([]int{100}, "iter3"),
				},
				expected: []ItemAndCursor[int]{
					{Item: 10, Cursor: Cursor{"iter2-1", "1"}},
					{Item: 20, Cursor: Cursor{"iter2-2", "1"}},
					{Item: 30, Cursor: Cursor{"iter2-3", "1"}},
					{Item: 100, Cursor: Cursor{"iter3-1", "2"}},
				},
			},
			{
				name:      "single iterator",
				cursor:    Cursor{},
				iterators: []Next[int]{simpleTestIterator([]int{42, 43, 44}, "single")},
				expected: []ItemAndCursor[int]{
					{Item: 42, Cursor: Cursor{"single-1", "0"}},
					{Item: 43, Cursor: Cursor{"single-2", "0"}},
					{Item: 44, Cursor: Cursor{"single-3", "0"}},
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Store results for each concurrency level
				results := make(map[uint16][]ItemAndCursor[int])

				for _, concurrency := range concurrencyLevels {
					t.Run(fmt.Sprintf("concurrency_%d", concurrency), func(t *testing.T) {
						actualConcurrency := concurrency
						if concurrency == 0 {
							actualConcurrency = 1 // 0 should behave like 1
						}

						result := CursoredParallelIterators(ctx, tc.cursor, actualConcurrency, tc.iterators...)
						items := collectNoError(t, result)
						results[concurrency] = items

						// Verify each result matches expected
						require.Len(t, items, len(tc.expected))
						for i, expectedItem := range tc.expected {
							require.Equal(t, expectedItem, items[i])
						}
					})
				}

				// Verify all concurrency levels produce identical results
				baseline := results[0]
				for concurrency := uint16(1); concurrency <= 10; concurrency++ {
					if _, exists := results[concurrency]; exists {
						require.Equal(t, baseline, results[concurrency],
							"Results differ between concurrency 0 and %d", concurrency)
					}
				}
			})
		}
	})

	t.Run("CursoredProducerMapperIterator consistency", func(t *testing.T) {
		// Cursor converter functions
		intFromString := strconv.Atoi
		intToString := func(i int) (string, error) {
			return strconv.Itoa(i), nil
		}

		testCases := []struct {
			name     string
			cursor   Cursor
			chunks   []Chunk[[]string, int]
			expected []ItemAndCursor[int]
		}{
			{
				name:   "multiple chunks",
				cursor: Cursor{},
				chunks: []Chunk[[]string, int]{
					{CurrentChunk: []string{"ab", "cd", "ef"}, CurrentChunkCursor: 1},
					{CurrentChunk: []string{"hello", "world"}, CurrentChunkCursor: 2},
					{CurrentChunk: []string{"test"}, CurrentChunkCursor: 3},
				},
				expected: []ItemAndCursor[int]{
					{Item: 2, Cursor: Cursor{"mapped-0", "1"}}, // len("ab")
					{Item: 2, Cursor: Cursor{"mapped-1", "1"}}, // len("cd")
					{Item: 2, Cursor: Cursor{"mapped-2", "1"}}, // len("ef")
					{Item: 5, Cursor: Cursor{"mapped-0", "2"}}, // len("hello")
					{Item: 5, Cursor: Cursor{"mapped-1", "2"}}, // len("world")
					{Item: 4, Cursor: Cursor{"mapped-0", "3"}}, // len("test")
				},
			},
			{
				name:   "with cursor starting index",
				cursor: Cursor{"remaining", "1"},
				chunks: []Chunk[[]string, int]{
					{CurrentChunk: []string{"skip"}, CurrentChunkCursor: 1},
					{CurrentChunk: []string{"process", "this"}, CurrentChunkCursor: 2},
					{CurrentChunk: []string{"and", "this", "too"}, CurrentChunkCursor: 3},
				},
				expected: []ItemAndCursor[int]{
					{Item: 7, Cursor: Cursor{"mapped-0", "2"}}, // len("process")
					{Item: 4, Cursor: Cursor{"mapped-1", "2"}}, // len("this")
					{Item: 3, Cursor: Cursor{"mapped-0", "3"}}, // len("and")
					{Item: 4, Cursor: Cursor{"mapped-1", "3"}}, // len("this")
					{Item: 3, Cursor: Cursor{"mapped-2", "3"}}, // len("too")
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Store results for each concurrency level
				results := make(map[uint16][]ItemAndCursor[int])

				for _, concurrency := range concurrencyLevels {
					t.Run(fmt.Sprintf("concurrency_%d", concurrency), func(t *testing.T) {
						actualConcurrency := concurrency
						if concurrency == 0 {
							actualConcurrency = 1 // 0 should behave like 1
						}

						// Create producer function for this test case
						producer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
							return func(yield func(ChunkOrHold[[]string, int], error) bool) {
								for i := startIndex; i < len(tc.chunks); i++ {
									if !yield(tc.chunks[i], nil) {
										return
									}
								}
							}
						}

						// Simple mapper that converts string length to int
						mapper := func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[int], error] {
							return func(yield func(ItemAndCursor[int], error) bool) {
								for i, item := range chunk {
									cursorStr := fmt.Sprintf("mapped-%d", i)
									value := len(item)
									if !yield(ItemAndCursor[int]{Item: value, Cursor: Cursor{cursorStr}}, nil) {
										return
									}
								}
							}
						}

						result := CursoredProducerMapperIterator(ctx, tc.cursor, actualConcurrency, intFromString, intToString, producer, mapper)
						items := collectNoError(t, result)
						results[concurrency] = items

						// Verify each result matches expected
						require.Len(t, items, len(tc.expected))
						for i, expectedItem := range tc.expected {
							require.Equal(t, expectedItem, items[i])
						}
					})
				}

				// Verify all concurrency levels produce identical results
				baseline := results[0]
				for concurrency := uint16(1); concurrency <= 10; concurrency++ {
					if _, exists := results[concurrency]; exists {
						require.Equal(t, baseline, results[concurrency],
							"Results differ between concurrency 0 and %d", concurrency)
					}
				}
			})
		}
	})
}

// Helper function for creating simple test iterators
func simpleTestIterator(items []int, prefix string) Next[int] {
	return func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
		return func(yield func(ItemAndCursor[int], error) bool) {
			for i, item := range items {
				select {
				case <-ctx.Done():
					return
				default:
					cursorStr := fmt.Sprintf("%s-%d", prefix, i+1)
					if !yield(ItemAndCursor[int]{Item: item, Cursor: Cursor{cursorStr}}, nil) {
						return
					}
				}
			}
		}
	}
}

func TestDisableCursorsInContext(t *testing.T) {
	ctx := t.Context()

	t.Run("CursoredWithIntegerHeader with cursors disabled", func(t *testing.T) {
		ctxWithoutCursors := DisableCursorsInContext(ctx)

		header := func(ctx context.Context, startIndex int) iter.Seq2[int, error] {
			return simpleIntSequence(startIndex, startIndex+3)
		}

		nextFunc := func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
			return func(yield func(ItemAndCursor[int], error) bool) {
				if !yield(ItemAndCursor[int]{Item: 100, Cursor: Cursor{"next-cursor"}}, nil) {
					return
				}
			}
		}

		result := CursoredWithIntegerHeader(ctxWithoutCursors, Cursor{}, header, nextFunc)
		items := collectNoError(t, result)

		require.Len(t, items, 4)

		// All cursors should be nil when disabled
		for _, item := range items {
			require.Nil(t, item.Cursor, "cursor should be nil when cursors are disabled")
		}

		// Items should still be present
		require.Equal(t, 0, items[0].Item)
		require.Equal(t, 1, items[1].Item)
		require.Equal(t, 2, items[2].Item)
		require.Equal(t, 100, items[3].Item)
	})

	t.Run("CursoredWithIntegerHeader with cursors enabled", func(t *testing.T) {
		header := func(ctx context.Context, startIndex int) iter.Seq2[int, error] {
			return simpleIntSequence(startIndex, startIndex+2)
		}

		nextFunc := Empty[int]

		result := CursoredWithIntegerHeader(ctx, Cursor{}, header, nextFunc)
		items := collectNoError(t, result)

		require.Len(t, items, 2)

		// All cursors should be non-nil when enabled
		for _, item := range items {
			require.NotNil(t, item.Cursor, "cursor should not be nil when cursors are enabled")
		}

		require.Equal(t, Cursor{"1"}, items[0].Cursor)
		require.Equal(t, Cursor{"2"}, items[1].Cursor)
	})

	t.Run("CursoredParallelIterators with cursors disabled", func(t *testing.T) {
		ctxWithoutCursors := DisableCursorsInContext(ctx)

		iterators := []Next[int]{
			simpleTestIterator([]int{1, 2}, "iter1"),
			simpleTestIterator([]int{10, 20}, "iter2"),
			simpleTestIterator([]int{100}, "iter3"),
		}

		result := CursoredParallelIterators(ctxWithoutCursors, Cursor{}, 2, iterators...)
		items := collectNoError(t, result)

		require.Len(t, items, 5)

		// All cursors should be nil when disabled
		for _, item := range items {
			require.Nil(t, item.Cursor, "cursor should be nil when cursors are disabled")
		}

		// Items should still be present in order
		require.Equal(t, 1, items[0].Item)
		require.Equal(t, 2, items[1].Item)
		require.Equal(t, 10, items[2].Item)
		require.Equal(t, 20, items[3].Item)
		require.Equal(t, 100, items[4].Item)
	})

	t.Run("CursoredParallelIterators with cursors enabled", func(t *testing.T) {
		iterators := []Next[int]{
			simpleTestIterator([]int{1, 2}, "iter1"),
			simpleTestIterator([]int{10}, "iter2"),
		}

		result := CursoredParallelIterators(ctx, Cursor{}, 2, iterators...)
		items := collectNoError(t, result)

		require.Len(t, items, 3)

		// All cursors should be non-nil when enabled
		for _, item := range items {
			require.NotNil(t, item.Cursor, "cursor should not be nil when cursors are enabled")
		}

		require.Equal(t, Cursor{"iter1-1", "0"}, items[0].Cursor)
		require.Equal(t, Cursor{"iter1-2", "0"}, items[1].Cursor)
		require.Equal(t, Cursor{"iter2-1", "1"}, items[2].Cursor)
	})

	t.Run("CursoredParallelIterators with cursors disabled concurrency=1", func(t *testing.T) {
		ctxWithoutCursors := DisableCursorsInContext(ctx)

		iterators := []Next[int]{
			simpleTestIterator([]int{1, 2}, "iter1"),
			simpleTestIterator([]int{10}, "iter2"),
		}

		result := CursoredParallelIterators(ctxWithoutCursors, Cursor{}, 1, iterators...)
		items := collectNoError(t, result)

		require.Len(t, items, 3)

		// All cursors should be nil when disabled
		for _, item := range items {
			require.Nil(t, item.Cursor, "cursor should be nil when cursors are disabled")
		}
	})

	t.Run("CursoredProducerMapperIterator with cursors disabled", func(t *testing.T) {
		ctxWithoutCursors := DisableCursorsInContext(ctx)

		intFromString := strconv.Atoi

		intToString := func(i int) (string, error) {
			return strconv.Itoa(i), nil
		}

		chunks := []Chunk[[]string, int]{
			{CurrentChunk: []string{"ab", "cd"}, CurrentChunkCursor: 1},
			{CurrentChunk: []string{"efg"}, CurrentChunkCursor: 2},
		}

		producer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
			return func(yield func(ChunkOrHold[[]string, int], error) bool) {
				for i := startIndex; i < len(chunks); i++ {
					if !yield(chunks[i], nil) {
						return
					}
				}
			}
		}

		mapper := func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[int], error] {
			return func(yield func(ItemAndCursor[int], error) bool) {
				for i, item := range chunk {
					cursorStr := fmt.Sprintf("mapped-%d", i)
					value := len(item)
					if !yield(ItemAndCursor[int]{Item: value, Cursor: Cursor{cursorStr}}, nil) {
						return
					}
				}
			}
		}

		result := CursoredProducerMapperIterator(ctxWithoutCursors, Cursor{}, 2, intFromString, intToString, producer, mapper)
		items := collectNoError(t, result)

		require.Len(t, items, 3)

		// All cursors should be nil when disabled
		for _, item := range items {
			require.Nil(t, item.Cursor, "cursor should be nil when cursors are disabled")
		}

		// Items should still be present
		require.Equal(t, 2, items[0].Item) // len("ab")
		require.Equal(t, 2, items[1].Item) // len("cd")
		require.Equal(t, 3, items[2].Item) // len("efg")
	})

	t.Run("CursoredProducerMapperIterator with cursors enabled", func(t *testing.T) {
		intFromString := strconv.Atoi

		intToString := func(i int) (string, error) {
			return strconv.Itoa(i), nil
		}

		chunks := []Chunk[[]string, int]{
			{CurrentChunk: []string{"ab"}, CurrentChunkCursor: 1},
		}

		producer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
			return func(yield func(ChunkOrHold[[]string, int], error) bool) {
				for i := startIndex; i < len(chunks); i++ {
					if !yield(chunks[i], nil) {
						return
					}
				}
			}
		}

		mapper := func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[int], error] {
			return func(yield func(ItemAndCursor[int], error) bool) {
				for i, item := range chunk {
					cursorStr := fmt.Sprintf("mapped-%d", i)
					value := len(item)
					if !yield(ItemAndCursor[int]{Item: value, Cursor: Cursor{cursorStr}}, nil) {
						return
					}
				}
			}
		}

		result := CursoredProducerMapperIterator(ctx, Cursor{}, 2, intFromString, intToString, producer, mapper)
		items := collectNoError(t, result)

		require.Len(t, items, 1)

		// Cursor should be non-nil when enabled
		require.NotNil(t, items[0].Cursor, "cursor should not be nil when cursors are enabled")
		require.Equal(t, Cursor{"mapped-0", "1"}, items[0].Cursor)
	})

	t.Run("CursoredProducerMapperIterator with cursors disabled concurrency=1", func(t *testing.T) {
		ctxWithoutCursors := DisableCursorsInContext(ctx)

		intFromString := strconv.Atoi

		intToString := func(i int) (string, error) {
			return strconv.Itoa(i), nil
		}

		chunks := []Chunk[[]string, int]{
			{CurrentChunk: []string{"test"}, CurrentChunkCursor: 1},
		}

		producer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
			return func(yield func(ChunkOrHold[[]string, int], error) bool) {
				for i := startIndex; i < len(chunks); i++ {
					if !yield(chunks[i], nil) {
						return
					}
				}
			}
		}

		mapper := func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[int], error] {
			return func(yield func(ItemAndCursor[int], error) bool) {
				for i, item := range chunk {
					cursorStr := fmt.Sprintf("mapped-%d", i)
					value := len(item)
					if !yield(ItemAndCursor[int]{Item: value, Cursor: Cursor{cursorStr}}, nil) {
						return
					}
				}
			}
		}

		result := CursoredProducerMapperIterator(ctxWithoutCursors, Cursor{}, 1, intFromString, intToString, producer, mapper)
		items := collectNoError(t, result)

		require.Len(t, items, 1)

		// Cursor should be nil when disabled
		require.Nil(t, items[0].Cursor, "cursor should be nil when cursors are disabled")
		require.Equal(t, 4, items[0].Item) // len("test")
	})

	t.Run("DisableCursorsInContext can be called multiple times", func(t *testing.T) {
		ctxWithoutCursors1 := DisableCursorsInContext(ctx)
		ctxWithoutCursors2 := DisableCursorsInContext(ctxWithoutCursors1)

		header := func(ctx context.Context, startIndex int) iter.Seq2[int, error] {
			return simpleIntSequence(startIndex, startIndex+1)
		}

		result := CursoredWithIntegerHeader(ctxWithoutCursors2, Cursor{}, header, Empty[int])
		items := collectNoError(t, result)

		require.Len(t, items, 1)
		require.Nil(t, items[0].Cursor, "cursor should be nil when cursors are disabled")
	})
}
