package cursorediterator

import (
	"context"
	"fmt"
	"iter"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCursoredWithIntegerHeader(t *testing.T) {
	ctx := context.Background()

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

		result := CursoredWithIntegerHeader(ctx, Cursor{"5", "remaining"}, header, nextFunc)
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

		result := CursoredWithIntegerHeader(ctx, Cursor{"-1", "remaining", "cursor"}, header, nextFunc)
		items := collectNoError(t, result)

		require.False(t, headerCalled)
		require.True(t, nextCalled)
		require.Equal(t, Cursor{"remaining", "cursor"}, capturedRemainingCursor)
		require.Len(t, items, 1)

		// Next items should have Cursor prefixed with -1
		require.Equal(t, 100, items[0].Item)
		require.Equal(t, Cursor{"-1", "next-cursor"}, items[0].Cursor)
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

		result := CursoredWithIntegerHeader(ctx, Cursor{"3", "remaining"}, header, nextFunc)
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
		require.Equal(t, Cursor{"-1", "next1"}, items[2].Cursor)
		require.Equal(t, 201, items[3].Item)
		require.Equal(t, Cursor{"-1", "next2"}, items[3].Cursor)
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

	t.Run("header with range loop processes all items despite errors", func(t *testing.T) {
		// Note: The implementation uses a range loop over the header iterator
		// which means errors are discarded. This test documents current behavior.
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
		items := collectNoError(t, result)

		// Range loop discards errors, so all items are processed
		require.Len(t, items, 3)
		require.Equal(t, 0, items[0].Item)
		require.Equal(t, 1, items[1].Item) // error was discarded
		require.Equal(t, 2, items[2].Item)
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

		result := CursoredWithIntegerHeader(ctx, Cursor{"5", "extra", "data"}, header, nextFunc)
		items := collectNoError(t, result)

		require.Equal(t, Cursor{"extra", "data"}, capturedCursor)
		require.Len(t, items, 2)

		// Header item
		require.Equal(t, 5, items[0].Item)
		require.Equal(t, Cursor{"6"}, items[0].Cursor)

		// Next item with -1 prefix
		require.Equal(t, 100, items[1].Item)
		require.Equal(t, Cursor{"-1", "custom"}, items[1].Cursor)
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
						{Item: 1, Cursor: Cursor{"0", "iter1-1"}},
						{Item: 2, Cursor: Cursor{"0", "iter1-2"}},
						{Item: 10, Cursor: Cursor{"1", "iter2-1"}},
						{Item: 100, Cursor: Cursor{"2", "iter3-1"}},
						{Item: 101, Cursor: Cursor{"2", "iter3-2"}},
						{Item: 102, Cursor: Cursor{"2", "iter3-3"}},
					},
				},
				{
					name:   "cursor with starting index skips earlier iterators",
					cursor: Cursor{"1", "remaining"},
					iterators: []Next[int]{
						simpleIterator([]int{1}, "iter1"),
						simpleIterator([]int{10}, "iter2"),
						simpleIterator([]int{100}, "iter3"),
					},
					expected: []ItemAndCursor[int]{
						{Item: 10, Cursor: Cursor{"1", "iter2-1"}},
						{Item: 100, Cursor: Cursor{"2", "iter3-1"}},
					},
				},
				{
					name:   "multiple result Cursor with starting index skips earlier iterators",
					cursor: Cursor{"1", "remaining"},
					iterators: []Next[int]{
						simpleIterator([]int{1, 2, 3, 4, 5}, "iter1"),
						simpleIterator([]int{10, 20, 30, 40}, "iter2"),
						simpleIterator([]int{100, 101, 102}, "iter3"),
					},
					expected: []ItemAndCursor[int]{
						{Item: 10, Cursor: Cursor{"1", "iter2-1"}},
						{Item: 20, Cursor: Cursor{"1", "iter2-2"}},
						{Item: 30, Cursor: Cursor{"1", "iter2-3"}},
						{Item: 40, Cursor: Cursor{"1", "iter2-4"}},
						{Item: 100, Cursor: Cursor{"2", "iter3-1"}},
						{Item: 101, Cursor: Cursor{"2", "iter3-2"}},
						{Item: 102, Cursor: Cursor{"2", "iter3-3"}},
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
					expected:      []ItemAndCursor[int]{{Item: 1, Cursor: Cursor{"0", "item-0"}}},
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
					expected:  []ItemAndCursor[int]{{Item: 42, Cursor: Cursor{"0", "single-1"}}},
				},
			}

			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					ctx := context.Background()
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
		ctx := context.Background()
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
			require.Equal(t, Cursor{"0", "iter1-done"}, items[0].Cursor)
		})
	})
}

func TestCursoredProducerMapperIterator(t *testing.T) {
	ctx := context.Background()

	// Helper function to create a simple producer that yields chunks
	simpleProducer := func(chunks []ChunkAndFollowCursor[string, int]) func(ctx context.Context, startIndex int) iter.Seq2[ChunkAndFollowCursor[string, int], error] {
		return func(ctx context.Context, startIndex int) iter.Seq2[ChunkAndFollowCursor[string, int], error] {
			return func(yield func(ChunkAndFollowCursor[string, int], error) bool) {
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
	intFromString := func(s string) (int, error) {
		return strconv.Atoi(s)
	}

	intToString := func(i int) string {
		return strconv.Itoa(i)
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
		chunks := []ChunkAndFollowCursor[string, int]{
			{Chunk: []string{"ab", "cd"}, Follow: 1},
			{Chunk: []string{"efg"}, Follow: 2},
		}

		producer := simpleProducer(chunks)
		mapper := simpleMapper("mapped")

		result := CursoredProducerMapperIterator(ctx, Cursor{}, intFromString, intToString, producer, mapper)
		items := collectNoError(t, result)

		require.Len(t, items, 3)
		require.Equal(t, 2, items[0].Item) // len("ab")
		require.Equal(t, Cursor{"1", "mapped-0"}, items[0].Cursor)
		require.Equal(t, 2, items[1].Item) // len("cd")
		require.Equal(t, Cursor{"1", "mapped-1"}, items[1].Cursor)
		require.Equal(t, 3, items[2].Item) // len("efg")
		require.Equal(t, Cursor{"2", "mapped-0"}, items[2].Cursor)
	})

	t.Run("cursor with head value starts from correct position", func(t *testing.T) {
		chunks := []ChunkAndFollowCursor[string, int]{
			{Chunk: []string{"ab", "cd"}, Follow: 1},
			{Chunk: []string{"efg"}, Follow: 2},
			{Chunk: []string{"hi"}, Follow: 3},
		}

		producer := simpleProducer(chunks)
		mapper := simpleMapper("mapped")

		result := CursoredProducerMapperIterator(ctx, Cursor{"1", "remaining"}, intFromString, intToString, producer, mapper)
		items := collectNoError(t, result)

		require.Len(t, items, 2)
		require.Equal(t, 3, items[0].Item) // len("efg")
		require.Equal(t, Cursor{"2", "mapped-0"}, items[0].Cursor)
		require.Equal(t, 2, items[1].Item) // len("hi")
		require.Equal(t, Cursor{"3", "mapped-0"}, items[1].Cursor)
	})

	t.Run("invalid Cursor head returns error", func(t *testing.T) {
		chunks := []ChunkAndFollowCursor[string, int]{
			{Chunk: []string{"item"}, Follow: 1},
		}

		producer := simpleProducer(chunks)
		mapper := simpleMapper("mapped")

		result := CursoredProducerMapperIterator(ctx, Cursor{"invalid-int"}, intFromString, intToString, producer, mapper)
		items, errs := collectAll(result)

		require.Len(t, items, 1) // yieldsError yields one default value
		require.Len(t, errs, 1)
		require.Error(t, errs[0])
		require.Contains(t, errs[0].Error(), "invalid syntax")
	})

	t.Run("empty producer yields no items", func(t *testing.T) {
		emptyProducer := func(ctx context.Context, startIndex int) iter.Seq2[ChunkAndFollowCursor[string, int], error] {
			return func(yield func(ChunkAndFollowCursor[string, int], error) bool) {
				// Yield nothing
			}
		}

		mapper := simpleMapper("mapped")

		result := CursoredProducerMapperIterator(ctx, Cursor{}, intFromString, intToString, emptyProducer, mapper)
		items := collectNoError(t, result)

		require.Len(t, items, 0)
	})
}
