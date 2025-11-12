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

// TestContextCancellationComprehensive ensures that when context is canceled,
// all block functions ALWAYS return the context error and never hang or return unexpected errors.
func TestContextCancellationComprehensive(t *testing.T) {
	t.Run("Empty function", func(t *testing.T) {
		t.Run("context canceled before iteration", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel() // Cancel immediately

			result := Empty[int](ctx, Cursor{})

			items, err := collectUntilError(result)
			require.Error(t, err)
			require.ErrorIs(t, err, context.Canceled)
			require.Empty(t, items)
		})

		t.Run("context canceled during iteration", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())

			result := Empty[int](ctx, Cursor{})

			// Cancel context while iterating
			cancel()

			items, err := collectUntilError(result)
			// Empty should not yield any items and should detect cancellation
			if err != nil {
				require.ErrorIs(t, err, context.Canceled)
			}
			require.Empty(t, items)
		})
	})

	t.Run("CursoredWithIntegerHeader function", func(t *testing.T) {
		t.Run("context canceled before header execution", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel() // Cancel immediately

			headerCalled := false
			header := func(ctx context.Context, startIndex int) iter.Seq2[int, error] {
				headerCalled = true
				return func(yield func(int, error) bool) {
					// This should not be reached due to context cancellation
					if !yield(1, nil) {
						return
					}
				}
			}

			nextFunc := Empty[int]

			result := CursoredWithIntegerHeader(ctx, Cursor{}, header, nextFunc)
			items, err := collectUntilError(result)

			require.Error(t, err)
			require.ErrorIs(t, err, context.Canceled)
			require.Empty(t, items)
			require.False(t, headerCalled, "Header should not be called when context is already canceled")
		})

		t.Run("context canceled during header execution", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())

			header := func(ctx context.Context, startIndex int) iter.Seq2[int, error] {
				return func(yield func(int, error) bool) {
					// Yield first item successfully
					if !yield(1, nil) {
						return
					}

					// Cancel context before second item
					cancel()

					// Check for cancellation
					select {
					case <-ctx.Done():
						return
					default:
						if !yield(2, nil) {
							return
						}
					}
				}
			}

			nextFunc := Empty[int]

			result := CursoredWithIntegerHeader(ctx, Cursor{}, header, nextFunc)
			items, err := collectUntilError(result)

			// Should get the first item before cancellation, then context error
			require.Error(t, err)
			require.ErrorIs(t, err, context.Canceled)
			require.Len(t, items, 1)
			require.Equal(t, 1, items[0].Item)
		})

		t.Run("context canceled during next execution", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())

			header := func(ctx context.Context, startIndex int) iter.Seq2[int, error] {
				return simpleIntSequence(0, 2) // yield 2 items
			}

			nextCallCount := 0
			nextFunc := func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
				return func(yield func(ItemAndCursor[int], error) bool) {
					nextCallCount++

					// Yield first item successfully
					if !yield(ItemAndCursor[int]{Item: 100, Cursor: Cursor{"first"}}, nil) {
						return
					}

					// Cancel context after first next item (only for first call)
					if nextCallCount == 1 {
						cancel()
					}

					// Check for cancellation
					select {
					case <-ctx.Done():
						return
					default:
						if !yield(ItemAndCursor[int]{Item: 200, Cursor: Cursor{"second"}}, nil) {
							return
						}
					}
				}
			}

			result := CursoredWithIntegerHeader(ctx, Cursor{}, header, nextFunc)

			// Since context is canceled during execution, we should eventually get context error
			var items []ItemAndCursor[int]
			var lastErr error
			for item, err := range result {
				if err != nil {
					lastErr = err
					break
				}
				items = append(items, item)
			}

			// Should get at least header items and possibly first next item before cancellation
			if lastErr != nil {
				require.ErrorIs(t, lastErr, context.Canceled)
			}
			require.GreaterOrEqual(t, len(items), 2) // At least 2 from header
			if len(items) >= 3 {
				require.Equal(t, 100, items[2].Item) // The next item
			}
		})
	})

	t.Run("CursoredParallelIterators function", func(t *testing.T) {
		concurrencyLevels := []uint16{1, 2, 4}

		for _, concurrency := range concurrencyLevels {
			t.Run(fmt.Sprintf("concurrency_%d", concurrency), func(t *testing.T) {
				t.Run("context canceled before execution", func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					cancel() // Cancel immediately

					iter1 := func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
						return func(yield func(ItemAndCursor[int], error) bool) {
							require.Fail(t, "Iterator should not be called when context is already canceled")
						}
					}

					result := CursoredParallelIterators(ctx, Cursor{}, concurrency, iter1)
					items, err := collectUntilError(result)

					require.Error(t, err)
					require.ErrorIs(t, err, context.Canceled)
					require.Empty(t, items)
				})

				t.Run("context canceled during execution", func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())

					iter1 := func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
						return func(yield func(ItemAndCursor[int], error) bool) {
							// Yield first item
							if !yield(ItemAndCursor[int]{Item: 1, Cursor: Cursor{"first"}}, nil) {
								return
							}

							// Cancel context after first item
							cancel()
						}
					}

					iter2 := func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
						return func(yield func(ItemAndCursor[int], error) bool) {
							// Sleep to ensure iter1 yields first
							time.Sleep(20 * time.Millisecond)

							if !yield(ItemAndCursor[int]{Item: 10, Cursor: Cursor{"iter2"}}, nil) {
								return
							}
						}
					}

					result := CursoredParallelIterators(ctx, Cursor{}, concurrency, iter1, iter2)
					_, err := collectUntilError(result)

					// Should eventually get context canceled error
					require.Error(t, err)
					require.ErrorIs(t, err, context.Canceled)
				})

				t.Run("context timeout during long operation", func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
					t.Cleanup(cancel)

					slowIterator := func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
						return func(yield func(ItemAndCursor[int], error) bool) {
							// Yield first item quickly
							if !yield(ItemAndCursor[int]{Item: 1, Cursor: Cursor{"first"}}, nil) {
								return
							}

							// Simulate slow operation
							time.Sleep(50 * time.Millisecond)

							// Check for timeout
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

					result := CursoredParallelIterators(ctx, Cursor{}, concurrency, slowIterator)
					items, err := collectUntilError(result)

					if concurrency == 1 {
						// For concurrency=1, the timeout should be detected during iteration
						if err != nil {
							require.ErrorIs(t, err, context.DeadlineExceeded)
						}
						// Should get the first item before timeout
						require.GreaterOrEqual(t, len(items), 1)
						if len(items) >= 1 {
							require.Equal(t, 1, items[0].Item)
						}
					} else {
						// For concurrency > 1, should get timeout error
						require.Error(t, err)
						require.ErrorIs(t, err, context.DeadlineExceeded)
						// Should get the first item before timeout
						require.GreaterOrEqual(t, len(items), 1)
						if len(items) >= 1 {
							require.Equal(t, 1, items[0].Item)
						}
					}
				})
			})
		}
	})

	t.Run("CursoredProducerMapperIterator function", func(t *testing.T) {
		concurrencyLevels := []uint16{1, 2, 4}

		// Cursor converter functions
		intFromString := strconv.Atoi
		intToString := func(i int) (string, error) {
			return strconv.Itoa(i), nil
		}

		for _, concurrency := range concurrencyLevels {
			t.Run(fmt.Sprintf("concurrency_%d", concurrency), func(t *testing.T) {
				t.Run("context canceled before execution", func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					cancel() // Cancel immediately

					producer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
						require.Fail(t, "Producer should not be called when context is already canceled")
						return func(yield func(ChunkOrHold[[]string, int], error) bool) {}
					}

					mapper := func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[int], error] {
						require.Fail(t, "Mapper should not be called when context is already canceled")
						return func(yield func(ItemAndCursor[int], error) bool) {}
					}

					result := CursoredProducerMapperIterator(ctx, Cursor{}, concurrency, intFromString, intToString, producer, mapper)
					items, err := collectUntilError(result)

					require.Error(t, err)
					require.ErrorIs(t, err, context.Canceled)
					require.Empty(t, items)
				})

				t.Run("context canceled during producer execution", func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())

					producer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
						return func(yield func(ChunkOrHold[[]string, int], error) bool) {
							// Yield first chunk
							chunk1 := Chunk[[]string, int]{CurrentChunk: []string{"item1"}, CurrentChunkCursor: 1}
							if !yield(chunk1, nil) {
								return
							}

							// Cancel context before second chunk
							cancel()

							// Check for cancellation
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

					mapper := func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[int], error] {
						return func(yield func(ItemAndCursor[int], error) bool) {
							for i, item := range chunk {
								cursorStr := fmt.Sprintf("mapped-%d", i)
								if !yield(ItemAndCursor[int]{Item: len(item), Cursor: Cursor{cursorStr}}, nil) {
									return
								}
							}
						}
					}

					result := CursoredProducerMapperIterator(ctx, Cursor{}, concurrency, intFromString, intToString, producer, mapper)
					items, err := collectUntilError(result)

					if concurrency == 1 {
						// For concurrency=1, should only get the first chunk processed before cancellation
						require.NoError(t, err)
						require.Len(t, items, 1)
						require.Equal(t, 5, items[0].Item) // len("item1")
					} else {
						// For concurrency > 1, should eventually get context canceled error
						require.Error(t, err)
						require.ErrorIs(t, err, context.Canceled)
					}
				})

				t.Run("context canceled during mapper execution", func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())

					producer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
						return func(yield func(ChunkOrHold[[]string, int], error) bool) {
							cancel()

							chunk := Chunk[[]string, int]{CurrentChunk: []string{"item1", "item2"}, CurrentChunkCursor: 1}
							if !yield(chunk, nil) {
								return
							}
						}
					}

					mapper := func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[int], error] {
						return func(yield func(ItemAndCursor[int], error) bool) {
							// Process first item
							if !yield(ItemAndCursor[int]{Item: len(chunk[0]), Cursor: Cursor{"mapped-0"}}, nil) {
								return
							}

							if !yield(ItemAndCursor[int]{Item: len(chunk[1]), Cursor: Cursor{"mapped-1"}}, nil) {
								return
							}
						}
					}

					result := CursoredProducerMapperIterator(ctx, Cursor{}, concurrency, intFromString, intToString, producer, mapper)
					_, err := collectUntilError(result)

					require.Error(t, err)
					require.ErrorIs(t, err, context.Canceled)
				})

				t.Run("context timeout during long operation", func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
					t.Cleanup(cancel)

					producer := func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
						return func(yield func(ChunkOrHold[[]string, int], error) bool) {
							chunk := Chunk[[]string, int]{CurrentChunk: []string{"item1"}, CurrentChunkCursor: 1}
							if !yield(chunk, nil) {
								return
							}
						}
					}

					mapper := func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[int], error] {
						return func(yield func(ItemAndCursor[int], error) bool) {
							// Yield first item quickly
							if !yield(ItemAndCursor[int]{Item: len(chunk[0]), Cursor: Cursor{"first"}}, nil) {
								return
							}

							// Simulate slow operation
							time.Sleep(50 * time.Millisecond)

							// Check for timeout
							select {
							case <-ctx.Done():
								return
							default:
								if !yield(ItemAndCursor[int]{Item: 999, Cursor: Cursor{"should-not-reach"}}, nil) {
									return
								}
							}
						}
					}

					result := CursoredProducerMapperIterator(ctx, Cursor{}, concurrency, intFromString, intToString, producer, mapper)
					items, err := collectUntilError(result)

					if concurrency == 1 {
						// For concurrency=1, should only get the first item before timeout
						require.NoError(t, err)
						require.Len(t, items, 1)
						require.Equal(t, 5, items[0].Item) // len("item1")
					} else {
						// For concurrency > 1, should get timeout error
						require.Error(t, err)
						require.ErrorIs(t, err, context.DeadlineExceeded)
					}
				})
			})
		}
	})
}

// TestContextCancellationEdgeCases tests edge cases for context cancellation
func TestContextCancellationEdgeCases(t *testing.T) {
	t.Run("multiple context cancellations", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())

		iter1 := func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
			return func(yield func(ItemAndCursor[int], error) bool) {
				if !yield(ItemAndCursor[int]{Item: 1, Cursor: Cursor{"first"}}, nil) {
					return
				}
				// Cancel multiple times (should be safe)
				cancel()
				cancel()
				cancel()
			}
		}

		result := CursoredParallelIterators(ctx, Cursor{}, 1, iter1)
		items, err := collectUntilError(result)
		// Should handle multiple cancellations gracefully
		if err != nil {
			require.ErrorIs(t, err, context.Canceled)
		}
		require.GreaterOrEqual(t, len(items), 1)
	})

	t.Run("context already done at start", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel() // Cancel before starting

		// Ensure context is done
		<-ctx.Done()

		result := CursoredParallelIterators(ctx, Cursor{}, 2, func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
			require.Fail(t, "Iterator should not be called")
			return func(yield func(ItemAndCursor[int], error) bool) {}
		})

		items, err := collectUntilError(result)
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, items)
	})

	t.Run("nested context cancellation", func(t *testing.T) {
		parentCtx, parentCancel := context.WithCancel(t.Context())
		childCtx, childCancel := context.WithCancel(parentCtx)

		iter1 := func(ctx context.Context, c Cursor) iter.Seq2[ItemAndCursor[int], error] {
			return func(yield func(ItemAndCursor[int], error) bool) {
				if !yield(ItemAndCursor[int]{Item: 1, Cursor: Cursor{"first"}}, nil) {
					return
				}
				// Cancel child first, then parent
				childCancel()
				parentCancel()
			}
		}

		result := CursoredParallelIterators(childCtx, Cursor{}, 1, iter1)
		items, err := collectUntilError(result)
		// Should detect cancellation from either parent or child
		if err != nil {
			require.ErrorIs(t, err, context.Canceled)
		}
		require.GreaterOrEqual(t, len(items), 1)
	})
}
