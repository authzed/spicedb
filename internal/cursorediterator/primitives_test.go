package cursorediterator

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJoin(t *testing.T) {
	t.Run("empty iterators", func(t *testing.T) {
		result := join[string]()

		items, errs := collectAll(result)

		require.Len(t, items, 0)
		require.Len(t, errs, 0)
	})

	t.Run("single iterator", func(t *testing.T) {
		iter1 := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return
			}
			if !yield("b", nil) {
				return
			}
		}

		result := join(iter1)

		items := collectNoError(t, result)

		require.Equal(t, []string{"a", "b"}, items)
	})

	t.Run("multiple iterators", func(t *testing.T) {
		iter1 := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return
			}
			if !yield("b", nil) {
				return
			}
		}

		iter2 := func(yield func(string, error) bool) {
			if !yield("c", nil) {
				return
			}
			if !yield("d", nil) {
				return
			}
		}

		iter3 := func(yield func(string, error) bool) {
			if !yield("e", nil) {
				return
			}
		}

		result := join(iter1, iter2, iter3)

		items := collectNoError(t, result)

		require.Equal(t, []string{"a", "b", "c", "d", "e"}, items)
	})

	t.Run("iterator with error stops processing", func(t *testing.T) {
		testError := errors.New("test error")

		iter1 := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return
			}
			if !yield("b", nil) {
				return
			}
		}

		iter2 := func(yield func(string, error) bool) {
			if !yield("c", nil) {
				return
			}
			if !yield("", testError) { // Error here
				return
			}
			if !yield("d", nil) { // Should not be reached
				return
			}
		}

		iter3 := func(yield func(string, error) bool) {
			if !yield("e", nil) { // Should not be reached
				return
			}
		}

		result := join(iter1, iter2, iter3)

		items, lastError := collectUntilError(result)

		require.Equal(t, []string{"a", "b", "c"}, items)
		require.Equal(t, testError, lastError)
	})

	t.Run("early termination by consumer", func(t *testing.T) {
		iter1 := func(yield func(int, error) bool) {
			if !yield(1, nil) {
				return
			}
			if !yield(2, nil) {
				return
			}
			if !yield(3, nil) { // Should not be reached due to early termination
				return
			}
		}

		iter2 := func(yield func(int, error) bool) {
			if !yield(4, nil) { // Should not be reached
				return
			}
		}

		result := join(iter1, iter2)

		items := collectFirst(t, result, 2)

		require.Equal(t, []int{1, 2}, items)
	})

	t.Run("empty iterator in sequence", func(t *testing.T) {
		iter1 := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return
			}
		}

		iter2 := func(yield func(string, error) bool) {
			// Empty iterator - yields nothing
		}

		iter3 := func(yield func(string, error) bool) {
			if !yield("b", nil) {
				return
			}
		}

		result := join(iter1, iter2, iter3)

		items := collectNoError(t, result)

		require.Equal(t, []string{"a", "b"}, items)
	})

	t.Run("error in first iterator", func(t *testing.T) {
		testError := errors.New("first iterator error")

		iter1 := func(yield func(string, error) bool) {
			if !yield("", testError) {
				return
			}
		}

		iter2 := func(yield func(string, error) bool) {
			if !yield("should not reach", nil) {
				return
			}
		}

		result := join(iter1, iter2)

		items, lastError := collectUntilError(result)

		require.Len(t, items, 0)
		require.Equal(t, testError, lastError)
	})

	t.Run("consumer returns false immediately", func(t *testing.T) {
		iter1 := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return // Consumer returned false, stop yielding
			}
			if !yield("should not reach", nil) {
				return
			}
		}

		iter2 := func(yield func(string, error) bool) {
			if !yield("should not reach", nil) {
				return
			}
		}

		result := join(iter1, iter2)

		// Consumer that immediately returns false
		result(func(item string, err error) bool {
			require.Equal(t, "a", item)
			require.NoError(t, err)
			return false
		})
	})

	t.Run("different types", func(t *testing.T) {
		iter1 := func(yield func(int, error) bool) {
			if !yield(1, nil) {
				return
			}
			if !yield(2, nil) {
				return
			}
		}

		iter2 := func(yield func(int, error) bool) {
			if !yield(3, nil) {
				return
			}
			if !yield(4, nil) {
				return
			}
		}

		result := join(iter1, iter2)

		items := collectNoError(t, result)

		require.Equal(t, []int{1, 2, 3, 4}, items)
	})
}

func TestYieldsError(t *testing.T) {
	t.Run("yields error with default value", func(t *testing.T) {
		testError := errors.New("test error")
		result := yieldsError[string](testError)

		items, errs := collectAll(result)

		require.Len(t, items, 1)
		require.Equal(t, "", items[0]) // Default value for string
		require.Len(t, errs, 1)
		require.Equal(t, testError, errs[0])
	})

	t.Run("yields error with int type", func(t *testing.T) {
		testError := errors.New("integer error")
		result := yieldsError[int](testError)

		items, errs := collectAll(result)

		require.Len(t, items, 1)
		require.Equal(t, 0, items[0]) // Default value for int
		require.Len(t, errs, 1)
		require.Equal(t, testError, errs[0])
	})

	t.Run("consumer returns false immediately", func(t *testing.T) {
		testError := errors.New("test error")
		result := yieldsError[string](testError)

		called := false
		result(func(item string, err error) bool {
			require.False(t, called, "yield should only be called once")
			called = true
			require.Equal(t, "", item)
			require.Equal(t, testError, err)
			return false
		})

		require.True(t, called, "yield should have been called")
	})
}
