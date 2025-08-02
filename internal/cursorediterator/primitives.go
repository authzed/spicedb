package cursorediterator

import (
	"iter"
)

// join combines multiple iterator sequences into one.
func join[I any](iters ...iter.Seq2[I, error]) iter.Seq2[I, error] {
	return func(yield func(I, error) bool) {
		for _, it := range iters {
			canceled := false
			y := func(i I, err error) bool {
				if canceled {
					return false
				}
				if !yield(i, err) {
					canceled = true
					return false
				}
				if err != nil {
					canceled = true
					return false
				}
				return true
			}
			it(y)
			if canceled {
				break
			}
		}
	}
}

// YieldsError creates an iterator that yields a default value and an error.
func YieldsError[I any](err error) iter.Seq2[I, error] {
	return func(yield func(I, error) bool) {
		var defaultValue I
		if !yield(defaultValue, err) {
			return
		}
	}
}

// UncursoredEmpty is a function that returns an empty iterator sequence without a cursor.
func UncursoredEmpty[I any]() iter.Seq2[I, error] {
	return func(yield func(I, error) bool) {}
}

// CountingIterator wraps a Seq2 iterator and counts the number of items yielded,
// invoking the callback with the final count once the iterator completes.
func CountingIterator[I any](source iter.Seq2[I, error], callback func(int)) iter.Seq2[I, error] {
	return func(yield func(I, error) bool) {
		count := 0
		source(func(item I, err error) bool {
			if err == nil {
				count++
			}
			return yield(item, err)
		})
		callback(count)
	}
}
