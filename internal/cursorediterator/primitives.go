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

// yieldsError creates an iterator that yields a default value and an error.
func yieldsError[I any](err error) iter.Seq2[I, error] {
	return func(yield func(I, error) bool) {
		var defaultValue I
		if !yield(defaultValue, err) {
			return
		}
	}
}
