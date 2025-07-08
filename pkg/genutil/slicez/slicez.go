package slicez

// Filter iterates over elements of a slice, returning a new slice with all
// elements that the predicate returns truthy for.
func Filter[T any, Slice ~[]T](xs Slice, pred func(T) bool) Slice {
	ys := make(Slice, 0, len(xs))
	for _, x := range xs {
		if pred(x) {
			ys = append(ys, x)
		}
	}
	return ys
}

// Map iterates over a slice and creates a new slice with each element
// transformed.
func Map[T any, R any](xs []T, fn func(T) R) []R {
	ys := make([]R, len(xs))
	for i, x := range xs {
		ys[i] = fn(x)
	}
	return ys
}

// Unique returns a duplicate-free version of a slice, in which only the first
// occurrence of each element is kept.
//
// The order of result values is determined by the order they occur.
func Unique[T comparable, Slice ~[]T](xs Slice) Slice {
	ys := make(Slice, 0, len(xs))
	seen := make(map[T]struct{}, len(xs))
	for _, x := range xs {
		if _, ok := seen[x]; ok {
			continue
		}

		seen[x] = struct{}{}
		ys = append(ys, x)
	}
	return ys
}
