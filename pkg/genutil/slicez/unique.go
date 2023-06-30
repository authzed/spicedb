package slicez

// Unique returns the items slice, but with duplicate items removed.
func Unique[T comparable](items []T) []T {
	updated := make([]T, 0, len(items))
	encountered := make(map[T]struct{}, len(items))
	for _, item := range items {
		if _, ok := encountered[item]; ok {
			continue
		}

		updated = append(updated, item)
		encountered[item] = struct{}{}
	}
	return updated
}
