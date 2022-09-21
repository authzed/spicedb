package util

func NewMultiMap[T comparable, Q any]() *MultiMap[T, Q] {
	return &MultiMap[T, Q]{
		items: map[T][]Q{},
	}
}

type MultiMap[T comparable, Q any] struct {
	items map[T][]Q
}

func (mm *MultiMap[T, Q]) Add(key T, item Q) {
	if _, ok := mm.items[key]; !ok {
		mm.items[key] = []Q{}
	}

	mm.items[key] = append(mm.items[key], item)
}

func (mm *MultiMap[T, Q]) Get(key T) []Q {
	found, ok := mm.items[key]
	if !ok {
		return []Q{}
	}

	return found
}
