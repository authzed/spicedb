package util

import (
	"golang.org/x/exp/maps"
)

func NewMultiMap[T comparable, Q any]() *MultiMap[T, Q] {
	return &MultiMap[T, Q]{
		items: map[T][]Q{},
	}
}

type MultiMap[T comparable, Q any] struct {
	items map[T][]Q
}

func (mm *MultiMap[T, Q]) Clear() {
	mm.items = map[T][]Q{}
}

func (mm *MultiMap[T, Q]) Add(key T, item Q) {
	if _, ok := mm.items[key]; !ok {
		mm.items[key] = []Q{}
	}

	mm.items[key] = append(mm.items[key], item)
}

func (mm *MultiMap[T, Q]) RemoveKey(key T) {
	delete(mm.items, key)
}

func (mm *MultiMap[T, Q]) Has(key T) bool {
	_, ok := mm.items[key]
	return ok
}

func (mm *MultiMap[T, Q]) Get(key T) ([]Q, bool) {
	found, ok := mm.items[key]
	if !ok {
		return []Q{}, false
	}

	return found, true
}

func (mm *MultiMap[T, Q]) IsEmpty() bool {
	return len(mm.items) == 0
}

func (mm *MultiMap[T, Q]) Len() int {
	return len(mm.items)
}

func (mm *MultiMap[T, Q]) Keys() []T {
	return maps.Keys(mm.items)
}
