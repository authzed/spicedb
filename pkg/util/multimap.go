package util

import (
	"golang.org/x/exp/maps"
)

// ReadOnlyMultimap is a read-only version of the multimap.
type ReadOnlyMultimap[T comparable, Q any] interface {
	// Has returns true if the key is found in the map.
	Has(key T) bool

	// Get returns the values for the given key in the map and whether the key existed. If the key
	// does not exist, an empty slice is returned.
	Get(key T) ([]Q, bool)

	// IsEmpty returns true if the map is currently empty.
	IsEmpty() bool

	// Len returns the length of the map, e.g. the number of *keys* present.
	Len() int

	// Keys returns the keys of the map.
	Keys() []T

	// Values returns all values in the map.
	Values() []Q
}

// NewMultiMap creates and returns a new MultiMap from keys of type T to values of type Q.
func NewMultiMap[T comparable, Q any]() *MultiMap[T, Q] {
	return &MultiMap[T, Q]{
		items: map[T][]Q{},
	}
}

// MultiMap represents a map that can contain 1 or more values for each key.
type MultiMap[T comparable, Q any] struct {
	items map[T][]Q
}

// Clear clears all entries in the map.
func (mm *MultiMap[T, Q]) Clear() {
	mm.items = map[T][]Q{}
}

// Add adds the value to the map for the given key. If there exists an existing value, then this
// value is added to those already present *without comparison*. This means a value can be added
// twice, if this method is called again for the same value.
func (mm *MultiMap[T, Q]) Add(key T, item Q) {
	if _, ok := mm.items[key]; !ok {
		mm.items[key] = []Q{}
	}

	mm.items[key] = append(mm.items[key], item)
}

// RemoveKey removes the given key from the map.
func (mm *MultiMap[T, Q]) RemoveKey(key T) {
	delete(mm.items, key)
}

// Has returns true if the key is found in the map.
func (mm *MultiMap[T, Q]) Has(key T) bool {
	_, ok := mm.items[key]
	return ok
}

// Get returns the values for the given key in the map and whether the key existed. If the key
// does not exist, an empty slice is returned.
func (mm *MultiMap[T, Q]) Get(key T) ([]Q, bool) {
	found, ok := mm.items[key]
	if !ok {
		return []Q{}, false
	}

	return found, true
}

// IsEmpty returns true if the map is currently empty.
func (mm *MultiMap[T, Q]) IsEmpty() bool {
	return len(mm.items) == 0
}

// Len returns the length of the map, e.g. the number of *keys* present.
func (mm *MultiMap[T, Q]) Len() int {
	return len(mm.items)
}

// Keys returns the keys of the map.
func (mm *MultiMap[T, Q]) Keys() []T {
	return maps.Keys(mm.items)
}

// Values returns all values in the map.
func (mm MultiMap[T, Q]) Values() []Q {
	values := make([]Q, 0, len(mm.items)*2)
	for _, valueSlice := range maps.Values(mm.items) {
		values = append(values, valueSlice...)
	}
	return values
}

// AsReadOnly returns a read-only *copy* of the mulitmap.
func (mm *MultiMap[T, Q]) AsReadOnly() ReadOnlyMultimap[T, Q] {
	return readOnlyMultimap[T, Q]{
		maps.Clone(mm.items),
	}
}

type readOnlyMultimap[T comparable, Q any] struct {
	items map[T][]Q
}

// Has returns true if the key is found in the map.
func (mm readOnlyMultimap[T, Q]) Has(key T) bool {
	_, ok := mm.items[key]
	return ok
}

// Get returns the values for the given key in the map and whether the key existed. If the key
// does not exist, an empty slice is returned.
func (mm readOnlyMultimap[T, Q]) Get(key T) ([]Q, bool) {
	found, ok := mm.items[key]
	if !ok {
		return []Q{}, false
	}

	return found, true
}

// IsEmpty returns true if the map is currently empty.
func (mm readOnlyMultimap[T, Q]) IsEmpty() bool {
	return len(mm.items) == 0
}

// Len returns the length of the map, e.g. the number of *keys* present.
func (mm readOnlyMultimap[T, Q]) Len() int {
	return len(mm.items)
}

// Keys returns the keys of the map.
func (mm readOnlyMultimap[T, Q]) Keys() []T {
	return maps.Keys(mm.items)
}

// Values returns all values in the map.
func (mm readOnlyMultimap[T, Q]) Values() []Q {
	values := make([]Q, 0, len(mm.items)*2)
	for _, valueSlice := range maps.Values(mm.items) {
		values = append(values, valueSlice...)
	}
	return values
}
