package mapz

import (
	"golang.org/x/exp/maps"
)

// ReadOnlyMultimap is a read-only multimap.
type ReadOnlyMultimap[T comparable, Q any] interface {
	// Has returns true if the key is found in the map.
	Has(key T) bool

	// Get returns the values for the given key in the map and whether the key
	// existed.
	// If the key does not exist, an empty slice is returned.
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

// NewMultiMap initializes a new MultiMap.
func NewMultiMap[T comparable, Q any]() *MultiMap[T, Q] {
	return &MultiMap[T, Q]{items: map[T][]Q{}}
}

// NewMultiMapWithCap initializes with the provided capacity for the top-level
// map.
func NewMultiMapWithCap[T comparable, Q any](capacity uint32) *MultiMap[T, Q] {
	return &MultiMap[T, Q]{items: make(map[T][]Q, capacity)}
}

// MultiMap represents a map that can contain 1 or more values for each key.
type MultiMap[T comparable, Q any] struct {
	items map[T][]Q
}

// Clear clears all entries in the map.
func (mm *MultiMap[T, Q]) Clear() {
	mm.items = map[T][]Q{}
}

// Add inserts the value into the map at the given key.
//
// If there exists an existing value, then this value is appended
// *without comparison*. Put another way, a value can be added twice, if this
// method is called twice for the same value.
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

// Get returns the values stored in the map for the provided key and whether
// the key existed.
//
// If the key does not exist, an empty slice is returned.
func (mm *MultiMap[T, Q]) Get(key T) ([]Q, bool) {
	found, ok := mm.items[key]
	if !ok {
		return []Q{}, false
	}

	return found, true
}

// Sets sets the values in the multimap to those provided.
func (mm *MultiMap[T, Q]) Set(key T, values []Q) {
	mm.items[key] = values
}

// IsEmpty returns true if the map is currently empty.
func (mm *MultiMap[T, Q]) IsEmpty() bool { return len(mm.items) == 0 }

// Len returns the length of the map, e.g. the number of *keys* present.
func (mm *MultiMap[T, Q]) Len() int { return len(mm.items) }

// Keys returns the keys of the map.
func (mm *MultiMap[T, Q]) Keys() []T { return maps.Keys(mm.items) }

// Values returns all values in the map.
func (mm MultiMap[T, Q]) Values() []Q {
	values := make([]Q, 0, len(mm.items)*2)
	for _, valueSlice := range maps.Values(mm.items) {
		values = append(values, valueSlice...)
	}
	return values
}

// Clone returns a clone of the map.
func (mm *MultiMap[T, Q]) Clone() *MultiMap[T, Q] {
	return &MultiMap[T, Q]{maps.Clone(mm.items)}
}

// CountOf returns the number of values stored for the given key.
func (mm *MultiMap[T, Q]) CountOf(key T) int {
	return len(mm.items[key])
}

// IndexOfValueInMultimap returns the index of the value in the map for the given key.
func IndexOfValueInMultimap[T comparable, Q comparable](mm *MultiMap[T, Q], key T, value Q) int {
	values, ok := mm.items[key]
	if !ok {
		return -1
	}

	for i, v := range values {
		if v == value {
			return i
		}
	}

	return -1
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
func (mm readOnlyMultimap[T, Q]) IsEmpty() bool { return len(mm.items) == 0 }

// Len returns the length of the map, e.g. the number of *keys* present.
func (mm readOnlyMultimap[T, Q]) Len() int { return len(mm.items) }

// Keys returns the keys of the map.
func (mm readOnlyMultimap[T, Q]) Keys() []T { return maps.Keys(mm.items) }

// Values returns all values in the map.
func (mm readOnlyMultimap[T, Q]) Values() []Q {
	values := make([]Q, 0, len(mm.items)*2)
	for _, valueSlice := range maps.Values(mm.items) {
		values = append(values, valueSlice...)
	}
	return values
}
