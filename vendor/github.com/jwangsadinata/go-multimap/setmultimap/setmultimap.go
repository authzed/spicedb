// Package setmultimap implements a multimap backed by a set.
//
// A setmultimap cannot hold duplicate key-value pairs.
// Adding a key-value pair that's already in the multimap has no effect.
//
// Elements are unordered in the map.
//
// Structure is not thread safe.
//
package setmultimap

import multimap "github.com/jwangsadinata/go-multimap"

var exists = struct{}{}

// Set represents a set object
type Set map[interface{}]struct{}

// MultiMap holds the elements in go's native map.
type MultiMap struct {
	m map[interface{}]Set
}

// New instantiates a new multimap.
func New() *MultiMap {
	return &MultiMap{m: make(map[interface{}]Set)}
}

// Get searches the element in the multimap by key.
// It returns its value or nil if key is not found in multimap.
// Second return parameter is true if key was found, otherwise false.
func (m *MultiMap) Get(key interface{}) (values []interface{}, found bool) {
	set, found := m.m[key]
	values = make([]interface{}, len(set))
	count := 0
	for value := range set {
		values[count] = value
		count++
	}
	return
}

// Put stores a key-value pair in this multimap.
func (m *MultiMap) Put(key interface{}, value interface{}) {
	set, found := m.m[key]
	if found {
		set[value] = exists
	} else {
		set = make(Set)
		set[value] = exists
		m.m[key] = set
	}
}

// PutAll stores a key-value pair in this multimap for each of the values, all using the same key key.
func (m *MultiMap) PutAll(key interface{}, values []interface{}) {
	for _, value := range values {
		m.Put(key, value)
	}
}

// Contains returns true if this multimap contains at least one key-value pair with the key key and the value value.
func (m *MultiMap) Contains(key interface{}, value interface{}) bool {
	set, found := m.m[key]
	if _, ok := set[value]; ok {
		return true && found
	}
	return false && found
}

// ContainsKey returns true if this multimap contains at least one key-value pair with the key key.
func (m *MultiMap) ContainsKey(key interface{}) (found bool) {
	_, found = m.m[key]
	return
}

// ContainsValue returns true if this multimap contains at least one key-value pair with the value value.
func (m *MultiMap) ContainsValue(value interface{}) bool {
	for _, set := range m.m {
		if _, ok := set[value]; ok {
			return true
		}
	}
	return false
}

// Remove removes a single key-value pair from this multimap, if such exists.
func (m *MultiMap) Remove(key interface{}, value interface{}) {
	set, found := m.m[key]
	if found {
		delete(set, value)
	}
	if len(m.m[key]) == 0 {
		delete(m.m, key)
	}
}

// RemoveAll removes all values associated with the key from the multimap.
func (m *MultiMap) RemoveAll(key interface{}) {
	delete(m.m, key)
}

// Empty returns true if multimap does not contain any key-value pairs.
func (m *MultiMap) Empty() bool {
	return m.Size() == 0
}

// Size returns number of key-value pairs in the multimap.
func (m *MultiMap) Size() int {
	size := 0
	for _, set := range m.m {
		size += len(set)
	}
	return size
}

// Keys returns a view collection containing the key from each key-value pair in this multimap.
// This is done without collapsing duplicates.
func (m *MultiMap) Keys() []interface{} {
	keys := make([]interface{}, m.Size())
	count := 0
	for key, value := range m.m {
		for range value {
			keys[count] = key
			count++
		}
	}
	return keys
}

// KeySet returns all distinct keys contained in this multimap.
func (m *MultiMap) KeySet() []interface{} {
	keys := make([]interface{}, len(m.m))
	count := 0
	for key := range m.m {
		keys[count] = key
		count++
	}
	return keys
}

// Values returns all values from each key-value pair contained in this multimap.
// This is done without collapsing duplicates. (size of Values() = MultiMap.Size()).
func (m *MultiMap) Values() []interface{} {
	values := make([]interface{}, m.Size())
	count := 0
	for _, set := range m.m {
		for value := range set {
			values[count] = value
			count++
		}
	}
	return values
}

// Entries view collection of all key-value pairs contained in this multimap.
// The return type is a slice of multimap.Entry instances.
// Retrieving the key and value from the entries result will be as trivial as:
//   - var entry = m.Entries()[0]
//   - var key = entry.Key
//   - var value = entry.Value
func (m *MultiMap) Entries() []multimap.Entry {
	entries := make([]multimap.Entry, m.Size())
	count := 0
	for key, set := range m.m {
		for value := range set {
			entries[count] = multimap.Entry{Key: key, Value: value}
			count++
		}
	}
	return entries
}

// Clear removes all elements from the map.
func (m *MultiMap) Clear() {
	m.m = make(map[interface{}]Set)
}
