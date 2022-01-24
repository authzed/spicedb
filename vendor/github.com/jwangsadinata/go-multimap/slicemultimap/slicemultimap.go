// Package slicemultimap implements a multimap backed by go's native slice.
//
// A slicemultimap is a multimap that can hold duplicate key-value pairs
// and that maintains the insertion ordering of values for a given key.
//
// This multimap is typically known as ListMultimap in other languages.
//
// Elements are unordered in the map.
//
// Structure is not thread safe.
//
package slicemultimap

import multimap "github.com/jwangsadinata/go-multimap"

// MultiMap holds the elements in go's native map.
type MultiMap struct {
	m map[interface{}][]interface{}
}

// New instantiates a new multimap.
func New() *MultiMap {
	return &MultiMap{m: make(map[interface{}][]interface{})}
}

// Get searches the element in the multimap by key.
// It returns its value or nil if key is not found in multimap.
// Second return parameter is true if key was found, otherwise false.
func (m *MultiMap) Get(key interface{}) (values []interface{}, found bool) {
	values, found = m.m[key]
	return
}

// Put stores a key-value pair in this multimap.
func (m *MultiMap) Put(key interface{}, value interface{}) {
	m.m[key] = append(m.m[key], value)
}

// PutAll stores a key-value pair in this multimap for each of the values, all using the same key key.
func (m *MultiMap) PutAll(key interface{}, values []interface{}) {
	for _, value := range values {
		m.Put(key, value)
	}
}

// Contains returns true if this multimap contains at least one key-value pair with the key key and the value value.
func (m *MultiMap) Contains(key interface{}, value interface{}) bool {
	values, found := m.m[key]
	for _, v := range values {
		if v == value {
			return true && found
		}
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
	for _, values := range m.m {
		for _, v := range values {
			if v == value {
				return true
			}
		}
	}
	return false
}

// Remove removes a single key-value pair from this multimap, if such exists.
func (m *MultiMap) Remove(key interface{}, value interface{}) {
	values, found := m.m[key]
	if found {
		for i, v := range values {
			if v == value {
				m.m[key] = append(values[:i], values[i+1:]...)
			}
		}
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
	for _, value := range m.m {
		size += len(value)
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
	for _, vs := range m.m {
		for _, value := range vs {
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
	for key, values := range m.m {
		for _, value := range values {
			entries[count] = multimap.Entry{Key: key, Value: value}
			count++
		}
	}
	return entries
}

// Clear removes all elements from the map.
func (m *MultiMap) Clear() {
	m.m = make(map[interface{}][]interface{})
}
