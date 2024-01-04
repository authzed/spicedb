package mapz

import "sync"

// CountingMultiMap is a multimap that counts the number of distinct values for each
// key, removing the key from the map when the count reaches zero. Safe for concurrent
// use.
type CountingMultiMap[T comparable, Q comparable] struct {
	valuesByKey map[T]*Set[Q]
	lock        sync.Mutex
}

// NewCountingMultiMap constructs a new counting multimap.
func NewCountingMultiMap[T comparable, Q comparable]() *CountingMultiMap[T, Q] {
	return &CountingMultiMap[T, Q]{
		valuesByKey: map[T]*Set[Q]{},
		lock:        sync.Mutex{},
	}
}

// Add adds the given value to the map at the given key. Returns true if the value
// already existed in the map for the given key.
func (cmm *CountingMultiMap[T, Q]) Add(key T, value Q) bool {
	cmm.lock.Lock()
	defer cmm.lock.Unlock()

	values, ok := cmm.valuesByKey[key]
	if !ok {
		values = NewSet[Q]()
		cmm.valuesByKey[key] = values
	}
	return !values.Add(value)
}

// Remove removes the given value for the given key from the map. If, after this removal,
// the key has no additional values, it is removed entirely from the map.
func (cmm *CountingMultiMap[T, Q]) Remove(key T, value Q) {
	cmm.lock.Lock()
	defer cmm.lock.Unlock()

	values, ok := cmm.valuesByKey[key]
	if !ok {
		return
	}

	values.Delete(value)
	if values.IsEmpty() {
		delete(cmm.valuesByKey, key)
	}
}
