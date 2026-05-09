/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ristretto

import (
	"sync"
	"time"
)

// TODO: Do we need this to be a separate struct from Item?
type storeItem struct {
	key        uint64
	conflict   uint64
	value      interface{}
	expiration time.Time
}

const numShards uint64 = 256

type updateFn func(prev, cur interface{}) bool
type shardedMap struct {
	shards       []*lockedMap
	expiryMap    *expirationMap
	shouldUpdate func(prev, cur interface{}) bool
}

// newShardedMap is safe for concurrent usage.
func newShardedMap(fn updateFn) *shardedMap {
	sm := &shardedMap{
		shards:    make([]*lockedMap, int(numShards)),
		expiryMap: newExpirationMap(),
	}
	if fn == nil {
		fn = func(prev, cur interface{}) bool {
			return true
		}
	}
	for i := range sm.shards {
		sm.shards[i] = newLockedMap(fn, sm.expiryMap)
	}
	return sm
}

func (sm *shardedMap) Get(key, conflict uint64) (interface{}, bool) {
	return sm.shards[key%numShards].get(key, conflict)
}

func (sm *shardedMap) Expiration(key uint64) time.Time {
	return sm.shards[key%numShards].Expiration(key)
}

func (sm *shardedMap) Set(i *Item) {
	if i == nil {
		// If item is nil make this Set a no-op.
		return
	}

	sm.shards[i.Key%numShards].Set(i)
}

func (sm *shardedMap) Del(key, conflict uint64) (uint64, interface{}) {
	return sm.shards[key%numShards].Del(key, conflict)
}

func (sm *shardedMap) Update(newItem *Item) (interface{}, bool) {
	return sm.shards[newItem.Key%numShards].Update(newItem)
}

func (sm *shardedMap) Cleanup(policy *lfuPolicy, onEvict itemCallback) {
	sm.expiryMap.cleanup(sm, policy, onEvict)
}

func (sm *shardedMap) Clear(onEvict itemCallback) {
	for i := uint64(0); i < numShards; i++ {
		sm.shards[i].Clear(onEvict)
	}
}

type lockedMap struct {
	sync.RWMutex
	data         map[uint64]storeItem
	em           *expirationMap
	shouldUpdate updateFn
}

func newLockedMap(fn updateFn, em *expirationMap) *lockedMap {
	return &lockedMap{
		data:         make(map[uint64]storeItem),
		em:           em,
		shouldUpdate: fn,
	}
}

func (m *lockedMap) get(key, conflict uint64) (interface{}, bool) {
	m.RLock()
	item, ok := m.data[key]
	m.RUnlock()
	if !ok {
		return nil, false
	}
	if conflict != 0 && (conflict != item.conflict) {
		return nil, false
	}

	// Handle expired items.
	if !item.expiration.IsZero() && time.Now().After(item.expiration) {
		return nil, false
	}
	return item.value, true
}

func (m *lockedMap) Expiration(key uint64) time.Time {
	m.RLock()
	defer m.RUnlock()
	return m.data[key].expiration
}

func (m *lockedMap) Set(i *Item) {
	if i == nil {
		// If the item is nil make this Set a no-op.
		return
	}

	m.Lock()
	defer m.Unlock()
	item, ok := m.data[i.Key]

	if ok {
		// The item existed already. We need to check the conflict key and reject the
		// update if they do not match. Only after that the expiration map is updated.
		if i.Conflict != 0 && (i.Conflict != item.conflict) {
			return
		}
		if !m.shouldUpdate(item.value, i.Value) {
			return
		}
		m.em.update(i.Key, i.Conflict, item.expiration, i.Expiration)
	} else {
		// The value is not in the map already. There's no need to return anything.
		// Simply add the expiration map.
		m.em.add(i.Key, i.Conflict, i.Expiration)
	}

	m.data[i.Key] = storeItem{
		key:        i.Key,
		conflict:   i.Conflict,
		value:      i.Value,
		expiration: i.Expiration,
	}
}

func (m *lockedMap) Del(key, conflict uint64) (uint64, interface{}) {
	m.Lock()
	item, ok := m.data[key]
	if !ok {
		m.Unlock()
		return 0, nil
	}
	if conflict != 0 && (conflict != item.conflict) {
		m.Unlock()
		return 0, nil
	}

	if !item.expiration.IsZero() {
		m.em.del(key, item.expiration)
	}

	delete(m.data, key)
	m.Unlock()
	return item.conflict, item.value
}

func (m *lockedMap) Update(newItem *Item) (interface{}, bool) {
	m.Lock()
	defer m.Unlock()

	item, ok := m.data[newItem.Key]
	if !ok {
		return nil, false
	}
	if newItem.Conflict != 0 && (newItem.Conflict != item.conflict) {
		return nil, false
	}
	if !m.shouldUpdate(item.value, newItem.Value) {
		return item.value, false
	}

	m.em.update(newItem.Key, newItem.Conflict, item.expiration, newItem.Expiration)
	m.data[newItem.Key] = storeItem{
		key:        newItem.Key,
		conflict:   newItem.Conflict,
		value:      newItem.Value,
		expiration: newItem.Expiration,
	}
	return item.value, true
}

func (m *lockedMap) Clear(onEvict itemCallback) {
	m.Lock()
	i := &Item{}
	if onEvict != nil {
		for _, si := range m.data {
			i.Key = si.key
			i.Conflict = si.conflict
			i.Value = si.value
			onEvict(i)
		}
	}
	m.data = make(map[uint64]storeItem)
	m.Unlock()
}
