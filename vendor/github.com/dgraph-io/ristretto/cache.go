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

// Ristretto is a fast, fixed size, in-memory cache with a dual focus on
// throughput and hit ratio performance. You can easily add Ristretto to an
// existing system and keep the most valuable data where you need it.
package ristretto

import (
	"errors"
	"sync"
	"time"
	"unsafe"

	"github.com/dgraph-io/ristretto/z"
)

// TODO: find the optimal value for this or make it configurable
var setBufSize = 32 * 1024

type itemCallback func(*Item)

const itemSize = int64(unsafe.Sizeof(storeItem{}))

// Cache is a thread-safe implementation of a hashmap with a TinyLFU admission
// policy and a Sampled LFU eviction policy. You can use the same Cache instance
// from as many goroutines as you want.
type Cache struct {
	// store is the central concurrent hashmap where key-value items are stored.
	store *shardedMap
	// getBuf is a custom ring buffer implementation that gets pushed to when
	// keys are read.
	getBuf *ringBuffer
	// onEvict is called for item evictions.
	onEvict itemCallback
	// onReject is called when an item is rejected via admission policy.
	onReject itemCallback
	// onExit is called whenever a value goes out of scope from the cache.
	onExit (func(interface{}))
	// KeyToHash function is used to customize the key hashing algorithm.
	// Each key will be hashed using the provided function. If keyToHash value
	// is not set, the default keyToHash function is used.
	keyToHash func(interface{}) (uint64, uint64)
	// cost calculates cost from a value.
	cost func(value interface{}) int64
	// ignoreInternalCost dictates whether to ignore the cost of internally storing
	// the item in the cost calculation.
	ignoreInternalCost bool
	// cleanupTicker is used to periodically check for entries whose TTL has passed.
	cleanupTicker *time.Ticker
	// Metrics contains a running log of important statistics like hits, misses,
	// and dropped items.
	Metrics *Metrics
	// internalsLock is a lock around the internals value.
	internalsLock sync.Mutex
	// internals holds the references to the cache internals, including the setBuf
	// channel and the policy. If the cache has been closed, this value is nil.
	internals *cacheInternals
}

type cacheInternals struct {
	// setBuf is a buffer allowing us to batch/drop Sets during times of high
	// contention.
	setBuf chan *Item
	// stop is used to stop the processItems goroutine.
	stop chan struct{}
	// policy determines what gets let in to the cache and what gets kicked out.
	policy *lfuPolicy
}

// Config is passed to NewCache for creating new Cache instances.
type Config struct {
	// NumCounters determines the number of counters (keys) to keep that hold
	// access frequency information. It's generally a good idea to have more
	// counters than the max cache capacity, as this will improve eviction
	// accuracy and subsequent hit ratios.
	//
	// For example, if you expect your cache to hold 1,000,000 items when full,
	// NumCounters should be 10,000,000 (10x). Each counter takes up roughly
	// 3 bytes (4 bits for each counter * 4 copies plus about a byte per
	// counter for the bloom filter). Note that the number of counters is
	// internally rounded up to the nearest power of 2, so the space usage
	// may be a little larger than 3 bytes * NumCounters.
	NumCounters int64
	// MaxCost can be considered as the cache capacity, in whatever units you
	// choose to use.
	//
	// For example, if you want the cache to have a max capacity of 100MB, you
	// would set MaxCost to 100,000,000 and pass an item's number of bytes as
	// the `cost` parameter for calls to Set. If new items are accepted, the
	// eviction process will take care of making room for the new item and not
	// overflowing the MaxCost value.
	MaxCost int64
	// BufferItems determines the size of Get buffers.
	//
	// Unless you have a rare use case, using `64` as the BufferItems value
	// results in good performance.
	BufferItems int64
	// Metrics determines whether cache statistics are kept during the cache's
	// lifetime. There *is* some overhead to keeping statistics, so you should
	// only set this flag to true when testing or throughput performance isn't a
	// major factor.
	Metrics bool
	// OnEvict is called for every eviction and passes the hashed key, value,
	// and cost to the function.
	OnEvict func(item *Item)
	// OnReject is called for every rejection done via the policy.
	OnReject func(item *Item)
	// OnExit is called whenever a value is removed from cache. This can be
	// used to do manual memory deallocation. Would also be called on eviction
	// and rejection of the value.
	OnExit func(val interface{})
	// KeyToHash function is used to customize the key hashing algorithm.
	// Each key will be hashed using the provided function. If keyToHash value
	// is not set, the default keyToHash function is used.
	KeyToHash func(key interface{}) (uint64, uint64)
	// shouldUpdate is called when a value already exists in cache and is being updated.
	ShouldUpdate func(prev, cur interface{}) bool
	// Cost evaluates a value and outputs a corresponding cost. This function
	// is ran after Set is called for a new item or an item update with a cost
	// param of 0.
	Cost func(value interface{}) int64
	// IgnoreInternalCost set to true indicates to the cache that the cost of
	// internally storing the value should be ignored. This is useful when the
	// cost passed to set is not using bytes as units. Keep in mind that setting
	// this to true will increase the memory usage.
	IgnoreInternalCost bool
}

type itemFlag byte

const (
	itemNew itemFlag = iota
	itemDelete
	itemUpdate
)

// Item is passed to setBuf so items can eventually be added to the cache.
type Item struct {
	flag       itemFlag
	Key        uint64
	Conflict   uint64
	Value      interface{}
	Cost       int64
	Expiration time.Time
	wg         *sync.WaitGroup
}

// NewCache returns a new Cache instance and any configuration errors, if any.
func NewCache(config *Config) (*Cache, error) {
	switch {
	case config.NumCounters == 0:
		return nil, errors.New("NumCounters can't be zero")
	case config.MaxCost == 0:
		return nil, errors.New("MaxCost can't be zero")
	case config.BufferItems == 0:
		return nil, errors.New("BufferItems can't be zero")
	}
	policy := newPolicy(config.NumCounters, config.MaxCost)
	cache := &Cache{
		store:              newShardedMap(config.ShouldUpdate),
		getBuf:             newRingBuffer(policy, config.BufferItems),
		keyToHash:          config.KeyToHash,
		cost:               config.Cost,
		ignoreInternalCost: config.IgnoreInternalCost,
		cleanupTicker:      time.NewTicker(time.Duration(bucketDurationSecs) * time.Second / 2),
		internals: &cacheInternals{
			policy: policy,
			stop:   make(chan struct{}),
			setBuf: make(chan *Item, setBufSize),
		},
	}
	cache.onExit = func(val interface{}) {
		if config.OnExit != nil && val != nil {
			config.OnExit(val)
		}
	}
	cache.onEvict = func(item *Item) {
		if config.OnEvict != nil {
			config.OnEvict(item)
		}
		cache.onExit(item.Value)
	}
	cache.onReject = func(item *Item) {
		if config.OnReject != nil {
			config.OnReject(item)
		}
		cache.onExit(item.Value)
	}
	cache.store.shouldUpdate = func(prev, cur interface{}) bool {
		if config.ShouldUpdate != nil {
			return config.ShouldUpdate(prev, cur)
		}
		return true
	}
	if cache.keyToHash == nil {
		cache.keyToHash = z.KeyToHash
	}
	if config.Metrics {
		cache.collectMetrics()
	}
	// NOTE: benchmarks seem to show that performance decreases the more
	//       goroutines we have running cache.processItems(), so 1 should
	//       usually be sufficient
	go cache.processItems()
	return cache, nil
}

func (c *Cache) getInternals() *cacheInternals {
	if c == nil {
		return nil
	}
	c.internalsLock.Lock()
	defer c.internalsLock.Unlock()
	return c.internals
}

func (c *Cache) Wait() {
	internals := c.getInternals()
	if internals == nil {
		return
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	internals.setBuf <- &Item{wg: wg}
	wg.Wait()
}

// Get returns the value (if any) and a boolean representing whether the
// value was found or not. The value can be nil and the boolean can be true at
// the same time.
func (c *Cache) Get(key interface{}) (interface{}, bool) {
	internals := c.getInternals()
	if internals == nil {
		return nil, false
	}

	if key == nil {
		return nil, false
	}

	keyHash, conflictHash := c.keyToHash(key)
	c.getBuf.Push(keyHash)
	value, ok := c.store.Get(keyHash, conflictHash)
	if ok {
		c.Metrics.add(hit, keyHash, 1)
	} else {
		c.Metrics.add(miss, keyHash, 1)
	}
	return value, ok
}

// Set attempts to add the key-value item to the cache. If it returns false,
// then the Set was dropped and the key-value item isn't added to the cache. If
// it returns true, there's still a chance it could be dropped by the policy if
// its determined that the key-value item isn't worth keeping, but otherwise the
// item will be added and other items will be evicted in order to make room.
//
// To dynamically evaluate the items cost using the Config.Coster function, set
// the cost parameter to 0 and Coster will be ran when needed in order to find
// the items true cost.
func (c *Cache) Set(key, value interface{}, cost int64) bool {
	return c.SetWithTTL(key, value, cost, 0*time.Second)
}

// SetWithTTL works like Set but adds a key-value pair to the cache that will expire
// after the specified TTL (time to live) has passed. A zero value means the value never
// expires, which is identical to calling Set. A negative value is a no-op and the value
// is discarded.
func (c *Cache) SetWithTTL(key, value interface{}, cost int64, ttl time.Duration) bool {
	return c.setInternal(key, value, cost, ttl, false)
}

// SetIfPresent is like Set, but only updates the value of an existing key. It
// does NOT add the key to cache if it's absent.
func (c *Cache) SetIfPresent(key, value interface{}, cost int64) bool {
	return c.setInternal(key, value, cost, 0*time.Second, true)
}

func (c *Cache) setInternal(key, value interface{},
	cost int64, ttl time.Duration, onlyUpdate bool) bool {
	internals := c.getInternals()
	if internals == nil {
		return false
	}

	if key == nil {
		return false
	}

	var expiration time.Time
	switch {
	case ttl == 0:
		// No expiration.
		break
	case ttl < 0:
		// Treat this a a no-op.
		return false
	default:
		expiration = time.Now().Add(ttl)
	}

	keyHash, conflictHash := c.keyToHash(key)
	i := &Item{
		flag:       itemNew,
		Key:        keyHash,
		Conflict:   conflictHash,
		Value:      value,
		Cost:       cost,
		Expiration: expiration,
	}
	if onlyUpdate {
		i.flag = itemUpdate
	}
	// cost is eventually updated. The expiration must also be immediately updated
	// to prevent items from being prematurely removed from the map.
	if prev, ok := c.store.Update(i); ok {
		c.onExit(prev)
		i.flag = itemUpdate
	} else if onlyUpdate {
		// The instruction was to update the key, but store.Update failed. So,
		// this is a NOOP.
		return false
	}
	// Attempt to send item to policy.
	select {
	case internals.setBuf <- i:
		return true
	default:
		if i.flag == itemUpdate {
			// Return true if this was an update operation since we've already
			// updated the store. For all the other operations (set/delete), we
			// return false which means the item was not inserted.
			return true
		}
		c.Metrics.add(dropSets, keyHash, 1)
		return false
	}
}

// Del deletes the key-value item from the cache if it exists.
func (c *Cache) Del(key interface{}) {
	internals := c.getInternals()
	if internals == nil {
		return
	}

	if key == nil {
		return
	}

	keyHash, conflictHash := c.keyToHash(key)
	// Delete immediately.
	_, prev := c.store.Del(keyHash, conflictHash)
	c.onExit(prev)
	// If we've set an item, it would be applied slightly later.
	// So we must push the same item to `setBuf` with the deletion flag.
	// This ensures that if a set is followed by a delete, it will be
	// applied in the correct order.
	internals.setBuf <- &Item{
		flag:     itemDelete,
		Key:      keyHash,
		Conflict: conflictHash,
	}
}

// GetTTL returns the TTL for the specified key and a bool that is true if the
// item was found and is not expired.
func (c *Cache) GetTTL(key interface{}) (time.Duration, bool) {
	if c == nil || key == nil {
		return 0, false
	}

	keyHash, conflictHash := c.keyToHash(key)
	if _, ok := c.store.Get(keyHash, conflictHash); !ok {
		// not found
		return 0, false
	}

	expiration := c.store.Expiration(keyHash)
	if expiration.IsZero() {
		// found but no expiration
		return 0, true
	}

	if time.Now().After(expiration) {
		// found but expired
		return 0, false
	}

	return time.Until(expiration), true
}

// Close stops all goroutines and closes all channels.
func (c *Cache) Close() {
	internals := c.getInternals()
	if internals == nil {
		return
	}

	c.Clear()

	// Block until processItems goroutine is returned.
	internals.stop <- struct{}{}
	close(internals.stop)

	c.internalsLock.Lock()
	c.internals = nil
	c.internalsLock.Unlock()

	internals.policy.Close()
}

// Clear empties the hashmap and zeroes all policy counters. Note that this is
// not an atomic operation (but that shouldn't be a problem as it's assumed that
// Set/Get calls won't be occurring until after this).
func (c *Cache) Clear() {
	internals := c.getInternals()
	if internals == nil {
		return
	}

	// Block until processItems goroutine is returned.
	internals.stop <- struct{}{}

	// Clear out the setBuf channel.
loop:
	for {
		select {
		case i := <-internals.setBuf:
			if i.wg != nil {
				i.wg.Done()
				continue
			}
			if i.flag != itemUpdate {
				// In itemUpdate, the value is already set in the store.  So, no need to call
				// onEvict here.
				c.onEvict(i)
			}
		default:
			break loop
		}
	}

	// Clear value hashmap and policy data.
	internals.policy.Clear()
	c.store.Clear(c.onEvict)
	// Only reset metrics if they're enabled.
	if c.Metrics != nil {
		c.Metrics.Clear()
	}
	// Restart processItems goroutine.
	go c.processItems()
}

// MaxCost returns the max cost of the cache.
func (c *Cache) MaxCost() int64 {
	internals := c.getInternals()
	if internals == nil {
		return 0
	}

	return internals.policy.MaxCost()
}

// UpdateMaxCost updates the maxCost of an existing cache.
func (c *Cache) UpdateMaxCost(maxCost int64) {
	internals := c.getInternals()
	if internals == nil {
		return
	}
	internals.policy.UpdateMaxCost(maxCost)
}

// processItems is ran by goroutines processing the Set buffer.
func (c *Cache) processItems() {
	internals := c.getInternals()
	if internals == nil {
		return
	}

	startTs := make(map[uint64]time.Time)
	numToKeep := 100000 // TODO: Make this configurable via options.

	trackAdmission := func(key uint64) {
		if c.Metrics == nil {
			return
		}
		startTs[key] = time.Now()
		if len(startTs) > numToKeep {
			for k := range startTs {
				if len(startTs) <= numToKeep {
					break
				}
				delete(startTs, k)
			}
		}
	}
	onEvict := func(i *Item) {
		if ts, has := startTs[i.Key]; has {
			c.Metrics.trackEviction(int64(time.Since(ts) / time.Second))
			delete(startTs, i.Key)
		}
		if c.onEvict != nil {
			c.onEvict(i)
		}
	}

	for {
		select {
		case i := <-internals.setBuf:
			if i.wg != nil {
				i.wg.Done()
				continue
			}
			// Calculate item cost value if new or update.
			if i.Cost == 0 && c.cost != nil && i.flag != itemDelete {
				i.Cost = c.cost(i.Value)
			}
			if !c.ignoreInternalCost {
				// Add the cost of internally storing the object.
				i.Cost += itemSize
			}

			switch i.flag {
			case itemNew:
				victims, added := internals.policy.Add(i.Key, i.Cost)
				if added {
					c.store.Set(i)
					c.Metrics.add(keyAdd, i.Key, 1)
					trackAdmission(i.Key)
				} else {
					c.onReject(i)
				}
				for _, victim := range victims {
					victim.Conflict, victim.Value = c.store.Del(victim.Key, 0)
					onEvict(victim)
				}

			case itemUpdate:
				internals.policy.Update(i.Key, i.Cost)

			case itemDelete:
				internals.policy.Del(i.Key) // Deals with metrics updates.
				_, val := c.store.Del(i.Key, i.Conflict)
				c.onExit(val)
			}
		case <-c.cleanupTicker.C:
			c.store.Cleanup(internals.policy, onEvict)
		case <-internals.stop:
			return
		}
	}
}
