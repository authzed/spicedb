/*
 * Copyright 2020 Dgraph Labs, Inc. and Contributors
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
	"math"
	"sync"
	"sync/atomic"

	"github.com/outcaste-io/ristretto/z"
)

const (
	// lfuSample is the number of items to sample when looking at eviction
	// candidates. 5 seems to be the most optimal number [citation needed].
	lfuSample = 5
)

// lfuPolicy encapsulates eviction/admission behavior.
type lfuPolicy struct {
	sync.Mutex
	admit    *tinyLFU
	costs    *keyCosts
	itemsCh  chan []uint64
	stop     chan struct{}
	isClosed bool
	metrics  *Metrics
}

func newPolicy(numCounters, maxCost int64) *lfuPolicy {
	p := &lfuPolicy{
		admit:   newTinyLFU(numCounters),
		costs:   newSampledLFU(maxCost),
		itemsCh: make(chan []uint64, 3),
		stop:    make(chan struct{}),
	}
	go p.processItems()
	return p
}

func (p *lfuPolicy) CollectMetrics(metrics *Metrics) {
	p.metrics = metrics
	p.costs.metrics = metrics
}

type policyPair struct {
	key  uint64
	cost int64
}

func (p *lfuPolicy) processItems() {
	for {
		select {
		case items := <-p.itemsCh:
			p.Lock()
			p.admit.Push(items)
			p.Unlock()
		case <-p.stop:
			return
		}
	}
}

func (p *lfuPolicy) Push(keys []uint64) bool {
	if p.isClosed {
		return false
	}

	if len(keys) == 0 {
		return true
	}

	select {
	case p.itemsCh <- keys:
		p.metrics.add(keepGets, keys[0], uint64(len(keys)))
		return true
	default:
		p.metrics.add(dropGets, keys[0], uint64(len(keys)))
		return false
	}
}

// Add decides whether the item with the given key and cost should be accepted by
// the policy. It returns the list of victims that have been evicted and a boolean
// indicating whether the incoming item should be accepted.
func (p *lfuPolicy) Add(key uint64, cost int64) ([]*Item, bool) {
	p.Lock()
	defer p.Unlock()

	// Cannot add an item bigger than entire cache.
	if cost > p.costs.getMaxCost() {
		return nil, false
	}

	// No need to go any further if the item is already in the cache.
	if has := p.costs.updateIfHas(key, cost); has {
		// An update does not count as an addition, so return false.
		return nil, false
	}

	// If the execution reaches this point, the key doesn't exist in the cache.
	// Calculate the remaining room in the cache (usually bytes).
	room := p.costs.roomLeft(cost)
	if room >= 0 {
		// There's enough room in the cache to store the new item without
		// overflowing. Do that now and stop here.
		p.costs.add(key, cost)
		p.metrics.add(costAdd, key, uint64(cost))
		return nil, true
	}

	// incHits is the hit count for the incoming item.
	incHits := p.admit.Estimate(key)
	// sample is the eviction candidate pool to be filled via random sampling.
	// TODO: perhaps we should use a min heap here. Right now our time
	// complexity is N for finding the min. Min heap should bring it down to
	// O(lg N).
	sample := make([]*policyPair, 0, lfuSample)
	// As items are evicted they will be appended to victims.
	victims := make([]*Item, 0)

	// Delete victims until there's enough space or a minKey is found that has
	// more hits than incoming item.
	for ; room < 0; room = p.costs.roomLeft(cost) {
		// Fill up empty slots in sample.
		sample = p.costs.fillSample(sample)

		// Find minimally used item in sample.
		minKey, minHits, minId, minCost := uint64(0), int64(math.MaxInt64), 0, int64(0)
		for i, pair := range sample {
			// Look up hit count for sample key.
			if hits := p.admit.Estimate(pair.key); hits < minHits {
				minKey, minHits, minId, minCost = pair.key, hits, i, pair.cost
			}
		}

		// If the incoming item isn't worth keeping in the policy, reject.
		if incHits < minHits {
			p.metrics.add(rejectSets, key, 1)
			return victims, false
		}

		// Delete the victim from metadata.
		p.costs.del(minKey)

		// Delete the victim from sample.
		sample[minId] = sample[len(sample)-1]
		sample = sample[:len(sample)-1]
		// Store victim in evicted victims slice.
		victims = append(victims, &Item{
			Key:      minKey,
			Conflict: 0,
			Cost:     minCost,
		})
	}

	p.costs.add(key, cost)
	p.metrics.add(costAdd, key, uint64(cost))
	return victims, true
}

func (p *lfuPolicy) Has(key uint64) bool {
	p.Lock()
	_, exists := p.costs.keyCosts[key]
	p.Unlock()
	return exists
}

func (p *lfuPolicy) Del(key uint64) {
	p.Lock()
	p.costs.del(key)
	p.Unlock()
}

func (p *lfuPolicy) Cap() int64 {
	p.Lock()
	capacity := int64(p.costs.getMaxCost() - p.costs.used)
	p.Unlock()
	return capacity
}

func (p *lfuPolicy) Update(key uint64, cost int64) {
	p.Lock()
	p.costs.updateIfHas(key, cost)
	p.Unlock()
}

func (p *lfuPolicy) Cost(key uint64) int64 {
	p.Lock()
	if cost, found := p.costs.keyCosts[key]; found {
		p.Unlock()
		return cost
	}
	p.Unlock()
	return -1
}

func (p *lfuPolicy) Clear() {
	p.Lock()
	p.admit.clear()
	p.costs.clear()
	p.Unlock()
}

func (p *lfuPolicy) Close() {
	if p.isClosed {
		return
	}

	// Block until the p.processItems goroutine returns.
	p.stop <- struct{}{}
	close(p.stop)
	close(p.itemsCh)
	p.isClosed = true
}

func (p *lfuPolicy) MaxCost() int64 {
	if p == nil || p.costs == nil {
		return 0
	}
	return p.costs.getMaxCost()
}

func (p *lfuPolicy) UpdateMaxCost(maxCost int64) {
	if p == nil || p.costs == nil {
		return
	}
	p.costs.updateMaxCost(maxCost)
}

// keyCosts stores key-cost pairs.
type keyCosts struct {
	// NOTE: align maxCost to 64-bit boundary for use with atomic.
	// As per https://golang.org/pkg/sync/atomic/: "On ARM, x86-32,
	// and 32-bit MIPS, it is the caller’s responsibility to arrange
	// for 64-bit alignment of 64-bit words accessed atomically.
	// The first word in a variable or in an allocated struct, array,
	// or slice can be relied upon to be 64-bit aligned."
	maxCost  int64
	used     int64
	metrics  *Metrics
	keyCosts map[uint64]int64
}

func newSampledLFU(maxCost int64) *keyCosts {
	return &keyCosts{
		keyCosts: make(map[uint64]int64),
		maxCost:  maxCost,
	}
}

func (p *keyCosts) getMaxCost() int64 {
	return atomic.LoadInt64(&p.maxCost)
}

func (p *keyCosts) updateMaxCost(maxCost int64) {
	atomic.StoreInt64(&p.maxCost, maxCost)
}

func (p *keyCosts) roomLeft(cost int64) int64 {
	return p.getMaxCost() - (p.used + cost)
}

func (p *keyCosts) fillSample(in []*policyPair) []*policyPair {
	if len(in) >= lfuSample {
		return in
	}
	for key, cost := range p.keyCosts {
		in = append(in, &policyPair{key, cost})
		if len(in) >= lfuSample {
			return in
		}
	}
	return in
}

func (p *keyCosts) del(key uint64) {
	cost, ok := p.keyCosts[key]
	if !ok {
		return
	}
	p.used -= cost
	delete(p.keyCosts, key)
	p.metrics.add(costEvict, key, uint64(cost))
	p.metrics.add(keyEvict, key, 1)
}

func (p *keyCosts) add(key uint64, cost int64) {
	p.keyCosts[key] = cost
	p.used += cost
}

func (p *keyCosts) updateIfHas(key uint64, cost int64) bool {
	if prev, found := p.keyCosts[key]; found {
		// Update the cost of an existing key, but don't worry about evicting.
		// Evictions will be handled the next time a new item is added.
		p.metrics.add(keyUpdate, key, 1)
		if prev > cost {
			diff := prev - cost
			p.metrics.add(costAdd, key, ^uint64(uint64(diff)-1))
		} else if cost > prev {
			diff := cost - prev
			p.metrics.add(costAdd, key, uint64(diff))
		}
		p.used += cost - prev
		p.keyCosts[key] = cost
		return true
	}
	return false
}

func (p *keyCosts) clear() {
	p.used = 0
	p.keyCosts = make(map[uint64]int64)
}

// tinyLFU is an admission helper that keeps track of access frequency using
// tiny (4-bit) counters in the form of a count-min sketch.
// tinyLFU is NOT thread safe.
type tinyLFU struct {
	freq    *cmSketch
	door    *z.Bloom
	incrs   int64
	resetAt int64
}

func newTinyLFU(numCounters int64) *tinyLFU {
	return &tinyLFU{
		freq:    newCmSketch(numCounters),
		door:    z.NewBloomFilter(float64(numCounters), 0.01),
		resetAt: numCounters,
	}
}

func (p *tinyLFU) Push(keys []uint64) {
	for _, key := range keys {
		p.Increment(key)
	}
}

func (p *tinyLFU) Estimate(key uint64) int64 {
	hits := p.freq.Estimate(key)
	if p.door.Has(key) {
		hits++
	}
	return hits
}

func (p *tinyLFU) Increment(key uint64) {
	// Flip doorkeeper bit if not already done.
	if added := p.door.AddIfNotHas(key); !added {
		// Increment count-min counter if doorkeeper bit is already set.
		p.freq.Increment(key)
	}
	p.incrs++
	if p.incrs >= p.resetAt {
		p.reset()
	}
}

func (p *tinyLFU) reset() {
	// Zero out incrs.
	p.incrs = 0
	// clears doorkeeper bits
	p.door.Clear()
	// halves count-min counters
	p.freq.Reset()
}

func (p *tinyLFU) clear() {
	p.incrs = 0
	p.door.Clear()
	p.freq.Clear()
}
