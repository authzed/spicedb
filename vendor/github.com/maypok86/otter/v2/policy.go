// Copyright (c) 2025 Alexey Mayshev and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otter

import (
	"github.com/maypok86/otter/v2/internal/deque"
	"github.com/maypok86/otter/v2/internal/generated/node"
	"github.com/maypok86/otter/v2/internal/xruntime"
)

const (
	isExp = false

	// The initial percent of the maximum weighted capacity dedicated to the main space.
	percentMain = 0.99
	// percentMainProtected is the percent of the maximum weighted capacity dedicated to the main's protected space.
	percentMainProtected = 0.80
	// The difference in hit rates that restarts the climber.
	hillClimberRestartThreshold = 0.05
	// The percent of the total size to adapt the window by.
	hillClimberStepPercent = 0.0625
	// The rate to decrease the step size to adapt by.
	hillClimberStepDecayRate = 0.98
	// admitHashdosThreshold is the minimum popularity for allowing randomized admission.
	admitHashdosThreshold = 6
	// The maximum number of entries that can be transferred between queues.
	queueTransferThreshold = 1_000
)

type policy[K comparable, V any] struct {
	sketch                    *sketch[K]
	window                    *deque.Linked[K, V]
	probation                 *deque.Linked[K, V]
	protected                 *deque.Linked[K, V]
	maximum                   uint64
	weightedSize              uint64
	windowMaximum             uint64
	windowWeightedSize        uint64
	mainProtectedMaximum      uint64
	mainProtectedWeightedSize uint64
	stepSize                  float64
	adjustment                int64
	hitsInSample              uint64
	missesInSample            uint64
	previousSampleHitRate     float64
	isWeighted                bool
	rand                      func() uint32
}

func newPolicy[K comparable, V any](isWeighted bool) *policy[K, V] {
	return &policy[K, V]{
		sketch:     newSketch[K](),
		window:     deque.NewLinked[K, V](isExp),
		probation:  deque.NewLinked[K, V](isExp),
		protected:  deque.NewLinked[K, V](isExp),
		isWeighted: isWeighted,
		rand:       xruntime.Fastrand,
	}
}

// access updates the eviction policy based on node accesses.
func (p *policy[K, V]) access(n node.Node[K, V]) {
	p.sketch.increment(n.Key())
	switch {
	case n.InWindow():
		reorder(p.window, n)
	case n.InMainProbation():
		p.reorderProbation(n)
	case n.InMainProtected():
		reorder(p.protected, n)
	}
	p.hitsInSample++
}

// add adds node to the eviction policy.
func (p *policy[K, V]) add(n node.Node[K, V], evictNode func(n node.Node[K, V], nowNanos int64)) {
	nodeWeight := uint64(n.Weight())

	p.weightedSize += nodeWeight
	p.windowWeightedSize += nodeWeight
	if p.weightedSize >= p.maximum>>1 {
		// Lazily initialize when close to the maximum
		capacity := p.maximum
		if p.isWeighted {
			//nolint:gosec // there's no overflow
			capacity = uint64(p.window.Len()) + uint64(p.probation.Len()) + uint64(p.protected.Len())
		}
		p.sketch.ensureCapacity(capacity)
	}

	p.sketch.increment(n.Key())
	p.missesInSample++

	// ignore out-of-order write operations
	if !n.IsAlive() {
		return
	}

	switch {
	case nodeWeight > p.maximum:
		evictNode(n, 0)
	case nodeWeight > p.windowMaximum:
		p.window.PushFront(n)
	default:
		p.window.PushBack(n)
	}
}

func (p *policy[K, V]) update(n, old node.Node[K, V], evictNode func(n node.Node[K, V], nowNanos int64)) {
	nodeWeight := uint64(n.Weight())
	p.updateNode(n, old)
	switch {
	case n.InWindow():
		p.windowWeightedSize += nodeWeight
		switch {
		case nodeWeight > p.maximum:
			evictNode(n, 0)
		case nodeWeight <= p.windowMaximum:
			p.access(n)
		case p.window.Contains(n):
			p.window.MoveToFront(n)
		}
	case n.InMainProbation():
		if nodeWeight <= p.maximum {
			p.access(n)
		} else {
			evictNode(n, 0)
		}
	case n.InMainProtected():
		p.mainProtectedWeightedSize += nodeWeight
		if nodeWeight <= p.maximum {
			p.access(n)
		} else {
			evictNode(n, 0)
		}
	}

	p.weightedSize += nodeWeight
}

func (p *policy[K, V]) updateNode(n, old node.Node[K, V]) {
	n.SetQueueType(old.GetQueueType())

	switch {
	case n.InWindow():
		p.window.UpdateNode(n, old)
	case n.InMainProbation():
		p.probation.UpdateNode(n, old)
	default:
		p.protected.UpdateNode(n, old)
	}
	p.makeDead(old)
}

// delete deletes node from the eviction policy.
func (p *policy[K, V]) delete(n node.Node[K, V]) {
	// add may not have been processed yet
	switch {
	case n.InWindow():
		p.window.Delete(n)
	case n.InMainProbation():
		p.probation.Delete(n)
	default:
		p.protected.Delete(n)
	}
	p.makeDead(n)
}

func (p *policy[K, V]) makeDead(n node.Node[K, V]) {
	if !n.IsDead() {
		nodeWeight := uint64(n.Weight())
		if n.InWindow() {
			p.windowWeightedSize -= nodeWeight
		} else if n.InMainProtected() {
			p.mainProtectedWeightedSize -= nodeWeight
		}
		p.weightedSize -= nodeWeight
		n.Die()
	}
}

func (p *policy[K, V]) setMaximumSize(maximum uint64) {
	if maximum == p.maximum {
		return
	}

	window := maximum - uint64(percentMain*float64(maximum))
	mainProtected := uint64(percentMainProtected * float64(maximum-window))

	p.maximum = maximum
	p.windowMaximum = window
	p.mainProtectedMaximum = mainProtected

	p.hitsInSample = 0
	p.missesInSample = 0
	p.stepSize = -hillClimberStepPercent * float64(maximum)

	if p.sketch != nil && !p.isWeighted && p.weightedSize >= (maximum>>1) {
		// Lazily initialize when close to the maximum size
		p.sketch.ensureCapacity(maximum)
	}
}

// Promote the node from probation to protected on access.
func (p *policy[K, V]) reorderProbation(n node.Node[K, V]) {
	nodeWeight := uint64(n.Weight())

	if p.probation.NotContains(n) {
		// Ignore stale accesses for an entry that is no longer present
		return
	} else if nodeWeight > p.mainProtectedMaximum {
		reorder(p.probation, n)
		return
	}

	// If the protected space exceeds its maximum, the LRU items are demoted to the probation space.
	// This is deferred to the adaption phase at the end of the maintenance cycle.
	p.mainProtectedWeightedSize += nodeWeight
	p.probation.Delete(n)
	p.protected.PushBack(n)
	n.MakeMainProtected()
}

func (p *policy[K, V]) evictNodes(evictNode func(n node.Node[K, V], nowNanos int64)) {
	candidate := p.evictFromWindow()
	p.evictFromMain(candidate, evictNode)
}

func (p *policy[K, V]) evictFromWindow() node.Node[K, V] {
	var first node.Node[K, V]
	n := p.window.Head()
	for p.windowWeightedSize > p.windowMaximum {
		// The pending operations will adjust the size to reflect the correct weight
		if node.Equals(n, nil) {
			break
		}

		next := n.Next()
		nodeWeight := uint64(n.Weight())
		if nodeWeight != 0 {
			n.MakeMainProbation()
			p.window.Delete(n)
			p.probation.PushBack(n)
			if first == nil {
				first = n
			}

			p.windowWeightedSize -= nodeWeight
		}
		n = next
	}
	return first
}

func (p *policy[K, V]) evictFromMain(candidate node.Node[K, V], evictNode func(n node.Node[K, V], nowNanos int64)) {
	victimQueue := node.InMainProbationQueue
	candidateQueue := node.InMainProbationQueue
	victim := p.probation.Head()
	for p.weightedSize > p.maximum {
		// Search the admission window for additional candidates
		if node.Equals(candidate, nil) && candidateQueue == node.InMainProbationQueue {
			candidate = p.window.Head()
			candidateQueue = node.InWindowQueue
		}

		// Try evicting from the protected and window queues
		if node.Equals(candidate, nil) && node.Equals(victim, nil) {
			if victimQueue == node.InMainProbationQueue {
				victim = p.protected.Head()
				victimQueue = node.InMainProtectedQueue
				continue
			} else if victimQueue == node.InMainProtectedQueue {
				victim = p.window.Head()
				victimQueue = node.InWindowQueue
				continue
			}

			// The pending operations will adjust the size to reflect the correct weight
			break
		}

		// Skip over entries with zero weight
		if !node.Equals(victim, nil) && victim.Weight() == 0 {
			victim = victim.Next()
			continue
		} else if !node.Equals(candidate, nil) && candidate.Weight() == 0 {
			candidate = candidate.Next()
			continue
		}

		// Evict immediately if only one of the entries is present
		if node.Equals(victim, nil) {
			previous := candidate.Next()
			evict := candidate
			candidate = previous
			evictNode(evict, 0)
			continue
		} else if node.Equals(candidate, nil) {
			evict := victim
			victim = victim.Next()
			evictNode(evict, 0)
			continue
		}

		// Evict immediately if both selected the same entry
		if node.Equals(candidate, victim) {
			victim = victim.Next()
			evictNode(candidate, 0)
			candidate = nil
			continue
		}

		// Evict immediately if an entry was deleted
		if !victim.IsAlive() {
			evict := victim
			victim = victim.Next()
			evictNode(evict, 0)
			continue
		} else if !candidate.IsAlive() {
			evict := candidate
			candidate = candidate.Next()
			evictNode(evict, 0)
			continue
		}

		// Evict immediately if the candidate's weight exceeds the maximum
		if uint64(candidate.Weight()) > p.maximum {
			evict := candidate
			candidate = candidate.Next()
			evictNode(evict, 0)
			continue
		}

		// Evict the entry with the lowest frequency
		if p.admit(candidate.Key(), victim.Key()) {
			evict := victim
			victim = victim.Next()
			evictNode(evict, 0)
			candidate = candidate.Next()
		} else {
			evict := candidate
			candidate = candidate.Next()
			evictNode(evict, 0)
		}
	}
}

func (p *policy[K, V]) admit(candidateKey, victimKey K) bool {
	victimFreq := p.sketch.frequency(victimKey)
	candidateFreq := p.sketch.frequency(candidateKey)
	if candidateFreq > victimFreq {
		return true
	}
	if candidateFreq >= admitHashdosThreshold {
		// The maximum frequency is 15 and halved to 7 after a reset to age the history. An attack
		// exploits that a hot candidate is rejected in favor of a hot victim. The threshold of a warm
		// candidate reduces the number of random acceptances to minimize the impact on the hit rate.
		return (p.rand() & 127) == 0
	}
	return false
}

func (p *policy[K, V]) climb() {
	p.determineAdjustment()
	p.demoteFromMainProtected()
	amount := p.adjustment
	if amount == 0 {
		return
	}
	if amount > 0 {
		p.increaseWindow()
	} else {
		p.decreaseWindow()
	}
}

func (p *policy[K, V]) determineAdjustment() {
	if p.sketch.isNotInitialized() {
		p.previousSampleHitRate = 0.0
		p.missesInSample = 0
		p.hitsInSample = 0
		return
	}

	requestCount := p.hitsInSample + p.missesInSample
	if requestCount < p.sketch.sampleSize {
		return
	}

	hitRate := float64(p.hitsInSample) / float64(requestCount)
	hitRateChange := hitRate - p.previousSampleHitRate
	amount := p.stepSize
	if hitRateChange < 0 {
		amount = -p.stepSize
	}
	var nextStepSize float64
	if abs(hitRateChange) >= hillClimberRestartThreshold {
		k := float64(-1)
		if amount >= 0 {
			k = float64(1)
		}
		nextStepSize = hillClimberStepPercent * float64(p.maximum) * k
	} else {
		nextStepSize = hillClimberStepDecayRate * amount
	}
	p.previousSampleHitRate = hitRate
	p.adjustment = int64(amount)
	p.stepSize = nextStepSize
	p.missesInSample = 0
	p.hitsInSample = 0
}

func (p *policy[K, V]) demoteFromMainProtected() {
	mainProtectedMaximum := p.mainProtectedMaximum
	mainProtectedWeightedSize := p.mainProtectedWeightedSize
	if mainProtectedWeightedSize <= mainProtectedMaximum {
		return
	}

	for i := 0; i < queueTransferThreshold; i++ {
		if mainProtectedWeightedSize <= mainProtectedMaximum {
			break
		}

		demoted := p.protected.PopFront()
		if node.Equals(demoted, nil) {
			break
		}
		demoted.MakeMainProbation()
		p.probation.PushBack(demoted)
		mainProtectedWeightedSize -= uint64(demoted.Weight())
	}

	p.mainProtectedWeightedSize = mainProtectedWeightedSize
}

func (p *policy[K, V]) increaseWindow() {
	if p.mainProtectedMaximum == 0 {
		return
	}

	quota := p.adjustment
	if p.mainProtectedMaximum < uint64(p.adjustment) {
		quota = int64(p.mainProtectedMaximum)
	}
	p.mainProtectedMaximum -= uint64(quota)
	p.windowMaximum += uint64(quota)
	p.demoteFromMainProtected()

	for i := 0; i < queueTransferThreshold; i++ {
		candidate := p.probation.Head()
		probation := true
		if node.Equals(candidate, nil) || quota < int64(candidate.Weight()) {
			candidate = p.protected.Head()
			probation = false
		}
		if node.Equals(candidate, nil) {
			break
		}

		weight := uint64(candidate.Weight())
		if quota < int64(weight) {
			break
		}

		quota -= int64(weight)
		if probation {
			p.probation.Delete(candidate)
		} else {
			p.mainProtectedWeightedSize -= weight
			p.protected.Delete(candidate)
		}
		p.windowWeightedSize += weight
		p.window.PushBack(candidate)
		candidate.MakeWindow()
	}

	p.mainProtectedMaximum += uint64(quota)
	p.windowMaximum -= uint64(quota)
	p.adjustment = quota
}

func (p *policy[K, V]) decreaseWindow() {
	if p.windowMaximum <= 1 {
		return
	}

	quota := -p.adjustment
	windowMaximum := max(0, p.windowMaximum-1)
	if windowMaximum < uint64(-p.adjustment) {
		quota = int64(windowMaximum)
	}
	p.mainProtectedMaximum += uint64(quota)
	p.windowMaximum -= uint64(quota)

	for i := 0; i < queueTransferThreshold; i++ {
		candidate := p.window.Head()
		if node.Equals(candidate, nil) {
			break
		}

		weight := int64(candidate.Weight())
		if quota < weight {
			break
		}

		quota -= weight
		p.windowWeightedSize -= uint64(weight)
		p.window.Delete(candidate)
		p.probation.PushBack(candidate)
		candidate.MakeMainProbation()
	}

	p.mainProtectedMaximum -= uint64(quota)
	p.windowMaximum += uint64(quota)
	p.adjustment = -quota
}

func abs(a float64) float64 {
	if a < 0 {
		return -a
	}
	return a
}

func reorder[K comparable, V any](d *deque.Linked[K, V], n node.Node[K, V]) {
	if d.Contains(n) {
		d.MoveToBack(n)
	}
}
