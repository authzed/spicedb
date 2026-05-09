// Copyright (c) 2024 Alexey Mayshev and contributors. All rights reserved.
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

package expiration

import (
	"math"
	"math/bits"
	"time"

	"github.com/maypok86/otter/v2/internal/generated/node"
	"github.com/maypok86/otter/v2/internal/xmath"
)

var (
	buckets = []uint64{64, 64, 32, 4, 1}
	spans   = []uint64{
		xmath.RoundUpPowerOf264(uint64((1 * time.Second).Nanoseconds())),             // 1.07s
		xmath.RoundUpPowerOf264(uint64((1 * time.Minute).Nanoseconds())),             // 1.14m
		xmath.RoundUpPowerOf264(uint64((1 * time.Hour).Nanoseconds())),               // 1.22h
		xmath.RoundUpPowerOf264(uint64((24 * time.Hour).Nanoseconds())),              // 1.63d
		buckets[3] * xmath.RoundUpPowerOf264(uint64((24 * time.Hour).Nanoseconds())), // 6.5d
		buckets[3] * xmath.RoundUpPowerOf264(uint64((24 * time.Hour).Nanoseconds())), // 6.5d
	}
	shift = []uint64{
		uint64(bits.TrailingZeros64(spans[0])),
		uint64(bits.TrailingZeros64(spans[1])),
		uint64(bits.TrailingZeros64(spans[2])),
		uint64(bits.TrailingZeros64(spans[3])),
		uint64(bits.TrailingZeros64(spans[4])),
	}
)

type Variable[K comparable, V any] struct {
	wheel [][]node.Node[K, V]
	time  uint64
}

func NewVariable[K comparable, V any](nodeManager *node.Manager[K, V]) *Variable[K, V] {
	wheel := make([][]node.Node[K, V], len(buckets))
	for i := 0; i < len(wheel); i++ {
		wheel[i] = make([]node.Node[K, V], buckets[i])
		for j := 0; j < len(wheel[i]); j++ {
			var k K
			var v V
			fn := nodeManager.Create(k, v, math.MaxInt64, math.MaxInt64, 1)
			fn.SetPrevExp(fn)
			fn.SetNextExp(fn)
			wheel[i][j] = fn
		}
	}
	return &Variable[K, V]{
		wheel: wheel,
	}
}

// findBucket determines the bucket that the timer event should be added to.
func (v *Variable[K, V]) findBucket(expiration uint64) node.Node[K, V] {
	duration := expiration - v.time
	length := len(v.wheel) - 1
	for i := 0; i < length; i++ {
		if duration < spans[i+1] {
			ticks := expiration >> shift[i]
			index := ticks & (buckets[i] - 1)
			return v.wheel[i][index]
		}
	}
	return v.wheel[length][0]
}

// Add schedules a timer event for the node.
func (v *Variable[K, V]) Add(n node.Node[K, V]) {
	//nolint:gosec // there is no overflow
	root := v.findBucket(uint64(n.ExpiresAt()))
	link(root, n)
}

// Delete removes a timer event for this entry if present.
func (v *Variable[K, V]) Delete(n node.Node[K, V]) {
	unlink(n)
	n.SetNextExp(nil)
	n.SetPrevExp(nil)
}

func (v *Variable[K, V]) DeleteExpired(nowNanos int64, expireNode func(n node.Node[K, V], nowNanos int64)) {
	currentTime := uint64(nowNanos)
	prevTime := v.time
	v.time = currentTime

	for i := 0; i < len(shift); i++ {
		previousTicks := prevTime >> shift[i]
		currentTicks := currentTime >> shift[i]
		delta := currentTicks - previousTicks
		if delta == 0 {
			break
		}

		v.deleteExpiredFromBucket(i, previousTicks, delta, expireNode)
	}
}

func (v *Variable[K, V]) deleteExpiredFromBucket(
	index int,
	prevTicks, delta uint64,
	expireNode func(n node.Node[K, V], nowNanos int64),
) {
	mask := buckets[index] - 1
	steps := min(delta+1, buckets[index])
	start := prevTicks & mask
	end := start + steps
	timerWheel := v.wheel[index]
	for i := start; i < end; i++ {
		root := timerWheel[i&mask]
		n := root.NextExp()
		root.SetPrevExp(root)
		root.SetNextExp(root)

		for !node.Equals(n, root) {
			next := n.NextExp()
			n.SetPrevExp(nil)
			n.SetNextExp(nil)

			if uint64(n.ExpiresAt()) < v.time {
				expireNode(n, int64(v.time))
			} else {
				v.Add(n)
			}

			n = next
		}
	}
}

// link adds the entry at the tail of the bucket's list.
func link[K comparable, V any](root, n node.Node[K, V]) {
	n.SetPrevExp(root.PrevExp())
	n.SetNextExp(root)

	root.PrevExp().SetNextExp(n)
	root.SetPrevExp(n)
}

// unlink removes the entry from its bucket, if scheduled.
func unlink[K comparable, V any](n node.Node[K, V]) {
	next := n.NextExp()
	if !node.Equals(next, nil) {
		prev := n.PrevExp()
		next.SetPrevExp(prev)
		prev.SetNextExp(next)
	}
}
