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
	"math"
	"math/bits"
	"sync/atomic"

	"github.com/maypok86/otter/v2/internal/xmath"
	"github.com/maypok86/otter/v2/internal/xruntime"
)

const (
	resetMask = 0x7777777777777777
	oneMask   = 0x1111111111111111
)

// sketch is a probabilistic multiset for estimating the popularity of an element within a time window. The
// maximum frequency of an element is limited to 15 (4-bits) and an aging process periodically
// halves the popularity of all elements.
type sketch[K comparable] struct {
	table         []uint64
	sampleSize    uint64
	blockMask     uint64
	size          uint64
	hasher        xruntime.Hasher[K]
	isInitialized atomic.Bool
}

func newSketch[K comparable]() *sketch[K] {
	return &sketch[K]{
		hasher: xruntime.NewHasher[K](),
	}
}

func (s *sketch[K]) ensureCapacity(maximumSize uint64) {
	if uint64(len(s.table)) >= maximumSize {
		return
	}

	if !s.isInitialized.Load() {
		s.isInitialized.Store(true)
	}
	newSize := xmath.RoundUpPowerOf264(maximumSize)
	if newSize < 8 {
		newSize = 8
	}

	s.table = make([]uint64, newSize)
	s.sampleSize = 10
	if maximumSize != 0 {
		s.sampleSize = 10 * maximumSize
	}
	s.blockMask = (uint64(len(s.table)) >> 3) - 1
	s.size = 0
	s.hasher = xruntime.NewHasher[K]()
}

func (s *sketch[K]) isNotInitialized() bool {
	return !s.isInitialized.Load()
}

func (s *sketch[K]) frequency(k K) uint64 {
	if s.isNotInitialized() {
		return 0
	}

	frequency := uint64(math.MaxUint64)
	blockHash := s.hash(k)
	counterHash := rehash(blockHash)
	block := (blockHash & s.blockMask) << 3
	for i := uint64(0); i < 4; i++ {
		h := counterHash >> (i << 3)
		index := (h >> 1) & 15
		offset := h & 1
		slot := block + offset + (i << 1)
		count := (s.table[slot] >> (index << 2)) & 0xf
		frequency = min(frequency, count)
	}

	return frequency
}

func (s *sketch[K]) increment(k K) {
	if s.isNotInitialized() {
		return
	}

	blockHash := s.hash(k)
	counterHash := rehash(blockHash)
	block := (blockHash & s.blockMask) << 3

	// Loop unrolling improves throughput by 10m ops/s
	h0 := counterHash
	h1 := counterHash >> 8
	h2 := counterHash >> 16
	h3 := counterHash >> 24

	index0 := (h0 >> 1) & 15
	index1 := (h1 >> 1) & 15
	index2 := (h2 >> 1) & 15
	index3 := (h3 >> 1) & 15

	slot0 := block + (h0 & 1)
	slot1 := block + (h1 & 1) + 2
	slot2 := block + (h2 & 1) + 4
	slot3 := block + (h3 & 1) + 6

	added := s.incrementAt(slot0, index0)
	added = s.incrementAt(slot1, index1) || added
	added = s.incrementAt(slot2, index2) || added
	added = s.incrementAt(slot3, index3) || added

	if added {
		s.size++
		if s.size == s.sampleSize {
			s.reset()
		}
	}
}

func (s *sketch[K]) incrementAt(i, j uint64) bool {
	offset := j << 2
	mask := uint64(0xf) << offset
	if (s.table[i] & mask) != mask {
		s.table[i] += uint64(1) << offset
		return true
	}
	return false
}

func (s *sketch[K]) reset() {
	count := 0
	for i := 0; i < len(s.table); i++ {
		count += bits.OnesCount64(s.table[i] & oneMask)
		s.table[i] = (s.table[i] >> 1) & resetMask
	}
	//nolint:gosec // there's no overflow
	s.size = (s.size - (uint64(count) >> 2)) >> 1
}

func (s *sketch[K]) hash(k K) uint64 {
	return spread(s.hasher.Hash(k))
}

func spread(h uint64) uint64 {
	h ^= h >> 17
	h *= 0xed5ad4bb
	h ^= h >> 11
	h *= 0xac4c1b51
	h ^= h >> 15
	return h
}

func rehash(h uint64) uint64 {
	h *= 0x31848bab
	h ^= h >> 14
	return h
}
