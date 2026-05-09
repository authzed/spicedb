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
//
// This is a port of lossy buffers from Caffeine.
// https://github.com/ben-manes/caffeine/blob/master/caffeine/src/main/java/com/github/benmanes/caffeine/cache/StripedBuffer.java

package lossy

import (
	"sync"
	"sync/atomic"

	"github.com/maypok86/otter/v2/internal/generated/node"
	"github.com/maypok86/otter/v2/internal/xruntime"
)

const (
	attempts = 3
)

// pool for P tokens.
var tokenPool sync.Pool

// a P token is used to point at the current OS thread (P)
// on which the goroutine is run; exact identity of the thread,
// as well as P migration tolerance, is not important since
// it's used to as a best effort mechanism for assigning
// concurrent operations (goroutines) to different stripes of
// the Adder.
type token struct {
	idx     uint32
	padding [xruntime.CacheLineSize - 4]byte
}

type striped[K comparable, V any] struct {
	buffers []atomic.Pointer[ring[K, V]]
	len     int
}

// Striped is a multiple-producer / single-consumer buffer that rejects new elements if it is full or
// fails spuriously due to contention. Unlike a queue and stack, a buffer does not guarantee an
// ordering of elements in either FIFO or LIFO order.
type Striped[K comparable, V any] struct {
	nodeManager *node.Manager[K, V]
	maxLen      int
	striped     atomic.Pointer[striped[K, V]]
	busy        atomic.Uint32
}

func NewStriped[K comparable, V any](maxLen int, nodeManager *node.Manager[K, V]) *Striped[K, V] {
	return &Striped[K, V]{
		nodeManager: nodeManager,
		maxLen:      maxLen,
	}
}

// Add inserts the specified element into this buffer if it is possible to do so immediately without
// violating capacity restrictions. The addition is allowed to fail spuriously if multiple
// goroutines insert concurrently.
func (s *Striped[K, V]) Add(n node.Node[K, V]) Status {
	t, ok := tokenPool.Get().(*token)
	if !ok {
		t = &token{
			idx: xruntime.Fastrand(),
		}
	}
	defer tokenPool.Put(t)

	bs := s.striped.Load()
	if bs == nil {
		return s.expandOrRetry(n, t, true)
	}

	//nolint:gosec // len will never overflow uint32
	buffer := bs.buffers[t.idx&uint32(bs.len-1)].Load()
	if buffer == nil {
		return s.expandOrRetry(n, t, true)
	}

	result := buffer.add(n)
	if result == Failed {
		return s.expandOrRetry(n, t, false)
	}

	return result
}

func (s *Striped[K, V]) expandOrRetry(n node.Node[K, V], t *token, wasUncontended bool) Status {
	result := Failed
	// True if last slot nonempty.
	collide := true

	for attempt := 0; attempt < attempts; attempt++ {
		bs := s.striped.Load()
		if bs != nil && bs.len > 0 {
			//nolint:gosec // len will never overflow uint32
			buffer := bs.buffers[t.idx&uint32(bs.len-1)].Load()
			//nolint:gocritic // the switch statement looks even worse here
			if buffer == nil {
				if s.busy.Load() == 0 && s.busy.CompareAndSwap(0, 1) {
					// Try to attach new buffer.
					created := false
					rs := s.striped.Load()
					if rs != nil && rs.len > 0 {
						// Recheck under lock.
						//nolint:gosec // len will never overflow uint32
						j := t.idx & uint32(rs.len-1)
						if rs.buffers[j].Load() == nil {
							rs.buffers[j].Store(newRing(s.nodeManager, n))
							created = true
						}
					}
					s.busy.Store(0)
					if created {
						result = Success
						break
					}
					// Slot is now non-empty.
					continue
				}
				collide = false
			} else if !wasUncontended {
				// CAS already known to fail.
				// Continue after rehash.
				wasUncontended = true
			} else {
				result = buffer.add(n)
				//nolint:gocritic // the switch statement looks even worse here
				if result != Failed {
					break
				} else if bs.len >= s.maxLen || s.striped.Load() != bs {
					// At max size or stale.
					collide = false
				} else if !collide {
					collide = true
				} else if s.busy.Load() == 0 && s.busy.CompareAndSwap(0, 1) {
					if s.striped.Load() == bs {
						length := bs.len << 1
						striped := &striped[K, V]{
							buffers: make([]atomic.Pointer[ring[K, V]], length),
							len:     length,
						}
						for j := 0; j < bs.len; j++ {
							striped.buffers[j].Store(bs.buffers[j].Load())
						}
						s.striped.Store(striped)
					}
					s.busy.Store(0)
					collide = false
					continue
				}
			}
			t.idx = xruntime.Fastrand()
		} else if s.busy.Load() == 0 && s.striped.Load() == bs && s.busy.CompareAndSwap(0, 1) {
			init := false
			if s.striped.Load() == bs {
				striped := &striped[K, V]{
					buffers: make([]atomic.Pointer[ring[K, V]], 1),
					len:     1,
				}
				striped.buffers[0].Store(newRing(s.nodeManager, n))
				s.striped.Store(striped)
				init = true
			}
			s.busy.Store(0)
			if init {
				result = Success
				break
			}
		}
	}

	return result
}

// DrainTo drains the buffer, sending each element to the consumer for processing. The caller must ensure
// that a consumer has exclusive read access to the buffer.
func (s *Striped[K, V]) DrainTo(consumer func(n node.Node[K, V])) {
	bs := s.striped.Load()
	if bs == nil {
		return
	}
	for i := 0; i < bs.len; i++ {
		b := bs.buffers[i].Load()
		if b != nil {
			b.drainTo(consumer)
		}
	}
}

func (s *Striped[K, V]) Len() int {
	result := 0
	bs := s.striped.Load()
	if bs == nil {
		return result
	}
	for i := 0; i < bs.len; i++ {
		b := bs.buffers[i].Load()
		if b == nil {
			continue
		}
		result += b.len()
	}
	return result
}

/*
func (s *Striped[K, V]) Clear() {
	bs := s.striped.Load()
	if bs == nil {
		return
	}
	for s.busy.Load() != 0 || !s.busy.CompareAndSwap(0, 1) {
		runtime.Gosched()
	}
	for i := 0; i < bs.len; i++ {
		b := bs.buffers[i].Load()
		if b != nil {
			b.clear()
		}
	}
	s.busy.Store(0)
}
*/
