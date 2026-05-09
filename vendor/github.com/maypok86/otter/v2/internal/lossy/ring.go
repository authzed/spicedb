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
// https://github.com/ben-manes/caffeine/blob/master/caffeine/src/main/java/com/github/benmanes/caffeine/cache/BoundedBuffer.java

package lossy

import (
	"sync/atomic"
	"unsafe"

	"github.com/maypok86/otter/v2/internal/generated/node"
	"github.com/maypok86/otter/v2/internal/xruntime"
)

// Status is the result of adding a node to the buffer.
type Status int8

const (
	// Success means that the node was added.
	Success Status = 0
	// Failed means that the CAS failed.
	Failed Status = -1
	// Full means that the buffer is full.
	Full Status = 1
)

const (
	// The maximum number of elements per buffer.
	bufferSize = 16
	mask       = uint64(bufferSize - 1)
)

// ring is a circular ring buffer stores the elements being transferred by the producers to the consumer.
// the monotonically increasing count of reads and writes allow indexing sequentially to the next
// element location based upon a power-of-two sizing.
//
// The producers race to read the counts, check if there is available capacity, and if so then try
// once to CAS to the next write count. If the increment is successful then the producer lazily
// publishes the element. The producer does not retry or block when unsuccessful due to a failed
// CAS or the buffer being full.
//
// The consumer reads the counts and takes the available elements. The clearing of the elements
// and the next read count are lazily set.
//
// This implementation is striped to further increase concurrency by rehashing and dynamically
// adding new buffers when contention is detected, up to an internal maximum. When rehashing in
// order to discover an available buffer, the producer may retry adding its element to determine
// whether it found a satisfactory buffer or if resizing is necessary.
type ring[K comparable, V any] struct {
	head        atomic.Uint64
	_           [xruntime.CacheLineSize - unsafe.Sizeof(atomic.Uint64{})]byte
	tail        atomic.Uint64
	_           [xruntime.CacheLineSize - unsafe.Sizeof(atomic.Uint64{})]byte
	nodeManager *node.Manager[K, V]
	buffer      [bufferSize]unsafe.Pointer // node.Node[K, V]
}

func newRing[K comparable, V any](nodeManager *node.Manager[K, V], n node.Node[K, V]) *ring[K, V] {
	r := &ring[K, V]{
		nodeManager: nodeManager,
	}
	r.buffer[0] = n.AsPointer()
	r.tail.Store(1)
	return r
}

func (r *ring[K, V]) add(n node.Node[K, V]) Status {
	head := r.head.Load()
	tail := r.tail.Load()
	size := tail - head
	if size >= bufferSize {
		return Full
	}

	if r.tail.CompareAndSwap(tail, tail+1) {
		atomic.StorePointer(&r.buffer[tail&mask], n.AsPointer())
		return Success
	}
	return Failed
}

func (r *ring[K, V]) drainTo(consumer func(n node.Node[K, V])) {
	head := r.head.Load()
	tail := r.tail.Load()
	size := tail - head
	if size == 0 {
		return
	}

	nm := r.nodeManager
	for head != tail {
		index := head & mask
		ptr := atomic.LoadPointer(&r.buffer[index])
		if ptr == nil {
			// not published.
			break
		}
		atomic.StorePointer(&r.buffer[index], nil)
		consumer(nm.FromPointer(ptr))
		head++
	}
	r.head.Store(head)
}

func (r *ring[K, V]) len() int {
	//nolint:gosec // there is no overflow
	return int(r.tail.Load() - r.head.Load())
}

/*
func (r *ring[K, V]) clear() {
	for i := 0; i < bufferSize; i++ {
		atomic.StorePointer(&r.buffer[i], nil)
	}
	r.head.Store(0)
	r.tail.Store(0)
}
*/
