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

package queue

import (
	"fmt"
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/maypok86/otter/v2/internal/xmath"
	"github.com/maypok86/otter/v2/internal/xruntime"
)

type buffer struct {
	data []unsafe.Pointer
}

func newBuffer(capacity uint64) *buffer {
	return &buffer{
		data: make([]unsafe.Pointer, capacity),
	}
}

// MPSC is an MPSC array queue which starts at initialCapacity and grows to maxCapacity in
// linked chunks of the initial size. The queue grows only when the current buffer is full and
// elements are not copied on resize, instead a link to the new buffer is stored in the old buffer
// for the consumer to follow.
type MPSC[T any] struct {
	producerIndex    atomic.Uint64
	_                [xruntime.CacheLineSize - 8]byte
	consumerBuffer   atomic.Pointer[buffer]
	consumerIndex    atomic.Uint64
	consumerMask     atomic.Uint64
	_                [xruntime.CacheLineSize - 8*3]byte
	producerBuffer   atomic.Pointer[buffer]
	producerLimit    atomic.Uint64
	producerMask     atomic.Uint64
	_                [xruntime.CacheLineSize - 8*2]byte
	jump             unsafe.Pointer
	maxQueueCapacity uint64
}

func NewMPSC[T any](initialCapacity, maxCapacity uint32) *MPSC[T] {
	if initialCapacity < 2 {
		panic(fmt.Sprintf("Initial capacity must be 2 or more. initialCapacity = %d", initialCapacity))
	}
	if maxCapacity < 4 {
		panic(fmt.Sprintf("Max capacity must be 4 or more. maxCapacity = %d", maxCapacity))
	}

	p2initialCapacity := xmath.RoundUpPowerOf2(initialCapacity)
	p2maxCapacity := xmath.RoundUpPowerOf2(maxCapacity)
	if p2maxCapacity < p2initialCapacity {
		s := fmt.Sprintf(
			"Initial capacity cannot exceed maximum capacity(both rounded up to a power of 2). initialCapacity = %d, maxCapacity = %d",
			initialCapacity,
			maxCapacity,
		)
		panic(s)
	}

	mask := uint64((p2initialCapacity - 1) << 1)
	buffer := newBuffer(uint64(p2initialCapacity) + 1)

	var zero T
	q := &MPSC[T]{
		maxQueueCapacity: uint64(p2maxCapacity) << 1,
		//nolint:gosec // it's ok
		jump: unsafe.Pointer(&zero),
	}
	q.consumerBuffer.Store(buffer)
	q.consumerMask.Store(mask)
	q.producerBuffer.Store(buffer)
	q.producerLimit.Store(mask)
	q.producerMask.Store(mask)

	return q
}

func (m *MPSC[T]) getNextBufferSize(buffer *buffer) uint64 {
	maxSize := m.maxQueueCapacity / 2
	bufferLength := uint64(len(buffer.data))
	if maxSize < bufferLength {
		panic(fmt.Sprintf("maxSize should be >= bufferLength. maxSize = %d, bufferLength = %d", maxSize, bufferLength))
	}
	newSize := 2 * (bufferLength - 1)
	return newSize + 1
}

func (m *MPSC[T]) getCurrentBufferCapacity(mask uint64) uint64 {
	if mask+2 == m.maxQueueCapacity {
		return m.maxQueueCapacity
	}
	return mask
}

func (m *MPSC[T]) availableInQueue(pIndex, cIndex uint64) uint64 {
	return m.maxQueueCapacity - (pIndex - cIndex)
}

func (m *MPSC[T]) capacity() int {
	//nolint:gosec // there's no overflow
	return int(m.maxQueueCapacity / 2)
}

func (m *MPSC[T]) TryPush(t *T) bool {
	var (
		mask   uint64
		pIndex uint64
		buffer *buffer
	)

	for {
		producerLimit := m.producerLimit.Load()
		pIndex = m.producerIndex.Load()
		// lower bit is indicative of resize, if we see it we spin until it's cleared
		if pIndex&1 == 1 {
			continue
		}
		// pIndex is even (lower bit is 0) -> actual index is (pIndex >> 1)

		// mask/buffer may get changed by resizing -> only use for array access after successful CAS.
		mask = m.producerMask.Load()
		buffer = m.producerBuffer.Load()
		// a successful CAS ties the ordering, lv(pIndex)-[mask/buffer]->cas(pIndex)

		// assumption behind this optimization is that queue is almost always empty or near empty
		if producerLimit <= pIndex {
			result := m.pushSlowPath(mask, pIndex, producerLimit)
			switch result {
			case 0:
				break
			case 1:
				continue
			case 2:
				return false
			case 3:
				m.resize(mask, buffer, pIndex, t)
				return true
			}
		}

		if m.producerIndex.CompareAndSwap(pIndex, pIndex+2) {
			break
		}
	}

	offset := modifiedCalcElementOffset(pIndex, mask)
	//nolint:gosec // it's ok
	atomic.StorePointer(&buffer.data[offset], unsafe.Pointer(t))
	return true
}

// We do not inline resize into this method because we do not resize on fill.
func (m *MPSC[T]) pushSlowPath(mask, pIndex, producerLimit uint64) uint8 {
	var result uint8 // 0 - goto pIndex CAS
	cIndex := m.consumerIndex.Load()
	bufferCapacity := m.getCurrentBufferCapacity(mask)

	switch {
	case cIndex+bufferCapacity > pIndex:
		if !m.producerLimit.CompareAndSwap(producerLimit, cIndex+bufferCapacity) {
			result = 1 // retry from top
		}
		// full and cannot grow
	case m.availableInQueue(pIndex, cIndex) <= 0:
		result = 2 // -> return false
	case m.producerIndex.CompareAndSwap(pIndex, pIndex+1):
		result = 3 // -> resize
	default:
		result = 1 // failed resize attempt, retry from top
	}

	return result
}

func (m *MPSC[T]) TryPop() *T {
	buffer := m.consumerBuffer.Load()
	index := m.consumerIndex.Load()
	mask := m.consumerMask.Load()

	offset := modifiedCalcElementOffset(index, mask)
	v := atomic.LoadPointer(&buffer.data[offset])
	if v == nil {
		if index == m.producerIndex.Load() {
			return nil
		}
		v = atomic.LoadPointer(&buffer.data[offset])
		for v == nil {
			v = atomic.LoadPointer(&buffer.data[offset])
		}
	}
	if v == m.jump {
		nextBuffer := m.getNextBuffer(buffer, mask)
		return m.newBufferTryPush(nextBuffer, index)
	}

	atomic.StorePointer(&buffer.data[offset], nil)
	m.consumerIndex.Store(index + 2)

	return (*T)(v)
}

func (m *MPSC[T]) Size() uint64 {
	if m == nil {
		return 0
	}
	// NOTE: because indices are on even numbers we cannot use the size util.

	// It is possible for a thread to be interrupted or reschedule between the read of the producer
	// and consumer indices, therefore protection is required to ensure size is within valid range.
	// In the event of concurrent polls/offers to this method the size is OVER estimated as we read
	// consumer index BEFORE the producer index.

	after := m.consumerIndex.Load()
	for {
		before := after
		currentProducerIndex := m.producerIndex.Load()
		after = m.consumerIndex.Load()
		if before == after {
			return (currentProducerIndex - after) >> 1
		}
	}
}

func (m *MPSC[T]) IsEmpty() bool {
	// Order matters!
	// Loading consumer before producer allows for producer increments after consumer index is read.
	// This ensures this method is conservative in its estimate. Note that as this is an MPMC there
	// is nothing we can do to make this an exact method.
	return m.consumerIndex.Load() == m.producerIndex.Load()
}

func (m *MPSC[T]) getNextBuffer(b *buffer, mask uint64) *buffer {
	nextBufferOffset := nextArrayOffset(mask)
	nextBuffer := (*buffer)(atomic.LoadPointer(&b.data[nextBufferOffset]))
	atomic.StorePointer(&b.data[nextBufferOffset], nil)
	if nextBuffer == nil {
		panic("nextBuffer should be != nil")
	}
	return nextBuffer
}

func (m *MPSC[T]) newBufferTryPush(b *buffer, index uint64) *T {
	offsetInNew := m.newBufferAndOffset(b, index)
	v := atomic.LoadPointer(&b.data[offsetInNew])
	if v == nil {
		panic("new buffer must have at least one element")
	}
	atomic.StorePointer(&b.data[offsetInNew], nil)
	m.consumerIndex.Store(index + 2)
	return (*T)(v)
}

func (m *MPSC[T]) newBufferAndOffset(b *buffer, index uint64) uint64 {
	m.consumerBuffer.Store(b)
	//nolint:gosec // there's no overflow
	mask := uint64(len(b.data)-2) << 1
	m.consumerMask.Store(mask)
	return modifiedCalcElementOffset(index, mask)
}

func (m *MPSC[T]) resize(oldMask uint64, oldBuffer *buffer, pIndex uint64, t *T) {
	newBufferLength := m.getNextBufferSize(oldBuffer)
	newBuffer := newBuffer(newBufferLength)

	m.producerBuffer.Store(newBuffer)
	newMask := (newBufferLength - 2) << 1
	m.producerMask.Store(newMask)

	offsetInOld := modifiedCalcElementOffset(pIndex, oldMask)
	offsetInNew := modifiedCalcElementOffset(pIndex, newMask)

	//nolint:gosec // it's ok
	atomic.StorePointer(&newBuffer.data[offsetInNew], unsafe.Pointer(t)) // element in new array
	//nolint:gosec // it's ok
	atomic.StorePointer(&oldBuffer.data[nextArrayOffset(oldMask)], unsafe.Pointer(newBuffer)) // buffer linked

	cIndex := m.consumerIndex.Load()
	availableInQueue := m.availableInQueue(pIndex, cIndex)
	if availableInQueue == 0 {
		panic(fmt.Sprintf("availableInQueue should be == . availableInQueue = %d", availableInQueue))
	}

	// Invalidate racing CASs
	// We never set the limit beyond the bounds of a buffer
	m.producerLimit.Store(pIndex + min(newMask, availableInQueue))

	// make resize visible to the other producers
	m.producerIndex.Store(pIndex + 2)

	// INDEX visible before ELEMENT, consistent with consumer expectation

	// make resize visible to consumer
	atomic.StorePointer(&oldBuffer.data[offsetInOld], m.jump)
}

func nextArrayOffset(mask uint64) uint64 {
	return modifiedCalcElementOffset(mask+2, math.MaxUint64)
}

// This method assumes index is actually (index << 1) because lower bit is used for resize. This
// is compensated for by reducing the element shift. The computation is constant folded, so
// there's no cost.
func modifiedCalcElementOffset(index, mask uint64) uint64 {
	return (index & mask) >> 1
}
