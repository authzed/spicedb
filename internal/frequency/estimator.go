// Package frequency estimates recent accesses per key in fixed memory.
//
// The estimator is a count-min sketch with a doorkeeper Bloom filter.
// Every window, it halves all counts and clears the doorkeeper.
// Thus a count is a decayed count, not an exact count in a fixed window:
// a key with a steady rate of r touches per window reads between r and 2r,
// and the count of an idle key decays toward 0.
// Hash collisions can make a count too high, but never too low.
// Concurrent touches of one key can lose a few increments.
//
// This package does not use the sketch of otter, because that sketch is unexported.
// Also, its counts stop at 15, it halves its counts after a number of samples and not after a time window,
// and it has no count of all touches.
package frequency

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
)

// Estimator estimates recent access counts per key and of all keys.
// A key with a steady rate of r touches per window reads between r and 2r.
// The count of an idle key decays toward 0.
// Estimator has one implementation. The interface lets tests wrap or replace the estimator.
type Estimator interface {
	// Touch records one access to key and returns its decayed count.
	// It does not keep key and does not allocate.
	Touch(key []byte) uint64

	// Total returns the decayed count of all touches.
	Total() uint64

	// Close stops the decay. Calls after the first call do nothing.
	Close()
}

const (
	// rows is the number of counter rows in the sketch.
	rows = 4

	// doorkeeperHashes is the number of bit positions for each key in the doorkeeper.
	doorkeeperHashes = 3

	// doorkeeperBitsPerCounter is the number of doorkeeper bits for each counter in one row.
	doorkeeperBitsPerCounter = 8

	// minWidth is the minimum number of counters in one row.
	minWidth = 64

	// maxWidth is the maximum number of counters in one row.
	maxWidth = 1 << 30
)

// NewEstimator returns an Estimator that uses at most maxCost bytes and halves its counts every window.
// A maxCost below the cost of the minimum width gives the minimum width.
func NewEstimator(maxCost int64, window time.Duration) (Estimator, error) {
	if window <= 0 {
		return nil, errors.New("frequency estimator window must be positive")
	}

	ticker := time.NewTicker(window)
	s := newSketch(maxCost, ticker.C)
	s.stopTicker = ticker.Stop
	return s, nil
}

// sketch is a count-min sketch with conservative update, a doorkeeper, and periodic halving.
type sketch struct {
	// width is the number of counters in one row. It is a power of two.
	width uint64

	// counters holds rows rows of width counters, row after row.
	counters []atomic.Uint32

	// doorkeeper is a Bloom filter of doorkeeperBitsPerCounter*width bits.
	doorkeeper []atomic.Uint64

	// total is the decayed count of all touches.
	total atomic.Uint64

	// stopTicker stops the source of the decay ticks.
	stopTicker func()

	// stop tells run to exit, and done closes when run exits.
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// newSketch returns a sketch that decays on each value from ticks.
func newSketch(maxCost int64, ticks <-chan time.Time) *sketch {
	width := widthFor(maxCost)
	s := &sketch{
		width:      width,
		counters:   make([]atomic.Uint32, rows*width),
		doorkeeper: make([]atomic.Uint64, doorkeeperBitsPerCounter*width/64),
		stopTicker: func() {},
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	go s.run(ticks)
	return s
}

// widthFor returns the largest power-of-two row width whose cost fits maxCost.
// The result is between minWidth and maxWidth.
func widthFor(maxCost int64) uint64 {
	width := uint64(minWidth)
	for width < maxWidth && (&sketch{width: 2 * width}).costBytes() <= maxCost {
		width *= 2
	}
	return width
}

// costBytes returns the memory of the counters and the doorkeeper for s.width.
// The doorkeeper has doorkeeperBitsPerCounter bits for each counter in a row.
func (s *sketch) costBytes() int64 {
	// nolint:gosec
	// G115: the width is at most 2*maxWidth, so the value is in range.
	return int64(s.width*rows*4 + doorkeeperBitsPerCounter*s.width/8)
}

// run decays s on each value from ticks until Close.
func (s *sketch) run(ticks <-chan time.Time) {
	defer close(s.done)
	for {
		select {
		case <-s.stop:
			return

		case <-ticks:
			s.decay()
		}
	}
}

// keyHash holds the two hash halves for double hashing.
type keyHash struct {
	h1, h2 uint64
}

// hashKey splits one xxhash of key into the two halves for double hashing.
// h2 is odd, so the hash sequence visits every position of a power-of-two width.
func hashKey(key []byte) keyHash {
	h := xxhash.Sum64(key)
	return keyHash{h1: h & math.MaxUint32, h2: (h >> 32) | 1}
}

// index returns the counter index of h in the given row.
func (s *sketch) index(h keyHash, row uint64) uint64 {
	return row*s.width + ((h.h1 + row*h.h2) & (s.width - 1))
}

// doorkeeperBit returns the bit position of h for doorkeeper hash number hashNum.
// The positions continue the hash sequence after the rows, so they are independent of the row indexes.
func (s *sketch) doorkeeperBit(h keyHash, hashNum uint64) uint64 {
	return (h.h1 + (rows+hashNum)*h.h2) & (doorkeeperBitsPerCounter*s.width - 1)
}

// doorkeeperHas reports whether all doorkeeper bits of h are set.
func (s *sketch) doorkeeperHas(h keyHash) bool {
	for hashNum := range uint64(doorkeeperHashes) {
		bit := s.doorkeeperBit(h, hashNum)
		if s.doorkeeper[bit/64].Load()&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// doorkeeperAdd sets all doorkeeper bits of h.
func (s *sketch) doorkeeperAdd(h keyHash) {
	for hashNum := range uint64(doorkeeperHashes) {
		bit := s.doorkeeperBit(h, hashNum)
		s.doorkeeper[bit/64].Or(1 << (bit % 64))
	}
}

// sketchMin returns the minimum counter of h across all rows.
func (s *sketch) sketchMin(h keyHash) uint32 {
	minimum := uint32(math.MaxUint32)
	for row := range uint64(rows) {
		minimum = min(minimum, s.counters[s.index(h, row)].Load())
	}
	return minimum
}

// Touch records one access to key and returns its decayed count.
// The first touch of a key in a window sets only its doorkeeper bits.
// Later touches increment the sketch rows that hold the minimum count for the key.
// The count is 1 plus the sketch minimum.
// A key with a steady rate of r touches per window reads between r and 2r.
// The count of an idle key decays toward 0.
func (s *sketch) Touch(key []byte) uint64 {
	s.total.Add(1)

	h := hashKey(key)
	if !s.doorkeeperHas(h) {
		s.doorkeeperAdd(h)
		return 1 + uint64(s.sketchMin(h))
	}

	// A touch that loses every row to concurrent touches retries with the new minimum.
	for {
		minimum := s.sketchMin(h)
		if minimum == math.MaxUint32 {
			return 1 + uint64(minimum)
		}

		incremented := false
		for row := range uint64(rows) {
			if s.counters[s.index(h, row)].CompareAndSwap(minimum, minimum+1) {
				incremented = true
			}
		}

		if incremented {
			return 2 + uint64(minimum)
		}
	}
}

// estimate returns the count of key without a touch.
func (s *sketch) estimate(key []byte) uint64 {
	h := hashKey(key)
	if !s.doorkeeperHas(h) {
		return uint64(s.sketchMin(h))
	}

	return 1 + uint64(s.sketchMin(h))
}

// Total returns the decayed count of all touches.
func (s *sketch) Total() uint64 { return s.total.Load() }

// decay halves every counter and the total, and clears the doorkeeper.
// Concurrent touches stay correct, and each counter halves exactly once.
func (s *sketch) decay() {
	for i := range s.counters {
		c := &s.counters[i]
		for {
			v := c.Load()
			if v == 0 || c.CompareAndSwap(v, v>>1) {
				break
			}
		}
	}

	for {
		v := s.total.Load()
		if v == 0 || s.total.CompareAndSwap(v, v>>1) {
			break
		}
	}

	for i := range s.doorkeeper {
		s.doorkeeper[i].Store(0)
	}
}

// Close stops the decay goroutine and waits for it to exit.
func (s *sketch) Close() {
	s.closeOnce.Do(func() {
		s.stopTicker()
		close(s.stop)
		<-s.done
	})
}
