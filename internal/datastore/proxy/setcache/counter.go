package setcache

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/authzed/spicedb/pkg/cache"
)

// AccessCounter counts accesses per set key.
type AccessCounter interface {
	// Touch records one access to key and returns its count.
	Touch(key string) uint64
	Close()
}

type counterKey string

func (k counterKey) KeyString() string { return string(k) }

type otterCounter struct {
	mu       sync.Mutex
	counters cache.Cache[counterKey, *atomic.Uint64]
}

// NewAccessCounter returns an AccessCounter that keeps one exact counter per key in an otter cache.
// A counter expires after window passes with no touch.
// When the cache is full, it can evict a counter or refuse a new one, and the next Touch starts that count again at 1.
// Set keys include the revision, so a count is the number of accesses at one revision.
func NewAccessCounter(maxCost int64, window time.Duration) (AccessCounter, error) {
	c, err := cache.NewStandardCache[counterKey, *atomic.Uint64](&cache.Config{
		MaxCost:    maxCost,
		DefaultTTL: window,
	})
	if err != nil {
		return nil, err
	}
	return &otterCounter{counters: c}, nil
}

func (e *otterCounter) Touch(key string) uint64 {
	ck := counterKey(key)
	if ctr, ok := e.counters.Get(ck); ok {
		return ctr.Add(1)
	}
	e.mu.Lock()
	ctr, ok := e.counters.Get(ck)
	if !ok {
		ctr = new(atomic.Uint64)
		// Otter may refuse admission. The next Touch then creates the counter again.
		// Counts are approximate.
		e.counters.Set(ck, ctr, int64(len(key))+64)
	}
	e.mu.Unlock()
	return ctr.Add(1)
}

func (e *otterCounter) Close() { e.counters.Close() }
