package fullrelationcache

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/authzed/spicedb/pkg/cache"
)

// AccessCounter counts accesses per set key.
// The proxy uses the count to decide when an object#relation is hot enough to materialize.
//
// A count is exact for its key while its counter lives.
// Set keys include the revision, so a count is the number of reads at one revision.
// A counter expires when no Touch occurs for the window.
// If the cache evicts a counter or refuses to admit it, the next Touch restarts the count at 1.
// A restarted count can only delay a materialization. It can never make a served answer wrong.
type AccessCounter interface {
	// Touch records one access to key and returns its count.
	Touch(key string) uint64

	// Close releases the counter storage.
	Close()
}

type counterKey string

// KeyString implements the cache key interface.
func (k counterKey) KeyString() string { return string(k) }

type cacheCounter struct {
	mu       sync.Mutex
	counters cache.Cache[counterKey, *atomic.Uint64] // GUARDED_BY(mu)
}

// NewAccessCounter returns an AccessCounter that keeps one counter per key in a pkg/cache cache of maxCost bytes.
// A counter expires when no Touch occurs for window.
func NewAccessCounter(maxCost int64, window time.Duration) (AccessCounter, error) {
	c, err := cache.NewStandardCache[counterKey, *atomic.Uint64](&cache.Config{
		MaxCost:    maxCost,
		DefaultTTL: window,
	})
	if err != nil {
		return nil, err
	}
	return &cacheCounter{counters: c}, nil
}

func (e *cacheCounter) Touch(key string) uint64 {
	ck := counterKey(key)
	if ctr, ok := e.counters.Get(ck); ok {
		return ctr.Add(1)
	}
	e.mu.Lock()
	ctr, ok := e.counters.Get(ck)
	if !ok {
		ctr = new(atomic.Uint64)
		// The cache can refuse admission. The next Touch then creates the counter again.
		e.counters.Set(ck, ctr, int64(len(key))+64)
	}
	e.mu.Unlock()
	return ctr.Add(1)
}

func (e *cacheCounter) Close() { e.counters.Close() }
