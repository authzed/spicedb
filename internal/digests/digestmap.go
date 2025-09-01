package digests

import (
	"sync"

	"github.com/caio/go-tdigest/v4"
	"github.com/puzpuzpuz/xsync/v4"

	log "github.com/authzed/spicedb/internal/logging"
)

const defaultTDigestCompression = float64(1000)

// DigestMap is a thread-safe map for storing t-digests keyed by string identifiers.
// It's designed for concurrent access patterns where multiple goroutines may read
// and write digest statistics simultaneously.
type DigestMap struct {
	m *xsync.Map[string, *digestAndLock]
}

// NewDigestMap creates a new DigestMap instance.
func NewDigestMap() *DigestMap {
	return &DigestMap{
		m: xsync.NewMap[string, *digestAndLock](),
	}
}

// digestAndLock holds a t-digest with its associated lock for thread-safe access.
type digestAndLock struct {
	digest *tdigest.TDigest // GUARDED_BY(lock)
	lock   sync.RWMutex
}

// Add adds a value to the digest for the given key.
// If the key doesn't exist, a new digest is created.
func (dm *DigestMap) Add(key string, value float64) {
	dal, _ := dm.m.LoadOrCompute(key, func() (newValue *digestAndLock, cancel bool) {
		newDigest, err := tdigest.New(tdigest.Compression(defaultTDigestCompression))
		if err != nil {
			log.Err(err).Msg("failed to create new TDigest")
			return nil, true
		}

		return &digestAndLock{
			digest: newDigest,
			lock:   sync.RWMutex{},
		}, false
	})
	if dal == nil {
		log.Error().Msg("failed to load or create digest for key")
		return
	}

	dal.lock.Lock()
	defer dal.lock.Unlock()

	if err := dal.digest.Add(value); err != nil {
		log.Err(err).Msg("failed to add value to TDigest")
		return
	}
}

// CDF returns the cumulative distribution function value at the given value for the key.
// Returns the CDF value and true if the key exists, 0 and false otherwise.
func (dm *DigestMap) CDF(key string, value float64) (float64, bool) {
	dal, ok := dm.m.Load(key)
	if !ok {
		return 0, false
	}

	dal.lock.RLock()
	defer dal.lock.RUnlock()
	return dal.digest.CDF(value), true
}
