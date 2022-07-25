package cache

// Config for caching.
// See: https://github.com/dgraph-io/ristretto#Config
type Config struct {
	// NumCounters determines the number of counters (keys) to keep that hold
	// access frequency information. It's generally a good idea to have more
	// counters than the max cache capacity, as this will improve eviction
	// accuracy and subsequent hit ratios.
	//
	// For example, if you expect your cache to hold 1,000,000 items when full,
	// NumCounters should be 10,000,000 (10x). Each counter takes up roughly
	// 3 bytes (4 bits for each counter * 4 copies plus about a byte per
	// counter for the bloom filter). Note that the number of counters is
	// internally rounded up to the nearest power of 2, so the space usage
	// may be a little larger than 3 bytes * NumCounters.
	NumCounters int64

	// MaxCost can be considered as the cache capacity, in whatever units you
	// choose to use.
	//
	// For example, if you want the cache to have a max capacity of 100MB, you
	// would set MaxCost to 100,000,000 and pass an item's number of bytes as
	// the `cost` parameter for calls to Set. If new items are accepted, the
	// eviction process will take care of making room for the new item and not
	// overflowing the MaxCost value.
	MaxCost int64

	// BufferItems determines the size of Get buffers.
	//
	// Unless you have a rare use case, using `64` as the BufferItems value
	// results in good performance.
	BufferItems int64

	// Metrics determines whether cache statistics are kept during the cache's
	// lifetime. There *is* some overhead to keeping statistics, so you should
	// only set this flag to true when testing or throughput performance isn't a
	// major factor.
	Metrics bool

	// Disabled, if specified, completely disables the cache.
	Disabled bool
}

// Cache defines an interface for a generic cache.
type Cache interface {
	// Get returns the value for the given key in the cache, if it exists.
	Get(key interface{}) (interface{}, bool)

	// Set sets a value for the key in the cache, with the given cost.
	Set(key interface{}, entry interface{}, cost int64) bool

	// Wait waits for the cache to process and apply updates.
	Wait()

	// Close closes the cache's background workers (if any).
	Close()

	// GetMetrics returns the metrics block for the cache.
	GetMetrics() Metrics
}

// Metrics defines metrics exported by the cache.
type Metrics interface {
	// Hits is the number of cache hits.
	Hits() uint64

	// Misses is the number of cache misses.
	Misses() uint64

	// CostAdded returns the total cost of added items.
	CostAdded() uint64

	// CostEvicted returns the total cost of evicted items.
	CostEvicted() uint64
}

// NoopCache returns an implementation of the cache interface that does nothing.
func NoopCache() Cache {
	return &noopCache{}
}

type noopCache struct{}

func (no *noopCache) Get(key interface{}) (interface{}, bool) {
	return nil, false
}

func (no *noopCache) Set(key interface{}, entry interface{}, cost int64) bool {
	return false
}

func (no *noopCache) Wait() {
}

func (no *noopCache) Close() {
}

func (no *noopCache) GetMetrics() Metrics {
	return nil
}
