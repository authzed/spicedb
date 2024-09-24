package cache

import (
	"time"

	"github.com/ccoveille/go-safecast"
	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog"
)

// KeyString is an interface for keys that can be converted to strings.
type KeyString interface {
	comparable
	KeyString() string
}

// StringKey is a simple string key.
type StringKey string

func (sk StringKey) KeyString() string {
	return string(sk)
}

// Config for caching.
// See: https://github.com/outcaste-io/ristretto#Config
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

	// DefaultTTL configures a default deadline on the lifetime of any keys set
	// to the cache.
	DefaultTTL time.Duration
}

func (c *Config) MarshalZerologObject(e *zerolog.Event) {
	maxCost, _ := safecast.ToUint64(c.MaxCost)
	e.
		Str("maxCost", humanize.IBytes(maxCost)).
		Int64("numCounters", c.NumCounters).
		Dur("defaultTTL", c.DefaultTTL)
}

// Cache defines an interface for a generic cache.
type Cache[K KeyString, V any] interface {
	// Get returns the value for the given key in the cache, if it exists.
	Get(key K) (V, bool)

	// Set sets a value for the key in the cache, with the given cost.
	Set(key K, entry V, cost int64) bool

	// Wait waits for the cache to process and apply updates.
	Wait()

	// Close closes the cache's background workers (if any).
	Close()

	// GetMetrics returns the metrics block for the cache.
	GetMetrics() Metrics

	zerolog.LogObjectMarshaler
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

// NoopCache returns a cache that does nothing.
func NoopCache[K KeyString, V any]() Cache[K, V] { return &noopCache[K, V]{} }

type noopCache[K KeyString, V any] struct{}

var _ Cache[StringKey, any] = (*noopCache[StringKey, any])(nil)

func (no *noopCache[K, V]) Get(_ K) (V, bool)          { return *new(V), false }
func (no *noopCache[K, V]) Set(_ K, _ V, _ int64) bool { return false }
func (no *noopCache[K, V]) Wait()                      {}
func (no *noopCache[K, V]) Close()                     {}
func (no *noopCache[K, V]) GetMetrics() Metrics        { return &noopMetrics{} }
func (no *noopCache[K, V]) MarshalZerologObject(e *zerolog.Event) {
	e.Bool("enabled", false)
}

type noopMetrics struct{}

var _ Metrics = (*noopMetrics)(nil)

func (no *noopMetrics) Hits() uint64        { return 0 }
func (no *noopMetrics) Misses() uint64      { return 0 }
func (no *noopMetrics) CostAdded() uint64   { return 0 }
func (no *noopMetrics) CostEvicted() uint64 { return 0 }
