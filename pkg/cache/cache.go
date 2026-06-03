package cache

import (
	"time"

	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog"

	"github.com/authzed/spicedb/pkg/spiceerrors"
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
type Config struct {
	// Deprecated: NumCounters was used to control behavior of the cache
	// when the underlying cache exposed it, but the current cache implementation
	// does not use it.
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
	maxCost := spiceerrors.MustSafecast[uint64](c.MaxCost)
	e.
		Str("maxCost", humanize.IBytes(maxCost)).
		Dur("defaultTTL", c.DefaultTTL)
}

// Cache defines an interface for a generic cache. Method semantics follow
// Ristretto, the original implementation; the current implementation is
// backed by Otter (see NewOtterCache).
type Cache[K KeyString, V any] interface {
	// Get returns the value for the given key and true if it is present;
	// otherwise it returns the zero value of V and false.
	Get(key K) (V, bool)

	// GetTTL returns the TTL configured for entries in this cache.
	// If zero, entries never expire. The current Otter-backed
	// implementation refreshes the TTL on access.
	GetTTL() time.Duration

	// Set attempts to store an entry for key, overwriting any existing entry,
	// and returns true if the write was accepted for processing. cost is
	// passed to the eviction policy and weighed against Config.MaxCost;
	// the Otter implementation saturates cost at math.MaxUint32.
	// A false return means the implementation did not enqueue the write
	// (as Ristretto may do under pressure); the Otter-backed implementation
	// always returns true. A true return does not guarantee retention —
	// the entry may still be dropped or evicted by the underlying
	// implementation, so writes are best-effort.
	Set(key K, entry V, cost int64) bool

	// Wait blocks until buffered Set calls have been processed by the
	// underlying implementation. Required for read-your-own-writes
	// semantics with implementations that buffer writes (e.g. Ristretto);
	// a no-op on the current Otter-backed implementation, which applies
	// writes synchronously.
	Wait()

	// Close stops the cache's background workers (if any) and tears down
	// associated metrics registration, if one was set up.
	Close()

	// GetMetrics returns the metrics block for the cache.
	// Some implementations may choose to not return some of these metrics.
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
func (no *noopCache[K, V]) GetTTL() time.Duration      { return time.Duration(0) }
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
