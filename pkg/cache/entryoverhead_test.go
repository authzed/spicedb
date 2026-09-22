//go:build !wasm

package cache

import (
	"encoding/binary"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/maypok86/otter/v2"
	"github.com/stretchr/testify/require"
)

// Number of entries inserted per measurement. Large enough that the per-entry
// figure is insensitive to a few KiB of allocator noise.
const overheadSampleEntries = 100_000

// Key and payload byte lengths used for the measurement. Both are exact Go size
// classes, so the allocator adds no rounding of its own and the measured delta is
// purely the structural cost entryStructuralOverhead is meant to cover.
const (
	overheadKeyLen     = 16
	overheadPayloadLen = 16
)

// Bounds the measurement must fall inside, as a fraction of
// entryStructuralOverhead. The constant is meant to be an upper bound on the real
// overhead, so measuring above it is the failure that matters; the small upward
// slack absorbs platform and allocator differences without making the test flaky.
// The lower bound catches the constant drifting into being wildly conservative.
const (
	overheadLowerBound = 0.6
	overheadUpperBound = 1.1
)

// measureStructuralOverhead inserts entries into a real Otter cache shaped like
// the dispatch caches (V=any holding a []byte, with a TTL — the most expensive of
// the shapes SpiceDB creates) and returns the heap bytes retained per entry
// beyond the key and payload bytes themselves.
func measureStructuralOverhead(t *testing.T) float64 {
	t.Helper()

	cache, err := otter.New(&otter.Options[string, valueAndCost[any]]{
		// Far above what the sample inserts, so nothing is evicted mid-measurement.
		MaximumWeight: uint64(overheadSampleEntries) * 1024,
		Weigher: func(_ string, value valueAndCost[any]) uint32 {
			return value.cost
		},
		ExpiryCalculator: otter.ExpiryAccessing[string, valueAndCost[any]](24 * time.Hour),
	})
	require.NoError(t, err)
	defer cache.StopAllGoroutines()

	before := stableHeapAlloc()
	for i := range uint64(overheadSampleEntries) {
		// Built inside the loop so every byte the entry retains — the key string's
		// backing array included — is allocated inside the measured window.
		key := make([]byte, overheadKeyLen)
		binary.LittleEndian.PutUint64(key, i)
		binary.LittleEndian.PutUint64(key[8:], ^i)
		payload := make([]byte, overheadPayloadLen)
		cache.Set(string(key), valueAndCost[any]{payload, overheadKeyLen + overheadPayloadLen})
	}
	// Drain Otter's pending maintenance so the write buffer is not still holding
	// entries that have not reached the hash table and eviction policy yet.
	cache.CleanUp()
	require.Equal(t, overheadSampleEntries, cache.EstimatedSize())

	after := stableHeapAlloc()
	runtime.KeepAlive(cache)

	const entryBytes = overheadSampleEntries * (overheadKeyLen + overheadPayloadLen)
	return (float64(after-before) - entryBytes) / overheadSampleEntries
}

// stableHeapAlloc returns the live heap size after repeated collections, so the
// figure reflects what is actually retained rather than what has not been swept
// yet.
func stableHeapAlloc() uint64 {
	for range 4 {
		runtime.GC()
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// TestEntryStructuralOverhead pins entryStructuralOverhead to a measured heap
// delta, so that an Otter upgrade which changes the per-entry cost cannot
// silently reintroduce the undercounting the constant exists to prevent. It
// follows the same shape as TestEstimatedDefinitionSizes in
// internal/datastore/proxy/schemacaching.
//
// Determinism: the measurement is over 100k entries with both key and payload on
// exact size classes, taken between forced collections and after draining Otter's
// maintenance, and the smallest of several runs is used — garbage the collector
// has not reached can only inflate a reading, never deflate it. The assertion is
// a band, not an exact byte count.
func TestEntryStructuralOverhead(t *testing.T) {
	// The measurement reads whole-heap statistics, so it cannot share a heap with
	// other tests running concurrently.
	defer debug.SetGCPercent(debug.SetGCPercent(-1))

	measured := measureStructuralOverhead(t)
	for range 2 {
		if next := measureStructuralOverhead(t); next < measured {
			measured = next
		}
	}

	t.Logf("measured structural overhead: %.1f bytes/entry (constant: %d)", measured, entryStructuralOverhead)
	require.GreaterOrEqual(t, measured, overheadLowerBound*entryStructuralOverhead,
		"entryStructuralOverhead (%d) is far above the measured per-entry overhead (%.1f bytes); "+
			"the cache is giving up capacity for overhead that no longer exists",
		entryStructuralOverhead, measured)
	require.LessOrEqual(t, measured, overheadUpperBound*entryStructuralOverhead,
		"entryStructuralOverhead (%d) undercounts the measured per-entry overhead (%.1f bytes); "+
			"raise the constant, or the caches will hold more memory than their configured budget",
		entryStructuralOverhead, measured)
}
