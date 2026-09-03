package schemacaching

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/cache"
	"github.com/authzed/spicedb/pkg/datastore"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// countingReader is a datastore.Reader that serves a fixed schema and counts
// how many times a definition was actually loaded from "the datastore" without
// hitting the cache.
type countingReader struct {
	proxy_test.MockReader

	defs  map[string]*core.NamespaceDefinition
	reads atomic.Int64
}

func (cr *countingReader) LegacyReadNamespaceByName(_ context.Context, name string) (*core.NamespaceDefinition, datastore.Revision, error) {
	cr.reads.Add(1)
	def, ok := cr.defs[name]
	if !ok {
		return nil, datastore.NoRevision, datastore.NewNamespaceNotFoundErr(name)
	}
	return def, revisions.NewForTransactionID(1), nil
}

// countingDatastore hands out the same countingReader for every revision. Only
// SnapshotReader and Close are exercised, so the embedded interface is left nil.
type countingDatastore struct {
	datastore.Datastore

	reader *countingReader
}

func (cd *countingDatastore) SnapshotReader(datastore.Revision) datastore.Reader {
	return cd.reader
}

func (cd *countingDatastore) Close() error { return nil }

// schemaForHitRateTest builds a schema of the given size, with definitions
// shaped roughly like real ones so the size estimator produces realistic costs.
func schemaForHitRateTest(numDefs int) map[string]*core.NamespaceDefinition {
	defs := make(map[string]*core.NamespaceDefinition, numDefs)
	for i := range numDefs {
		name := fmt.Sprintf("resource_definition_number_%d", i)
		defs[name] = ns.Namespace(
			name,
			ns.MustRelation("owner", nil, ns.AllowedRelation("user", "...")),
			ns.MustRelation("editor", nil, ns.AllowedRelation("user", "..."), ns.AllowedRelation("group", "member")),
			ns.MustRelation("viewer", nil, ns.AllowedRelation("user", "..."), ns.AllowedRelation("group", "member")),
			ns.MustRelation("view", ns.Union(
				ns.ComputedUserset("viewer"),
				ns.ComputedUserset("editor"),
				ns.ComputedUserset("owner"),
			)),
		)
	}
	return defs
}

// TestNamespaceCacheHitsUnderStandardUsage exercises the namespace cache the
// way a running SpiceDB exercises it: a stable schema is read over and over,
// while the revision at which it is read rotates as the quantization window
// advances.
//
// Because cache keys are (definition name, revision) pairs, every rotation
// produces a whole new set of keys and strands the previous set: no request
// will ever ask for the old revision again. The working set, though, is only
// ever one revision's worth of definitions, which fits with room to spare.
//
// Under those conditions the cache must keep serving the current revision's
// definitions from memory, so each (definition, revision) pair should reach the
// datastore exactly once no matter how many revisions have rolled by. This test
// runs long enough for the cache to fill several times over, because that is
// when the failure appears: a cache that lets stranded entries accumulate ends
// up permanently at capacity, and an eviction policy that prefers incumbents
// then starts turning away the definitions that are actually in use. Hit rate
// falls and the extra reads land on the datastore.
func TestNamespaceCacheHitsUnderStandardUsage(t *testing.T) {
	const (
		numDefinitions = 20

		// Simulated quantization window: how long each revision stays current,
		// and how many of them roll by over the course of the test.
		quantizationWindow = 5 * time.Second
		numRevisions       = 400

		// How many times each definition is read while its revision is current;
		// stands in for the requests served during one quantization window.
		readsPerRevision = 25

		// Cache budget, expressed in revisions' worth of schema. The live
		// working set is a single revision, so this is ample headroom; the test
		// is about entries that are stranded, not entries that don't fit.
		revisionsOfCapacity = 20
	)

	defs := schemaForHitRateTest(numDefinitions)
	names := make([]string, 0, numDefinitions)
	for name := range defs {
		names = append(names, name)
	}

	// Derive the cache budget from the cost the caching proxy actually charges
	// for these definitions, so the test isn't sensitive to the size estimator.
	var costPerRevision int64
	for _, def := range defs {
		costPerRevision += estimatedNamespaceDefinitionSize(def.SizeVT()) + 128 // ~key overhead
	}

	// Mirrors how the server builds this cache: see the namespace cache in
	// (*Config).complete, whose TTL comes from CacheConfig.WithRevisionParameters.
	ttl := 2 * quantizationWindow

	synctest.Test(t, func(t *testing.T) {
		c, err := cache.NewStandardCacheWithMetrics[cache.StringKey, CacheEntry](
			prometheus.NewRegistry(),
			"namespace",
			&cache.Config{
				MaxCost:    costPerRevision * revisionsOfCapacity,
				DefaultTTL: ttl,
			},
		)
		require.NoError(t, err)
		t.Cleanup(c.Close)

		reader := &countingReader{defs: defs}
		ds := MustNewDefinitionCachingProxy(&countingDatastore{reader: reader}, c)

		ctx := t.Context()

		// Reads are counted per quarter of the run so that a hit rate which
		// decays as the cache fills is distinguishable from one that is simply
		// bad from the start.
		var readsPerQuarter [4]int64
		previousReads := int64(0)

		for revNum := 1; revNum <= numRevisions; revNum++ {
			snapshot := ds.SnapshotReader(revisions.NewForTransactionID(uint64(revNum)))
			for range readsPerRevision {
				for _, name := range names {
					def, _, err := snapshot.LegacyReadNamespaceByName(ctx, name)
					require.NoError(t, err)
					require.Equal(t, name, def.Name)
				}
			}

			totalReads := reader.reads.Load()
			readsPerQuarter[(revNum-1)*4/numRevisions] += totalReads - previousReads
			previousReads = totalReads

			// Advance into the next quantization window.
			time.Sleep(quantizationWindow)
		}

		// Every (definition, revision) pair is a distinct key, so the ideal
		// number of datastore reads is one per pair.
		idealPerQuarter := int64(numDefinitions * numRevisions / 4)
		idealTotal := idealPerQuarter * 4
		actualTotal := reader.reads.Load()

		metrics := c.GetMetrics()
		t.Logf("datastore reads: %d (ideal %d, %.2fx); reads per quarter of run: %.2fx %.2fx %.2fx %.2fx",
			actualTotal, idealTotal, float64(actualTotal)/float64(idealTotal),
			float64(readsPerQuarter[0])/float64(idealPerQuarter),
			float64(readsPerQuarter[1])/float64(idealPerQuarter),
			float64(readsPerQuarter[2])/float64(idealPerQuarter),
			float64(readsPerQuarter[3])/float64(idealPerQuarter))
		t.Logf("cache hits: %d, misses: %d, hit rate: %.2f%%; cost added: %d, cost evicted: %d",
			metrics.Hits(), metrics.Misses(),
			100*float64(metrics.Hits())/float64(metrics.Hits()+metrics.Misses()),
			metrics.CostAdded(), metrics.CostEvicted())

		// The strict assertion: no definition is ever fetched twice for the same
		// revision. Anything above this is a definition that was cached, dropped
		// while still in use, and re-fetched.
		require.Equal(t, idealTotal, actualTotal,
			"the cache should serve every repeat read of a definition at a given revision")

		// And the shape of the failure the users reported: the last quarter of
		// the run, long after the cache has filled, must be no worse than the
		// first.
		require.LessOrEqual(t, readsPerQuarter[3], readsPerQuarter[0],
			"cache effectiveness degraded over the course of the run: reads per quarter were %v", readsPerQuarter)

		// The reported hit rate should match the reads actually issued; a Get
		// that misses is the only thing that should count as a miss.
		require.Equal(t, safecast.RequireConvert[uint64](t, actualTotal), metrics.Misses(),
			"reported cache misses should equal the number of datastore reads")
	})
}
