package combined

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/cache"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestCombinedRecursiveCall(t *testing.T) {
	dispatcher, err := NewDispatcher()
	require.NoError(t, err)

	t.Cleanup(func() { dispatcher.Close() })

	ctx := datalayer.ContextWithHandle(t.Context())

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(t, rawDS, `
		definition user {}

		definition resource {
			relation viewer: resource#viewer | user
			permission view = viewer
		}
	`, []tuple.Relationship{
		tuple.MustParse("resource:someresource#viewer@resource:someresource#viewer"),
	})

	require.NoError(t, datalayer.SetInContext(ctx, datalayer.NewDataLayer(ds)))

	_, err = dispatcher.DispatchCheck(ctx, &dispatchv1.DispatchCheckRequest{
		ResourceRelation: &core.RelationReference{
			Namespace: "resource",
			Relation:  "view",
		},
		ResourceIds: []string{"someresource"},
		Subject: &core.ObjectAndRelation{
			Namespace: "user",
			ObjectId:  "fred",
			Relation:  tuple.Ellipsis,
		},
		ResultsSetting: dispatchv1.DispatchCheckRequest_REQUIRE_ALL_RESULTS,
		Metadata: &dispatchv1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
			SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
		},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "max depth exceeded")
}

// TestNewDispatcher_AppliesAllOptions_NoUpstream verifies that every option
// setter writes to the expected optionState field and that NewDispatcher
// successfully wires those options through on the no-upstream branch.
func TestNewDispatcher_AppliesAllOptions_NoUpstream(t *testing.T) {
	cacheConfig := &cache.Config{
		NumCounters: 100,
		MaxCost:     1024,
		DefaultTTL:  1 * time.Second,
	}

	dispatchCache, err := cache.NewStandardCache[keys.DispatchCacheKey, any](&cache.Config{
		NumCounters: 100,
		MaxCost:     1024,
		DefaultTTL:  1 * time.Second,
	})
	require.NoError(t, err)

	concurrencyLimits := graph.ConcurrencyLimits{Check: 10, LookupResources: 5}

	reg := prometheus.NewRegistry()
	options := []Option{
		Metrics(dispatch.MetricsOptions{PrometheusSubsystem: "test_subsystem", PrometheusRegistry: reg}),
		DispatchChunkSize(50),
		RelationshipChunkCacheConfig(cacheConfig),
		CaveatTypeSet(caveattypes.Default.TypeSet),
		Cache(dispatchCache),
		ConcurrencyLimits(concurrencyLimits),
	}

	// Field-level assertions: each setter wrote to the right slot.
	opts := newOptions(options...)
	require.Equal(t, reg, opts.metrics.PrometheusRegistry)
	require.Equal(t, "test_subsystem", opts.metrics.PrometheusSubsystem)
	require.Equal(t, uint16(50), opts.dispatchChunkSize)
	require.Same(t, cacheConfig, opts.relationshipChunkCacheConfig)
	require.Same(t, caveattypes.Default.TypeSet, opts.caveatTypeSet)
	require.Same(t, dispatchCache, opts.cache)
	require.Equal(t, concurrencyLimits, opts.concurrencyLimits)

	dispatcher, err := NewDispatcher(options...)
	require.NoError(t, err)
	require.NotNil(t, dispatcher)
	t.Cleanup(func() { _ = dispatcher.Close() })
}

// TestNewDispatcher_WithProvidedRelationshipChunkCache hits the branch where
// the caller supplies a pre-built relationship chunk cache instead of a config.
// It asserts the option is bound to the right field and that no config was
// inferred in its place.
func TestNewDispatcher_WithProvidedRelationshipChunkCache(t *testing.T) {
	c, err := cache.NewStandardCache[cache.StringKey, any](&cache.Config{
		NumCounters: 100,
		MaxCost:     1024,
		DefaultTTL:  1 * time.Second,
	})
	require.NoError(t, err)

	options := []Option{
		RelationshipChunkCache(c),
	}

	opts := newOptions(options...)
	require.Same(t, c, opts.relationshipChunkCache)
	require.Nil(t, opts.relationshipChunkCacheConfig)

	dispatcher, err := NewDispatcher(options...)
	require.NoError(t, err)
	require.NotNil(t, dispatcher)
	t.Cleanup(func() { _ = dispatcher.Close() })
}

// TestNewDispatcher_WithUpstream exercises the upstream branch using an
// insecure preshared-key setup. grpc.DialContext is lazy, so a bogus but
// syntactically valid address is acceptable. It also asserts each option
// bound to the right optionState field.
func TestNewDispatcher_WithUpstream(t *testing.T) {
	options := []Option{
		UpstreamAddr("localhost:0"),
		GrpcPresharedKey("test-key"),
		RemoteDispatchTimeout(5 * time.Second),
		StartingPrimaryHedgingDelay(10 * time.Millisecond),
		GrpcDialOpts(),
	}

	opts := newOptions(options...)
	require.Equal(t, "localhost:0", opts.upstreamAddr)
	require.Equal(t, "test-key", opts.grpcPresharedKey)
	require.Equal(t, 5*time.Second, opts.remoteDispatchTimeout)
	require.Equal(t, 10*time.Millisecond, opts.startingPrimaryHedgingDelay)
	require.Empty(t, opts.grpcDialOpts)

	dispatcher, err := NewDispatcher(options...)
	require.NoError(t, err)
	require.NotNil(t, dispatcher)
	t.Cleanup(func() { _ = dispatcher.Close() })
}

// TestNewDispatcher_UpstreamCAPathMissing exercises the TLS branch by pointing
// UpstreamCAPath at a non-existent file; grpcutil.WithCustomCerts will error.
func TestNewDispatcher_UpstreamCAPathMissing(t *testing.T) {
	_, err := NewDispatcher(
		UpstreamAddr("localhost:0"),
		UpstreamCAPath("/nonexistent/path/to/ca.pem"),
		GrpcPresharedKey("test-key"),
	)
	require.Error(t, err)
}

// TestNewDispatcher_SecondaryInvalidHedgingDelay covers the parse-error branch
// for secondary upstream maximum primary hedging delays.
func TestNewDispatcher_SecondaryInvalidHedgingDelay(t *testing.T) {
	_, err := NewDispatcher(
		UpstreamAddr("localhost:0"),
		GrpcPresharedKey("test-key"),
		SecondaryUpstreamAddrs(map[string]string{"sec": "localhost:1"}),
		SecondaryMaximumPrimaryHedgingDelays(map[string]string{"sec": "not-a-duration"}),
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "error parsing maximum primary hedging delay")
}

// TestNewDispatcher_SecondaryNegativeHedgingDelay covers the zero/negative
// hedging delay branch.
func TestNewDispatcher_SecondaryNegativeHedgingDelay(t *testing.T) {
	_, err := NewDispatcher(
		UpstreamAddr("localhost:0"),
		GrpcPresharedKey("test-key"),
		SecondaryUpstreamAddrs(map[string]string{"sec": "localhost:1"}),
		SecondaryMaximumPrimaryHedgingDelays(map[string]string{"sec": "0s"}),
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "must be greater than 0")
}

// TestNewDispatcher_InvalidSecondaryDispatchExpr covers the parse-error branch
// for secondary dispatch expressions.
func TestNewDispatcher_InvalidSecondaryDispatchExpr(t *testing.T) {
	_, err := NewDispatcher(
		UpstreamAddr("localhost:0"),
		GrpcPresharedKey("test-key"),
		SecondaryUpstreamExprs(map[string]string{"check": "not a valid CEL expression @#$"}),
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "error parsing secondary dispatch expr")
}
