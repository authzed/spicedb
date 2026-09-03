package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/pkg/cmd/util"
)

func TestParsePercent(t *testing.T) {
	table := []struct {
		percent     string
		freeMem     uint64
		expected    uint64
		expectedErr error
	}{
		{"100%", 1000, 1000, nil},
		{"0%", 1000, 0, nil},
		{"50%", 1000, 500, nil},
		{"100%", 0, 0, nil},
		{"1000%", 1000, 0, errOverHundredPercent},
	}

	for _, tt := range table {
		v, err := parsePercent(tt.percent, tt.freeMem)
		if tt.expectedErr == nil {
			require.NoError(t, err)
		} else {
			require.Equal(t, tt.expectedErr, err)
		}
		require.Equal(t, tt.expected, v)
	}
}

func TestResolveMaxCost(t *testing.T) {
	t.Run("absolute byte value ignores available memory", func(t *testing.T) {
		got, err := resolveMaxCost(&CacheConfig{Name: "test", MaxCost: "1GiB"}, 0)
		require.NoError(t, err)
		require.Equal(t, int64(1024*1024*1024), got)
	})

	t.Run("percent resolves against available memory", func(t *testing.T) {
		got, err := resolveMaxCost(&CacheConfig{Name: "test", MaxCost: "25%"}, 1000)
		require.NoError(t, err)
		require.Equal(t, int64(250), got)
	})

	t.Run("invalid value errors", func(t *testing.T) {
		_, err := resolveMaxCost(&CacheConfig{Name: "test", MaxCost: "not-a-size"}, 1000)
		require.Error(t, err)
	})
}

func TestWithRevisionParameters(t *testing.T) {
	table := []struct {
		name                 string
		quantizationInterval time.Duration
		followerReadDelay    time.Duration
		maxStalenessPercent  float64
		expectedTTL          time.Duration
	}{
		{
			// A zero TTL means "never expire", which is not a safe value
			// revision-keyed cache where cache keys don't repeat as revisions
			// roll forward, so the fallback is used instead.
			name:                 "zero values",
			quantizationInterval: 0,
			followerReadDelay:    0,
			maxStalenessPercent:  0,
			expectedTTL:          fallbackRevisionTTL,
		},
		{
			name:                 "basic configuration",
			quantizationInterval: 5 * time.Second,
			followerReadDelay:    0,
			maxStalenessPercent:  0,
			expectedTTL:          10 * time.Second, // (5s * (1+0) + 0) * 2.0
		},
		{
			name:                 "with follower read delay",
			quantizationInterval: 5 * time.Second,
			followerReadDelay:    3 * time.Second,
			maxStalenessPercent:  0,
			expectedTTL:          16 * time.Second, // (5s * (1+0) + 3s) * 2.0
		},
		{
			name:                 "with staleness percent",
			quantizationInterval: 10 * time.Second,
			followerReadDelay:    0,
			maxStalenessPercent:  0.1,              // 10%
			expectedTTL:          22 * time.Second, // (10s * (1+0.1) + 0) * 2.0
		},
		{
			name:                 "all parameters set",
			quantizationInterval: 5 * time.Second,
			followerReadDelay:    2 * time.Second,
			maxStalenessPercent:  0.2,              // 20%
			expectedTTL:          16 * time.Second, // ((5s * 1.2) + 2s) * 2.0
		},
		{
			name:                 "high staleness percent",
			quantizationInterval: 1 * time.Minute,
			followerReadDelay:    10 * time.Second,
			maxStalenessPercent:  0.5,               // 50%
			expectedTTL:          200 * time.Second, // ((60s * 1.5) + 10s) * 2.0
		},
		{
			name:                 "millisecond precision",
			quantizationInterval: 500 * time.Millisecond,
			followerReadDelay:    100 * time.Millisecond,
			maxStalenessPercent:  0.15,
			expectedTTL:          1350 * time.Millisecond, // ((500ms * 1.15) + 100ms) * 2.0
		},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			cc := &CacheConfig{
				Name:    "test-cache",
				MaxCost: "1MB",
				Metrics: true,
				Enabled: true,
			}

			result := cc.WithRevisionParameters(
				tt.quantizationInterval,
				tt.followerReadDelay,
				tt.maxStalenessPercent,
			)

			// Verify it returns the same instance
			require.Same(t, cc, result)

			require.Equal(t, tt.expectedTTL, result.defaultTTL)
		})
	}
}

// TestRevisionKeyedCachesExpire asserts that every cache whose keys embed a
// datastore revision is completed with a TTL. Such a cache strands every entry
// it holds each time the quantization window advances, so without expiry it
// climbs to capacity and stays there, packed with keys no request can ask for
// again — at which point the eviction policy begins rejecting the entries that
// are still being read, and the hit rate falls away.
func TestRevisionKeyedCachesExpire(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 1*time.Second, 10*time.Second)
	require.NoError(t, err)

	c := ConfigWithOptions(
		&Config{},
		WithPresharedSecureKey("psk"),
		WithDatastore(ds),
		WithGRPCServer(util.GRPCServerConfig{Network: util.BufferedNetwork, Enabled: true}),
		// The cluster dispatch cache is only built when the dispatch server is on.
		WithDispatchServer(util.GRPCServerConfig{Network: util.BufferedNetwork, Enabled: true}),
		WithNamespaceCacheConfig(CacheConfig{Name: "namespace", Enabled: true, MaxCost: "32MiB"}),
		WithDispatchCacheConfig(CacheConfig{Name: "dispatch", Enabled: true, MaxCost: "1MiB"}),
		WithClusterDispatchCacheConfig(CacheConfig{Name: "cluster_dispatch", Enabled: true, MaxCost: "1MiB"}),
		WithEnableMemoryProtectionMiddleware(false),
	)

	completed, err := c.complete(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = completed.closeFunc() })

	// Note that this Config was assembled in code, so DatastoreConfig carries
	// zero revision parameters; the TTL comes from fallbackRevisionTTL.
	for name, cacheConfig := range map[string]CacheConfig{
		"namespace":        c.NamespaceCacheConfig,
		"dispatch":         c.DispatchCacheConfig,
		"cluster_dispatch": c.ClusterDispatchCacheConfig,
	} {
		require.Positive(t, cacheConfig.defaultTTL,
			"the %s cache is keyed by revision and must be given a TTL", name)
	}
}
