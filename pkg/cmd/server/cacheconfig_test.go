package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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

func TestWithRevisionParameters(t *testing.T) {
	table := []struct {
		name                 string
		quantizationInterval time.Duration
		followerReadDelay    time.Duration
		maxStalenessPercent  float64
		expectedTTL          time.Duration
	}{
		{
			name:                 "zero values",
			quantizationInterval: 0,
			followerReadDelay:    0,
			maxStalenessPercent:  0,
			expectedTTL:          0,
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
				Name:        "test-cache",
				MaxCost:     "1MB",
				NumCounters: 1000,
				Metrics:     true,
				Enabled:     true,
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
