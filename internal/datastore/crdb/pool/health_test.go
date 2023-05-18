package pool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeHealthTracker(t *testing.T) {
	tracker := &NodeHealthTracker{
		healthyNodes: make(map[uint32]struct{}),
	}

	tracker.SetNodeHealth(1, true)
	require.True(t, tracker.IsHealthy(1))
	require.False(t, tracker.IsHealthy(2))
	require.Equal(t, tracker.HealthyNodeCount(), 1)

	tracker.SetNodeHealth(2, true)
	require.True(t, tracker.IsHealthy(1))
	require.True(t, tracker.IsHealthy(2))
	require.Equal(t, tracker.HealthyNodeCount(), 2)

	tracker.SetNodeHealth(1, false)
	require.False(t, tracker.IsHealthy(1))
	require.True(t, tracker.IsHealthy(2))
	require.Equal(t, tracker.HealthyNodeCount(), 1)
}
