package pool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestNodeHealthTracker(t *testing.T) {
	tracker := &NodeHealthTracker{
		healthyNodes:  make(map[uint32]struct{}),
		nodesEverSeen: make(map[uint32]*rate.Limiter),
		newLimiter: func() *rate.Limiter {
			return rate.NewLimiter(rate.Every(1*time.Minute), 2)
		},
	}

	tracker.SetNodeHealth(1, true)
	require.True(t, tracker.IsHealthy(1))
	require.False(t, tracker.IsHealthy(2))
	require.Equal(t, tracker.HealthyNodeCount(), 1)

	tracker.SetNodeHealth(2, true)
	require.True(t, tracker.IsHealthy(1))
	require.True(t, tracker.IsHealthy(2))
	require.Equal(t, tracker.HealthyNodeCount(), 2)

	// just 1 mark isn't enough to trigger false
	tracker.SetNodeHealth(1, false)
	require.True(t, tracker.IsHealthy(1))
	require.True(t, tracker.IsHealthy(2))
	require.Equal(t, tracker.HealthyNodeCount(), 2)

	tracker.SetNodeHealth(1, false)
	tracker.SetNodeHealth(1, false)
	require.False(t, tracker.IsHealthy(1))
	require.True(t, tracker.IsHealthy(2))
	require.Equal(t, tracker.HealthyNodeCount(), 1)
}
