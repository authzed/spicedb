package pool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeHealthTracker(t *testing.T) {
	tracker, err := NewNodeHealthChecker("postgres://user:password@localhost:5432/dbname")
	require.NoError(t, err)
	t.Cleanup(func() {
		tracker.Close()
	})

	tracker.SetNodeHealth(1, true)
	require.True(t, tracker.IsHealthy(1))
	require.False(t, tracker.IsHealthy(2))
	require.Equal(t, 1, tracker.HealthyNodeCount())

	tracker.SetNodeHealth(2, true)
	require.True(t, tracker.IsHealthy(1))
	require.True(t, tracker.IsHealthy(2))
	require.Equal(t, 2, tracker.HealthyNodeCount())

	// just 1 mark isn't enough to trigger false
	tracker.SetNodeHealth(1, false)
	require.True(t, tracker.IsHealthy(1))
	require.True(t, tracker.IsHealthy(2))
	require.Equal(t, 2, tracker.HealthyNodeCount())

	tracker.SetNodeHealth(1, false)
	tracker.SetNodeHealth(1, false)
	require.False(t, tracker.IsHealthy(1))
	require.True(t, tracker.IsHealthy(2))
	require.Equal(t, 1, tracker.HealthyNodeCount())
}
