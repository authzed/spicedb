package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjectAffinityRoutingConfigDefaults(t *testing.T) {
	cfg := NewConfigWithOptionsAndDefaults()
	require.False(t, cfg.EnableExperimentalObjectAffinityRouting)
	require.Zero(t, cfg.DispatchObjectSpreadShare)
	require.Equal(t, uint8(2), cfg.DispatchObjectSpread)
	require.InDelta(t, 1.5, cfg.DispatchObjectSpreadLatencyFactor, 0)
}

func TestObjectSpreadConfigOptions(t *testing.T) {
	cfg := NewConfigWithOptionsAndDefaults(
		WithDispatchObjectSpreadShare(0.5),
		WithDispatchObjectSpreadLatencyFactor(2),
	)
	require.InDelta(t, 0.5, cfg.DispatchObjectSpreadShare, 0)
	require.InDelta(t, 2, cfg.DispatchObjectSpreadLatencyFactor, 0)
}
