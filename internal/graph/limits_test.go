package graph

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLimitsPrepareForPublishing(t *testing.T) {
	limits := newLimitTracker(10)

	for i := 0; i < 10; i++ {
		result := limits.prepareForPublishing()
		require.True(t, result)
	}

	result := limits.prepareForPublishing()
	require.False(t, result)
}

func TestLimitsMarkAlreadyPublished(t *testing.T) {
	limits := newLimitTracker(10)

	err := limits.markAlreadyPublished(5)
	require.Nil(t, err)

	err = limits.markAlreadyPublished(5)
	require.Nil(t, err)

	require.Panics(t, func() {
		_ = limits.markAlreadyPublished(1)
	})
}
