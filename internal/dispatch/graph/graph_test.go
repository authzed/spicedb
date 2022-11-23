package graph

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConcurrencyLimitsWithOverallDefaultLimit(t *testing.T) {
	cl := ConcurrencyLimits{}
	require.Equal(t, uint16(0), cl.Check)
	require.Equal(t, uint16(0), cl.LookupResources)
	require.Equal(t, uint16(0), cl.LookupSubjects)
	require.Equal(t, uint16(0), cl.ReachableResources)

	withDefaults := cl.WithOverallDefaultLimit(51)

	require.Equal(t, uint16(0), cl.Check)
	require.Equal(t, uint16(0), cl.LookupResources)
	require.Equal(t, uint16(0), cl.LookupSubjects)
	require.Equal(t, uint16(0), cl.ReachableResources)

	require.Equal(t, uint16(51), withDefaults.Check)
	require.Equal(t, uint16(51), withDefaults.LookupResources)
	require.Equal(t, uint16(51), withDefaults.LookupSubjects)
	require.Equal(t, uint16(51), withDefaults.ReachableResources)
}

func TestSharedConcurrencyLimits(t *testing.T) {
	cl := SharedConcurrencyLimits(42)
	require.Equal(t, uint16(42), cl.Check)
	require.Equal(t, uint16(42), cl.LookupResources)
	require.Equal(t, uint16(42), cl.LookupSubjects)
	require.Equal(t, uint16(42), cl.ReachableResources)

	withDefaults := cl.WithOverallDefaultLimit(51)

	require.Equal(t, uint16(42), cl.Check)
	require.Equal(t, uint16(42), cl.LookupResources)
	require.Equal(t, uint16(42), cl.LookupSubjects)
	require.Equal(t, uint16(42), cl.ReachableResources)

	require.Equal(t, uint16(42), withDefaults.Check)
	require.Equal(t, uint16(42), withDefaults.LookupResources)
	require.Equal(t, uint16(42), withDefaults.LookupSubjects)
	require.Equal(t, uint16(42), withDefaults.ReachableResources)
}
