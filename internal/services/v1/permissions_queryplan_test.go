package v1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExperimentalQueryPlanConfigZeroValue(t *testing.T) {
	// A zero-value ExperimentalQueryPlanConfig must disable all operations so that
	// the default behaviour is to fall through to the standard dispatcher.
	cfg := ExperimentalQueryPlanConfig{}
	require.False(t, cfg.Check, "Check should be disabled by default")
	require.False(t, cfg.LookupResources, "LookupResources should be disabled by default")
	require.False(t, cfg.LookupSubjects, "LookupSubjects should be disabled by default")
}

func TestExperimentalQueryPlanConfigIndependentFields(t *testing.T) {
	// Each field can be enabled independently without affecting the others.
	t.Run("only Check", func(t *testing.T) {
		cfg := ExperimentalQueryPlanConfig{Check: true}
		require.True(t, cfg.Check)
		require.False(t, cfg.LookupResources)
		require.False(t, cfg.LookupSubjects)
	})

	t.Run("only LookupResources", func(t *testing.T) {
		cfg := ExperimentalQueryPlanConfig{LookupResources: true}
		require.False(t, cfg.Check)
		require.True(t, cfg.LookupResources)
		require.False(t, cfg.LookupSubjects)
	})

	t.Run("only LookupSubjects", func(t *testing.T) {
		cfg := ExperimentalQueryPlanConfig{LookupSubjects: true}
		require.False(t, cfg.Check)
		require.False(t, cfg.LookupResources)
		require.True(t, cfg.LookupSubjects)
	})

	t.Run("all enabled", func(t *testing.T) {
		cfg := ExperimentalQueryPlanConfig{Check: true, LookupResources: true, LookupSubjects: true}
		require.True(t, cfg.Check)
		require.True(t, cfg.LookupResources)
		require.True(t, cfg.LookupSubjects)
	})
}
