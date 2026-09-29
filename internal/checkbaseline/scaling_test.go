package checkbaseline

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBackgroundGrowthPreservesWork(t *testing.T) {
	datasets := BackgroundDatasets([]int{200, 1000})
	a, err := Audit(t.Context(), datasets, AuditConfig{Policy: DefaultPolicy(), Repetitions: 1, DatasetPattern: "^sized/direct/", CasePattern: ".*"})
	require.NoError(t, err)
	require.Len(t, a.Results, 4)
	require.Equal(t, 200, a.Results[0].Dataset.Relationships)
	require.Equal(t, 1000, a.Results[2].Dataset.Relationships)
	require.NotEqual(t, a.Results[0].Dataset.Hash, a.Results[2].Dataset.Hash)
	for i := range 2 {
		for e := range 2 {
			require.Empty(t, CompareWork(a.Results[i].Engines[e].Work[0], a.Results[i+2].Engines[e].Work[0]))
		}
	}
}
func TestScalingCatalogUniqueAndSeparateAxes(t *testing.T) {
	datasets := ScalingDatasets()
	seen := map[string]bool{}
	for _, d := range datasets {
		require.False(t, seen[d.ID], d.ID)
		seen[d.ID] = true
	}
	require.True(t, seen["sized/direct/background/1000000"])
	require.True(t, seen["generated/direct/dense1000000"])
	require.True(t, seen["generated/arrow/fanout10000"])
	require.True(t, seen["generated/recursive/depth128"])
}
func TestDeepScalingDecisions(t *testing.T) {
	a, err := Audit(t.Context(), ScalingDatasets(), AuditConfig{Policy: DefaultPolicy(), Repetitions: 1, DatasetPattern: "^generated/recursive/depth64$", CasePattern: ".*"})
	require.NoError(t, err)
	require.Len(t, a.Results, 2)
	for _, r := range a.Results {
		require.True(t, r.Valid)
	}
}

func TestAllBackgroundFamiliesPreserveDecisionsAndWork(t *testing.T) {
	for _, family := range []string{"arrow", "groups", "recursive", "union", "intersection", "exclusion", "all"} {
		t.Run(family, func(t *testing.T) {
			a, err := Audit(t.Context(), BackgroundDatasets([]int{1000, 2000}), AuditConfig{Policy: DefaultPolicy(), Repetitions: 1, DatasetPattern: "^sized/" + family + "/", CasePattern: ".*"})
			require.NoError(t, err)
			n := len(a.Results) / 2
			require.Positive(t, n)
			for i := range n {
				require.Equal(t, 1000, a.Results[i].Dataset.Relationships)
				require.Equal(t, 2000, a.Results[i+n].Dataset.Relationships)
				for e := range 2 {
					require.Empty(t, CompareWork(a.Results[i].Engines[e].Work[0], a.Results[i+n].Engines[e].Work[0]), family)
				}
			}
		})
	}
}
