package checkbaseline

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompareWorkEqualCountsDifferentFilters(t *testing.T) {
	a := Work{Events: []*WorkEvent{{Operation: "query", Filter: json.RawMessage(`{"type":"a"}`), Rows: 1}}}
	b := Work{Events: []*WorkEvent{{Operation: "query", Filter: json.RawMessage(`{"type":"b"}`), Rows: 1}}}
	require.NotEmpty(t, CompareWork(a, b))
	require.Empty(t, CompareWork(a, a))
}

func TestAuditGenerated(t *testing.T) {
	datasets := GeneratedDatasets([]Scale{{Name: "small", Fanout: 3, Depth: 3, DirectRelationships: 10}})
	a, err := Audit(context.Background(), datasets, AuditConfig{Policy: DefaultPolicy(), Repetitions: 3, DatasetPattern: "^generated/direct/small$", CasePattern: ".*"})
	require.NoError(t, err)
	require.Len(t, a.Results, 2)
	for _, r := range a.Results {
		require.True(t, r.Valid)
		require.Positive(t, r.Engines[0].Preparation.NSPerOp)
		require.Positive(t, r.Engines[1].Preparation.NSPerOp)
		require.Len(t, r.Engines[0].Work, 3)
		require.Equal(t, 11, r.Dataset.Relationships)
	}
}

func TestAuditConsistencyFixture(t *testing.T) {
	ds, err := FixtureDatasets("../..")
	require.NoError(t, err)
	a, err := Audit(t.Context(), ds, AuditConfig{Policy: DefaultPolicy(), Repetitions: 1, DatasetPattern: "^consistency/basicrbac.yaml$", CasePattern: ".*"})
	require.NoError(t, err)
	require.NotEmpty(t, a.Results)
	require.Empty(t, a.Omissions)
}

func TestAuditProfiles(t *testing.T) {
	ds := GeneratedDatasets([]Scale{{Name: "small", Fanout: 3, Depth: 3, DirectRelationships: 10}})
	a, err := Audit(t.Context(), ds, AuditConfig{Policy: DefaultPolicy(), Repetitions: 1, DatasetPattern: "^generated/direct/small$", CasePattern: "hit", Profiles: []string{"memdb", "delay"}})
	require.NoError(t, err)
	require.Len(t, a.Results, 2)
	for i := range 2 {
		require.Len(t, relationshipEvents(a.Results[1].Engines[i].Work[0]), len(relationshipEvents(a.Results[0].Engines[i].Work[0])))
	}
}

func TestMixedTraitExpiredRelationship(t *testing.T) {
	ds, err := FixtureDatasets("../..")
	require.NoError(t, err)
	a, err := Audit(t.Context(), ds, AuditConfig{Policy: DefaultPolicy(), Repetitions: 1, DatasetPattern: "^steelthread/document-with-traits.yaml$", CasePattern: ".*"})
	require.NoError(t, err)
	require.Len(t, a.Results, 5)
	for _, r := range a.Results {
		require.True(t, r.Valid, r.Case.ID)
	}
}

func TestAlignedTraversalWork(t *testing.T) {
	ds := GeneratedDatasets([]Scale{{Name: "small", Fanout: 3, Depth: 3, DirectRelationships: 10}})
	a, err := Audit(t.Context(), ds, AuditConfig{Policy: DefaultPolicy(), Repetitions: 1, DatasetPattern: "^generated/(recursive|exclusion|all)/small$", CasePattern: ".*"})
	require.NoError(t, err)
	for _, r := range a.Results {
		x, y := relationshipEvents(r.Engines[0].Work[0]), relationshipEvents(r.Engines[1].Work[0])
		require.Len(t, y, len(x), r.Dataset.ID+"/"+r.Case.ID)
		rows := func(es []*WorkEvent) int {
			n := 0
			for _, e := range es {
				n += e.Rows
			}
			return n
		}
		require.Equal(t, rows(x), rows(y), r.Dataset.ID+"/"+r.Case.ID)
	}
}

func TestCaveatAccounting(t *testing.T) {
	ds, err := FixtureDatasets("../..")
	require.NoError(t, err)
	a, err := Audit(t.Context(), ds, AuditConfig{Policy: DefaultPolicy(), Repetitions: 1, DatasetPattern: "^steelthread/document-with-traits.yaml$", CasePattern: "caveat-true"})
	require.NoError(t, err)
	for _, e := range a.Results[0].Engines {
		leaves := 0
		for _, event := range e.Work[0].Events {
			if event.Operation == "caveat-leaf" {
				leaves++
			}
		}
		require.Positive(t, leaves, e.Name)
	}
}

func TestDeferredCaveatWork(t *testing.T) {
	ds, err := FixtureDatasets("../..")
	require.NoError(t, err)
	a, err := Audit(t.Context(), ds, AuditConfig{Policy: DefaultPolicy(), Repetitions: 1, DatasetPattern: "^consistency/(basiccaveat|caveatarrow).yaml$", CasePattern: ".*"})
	require.NoError(t, err)
	for _, r := range a.Results {
		x, y := relationshipEvents(r.Engines[0].Work[0]), relationshipEvents(r.Engines[1].Work[0])
		require.Len(t, y, len(x), r.Dataset.ID+"/"+r.Case.ID)
	}
}

func TestDirectWorkFullyMatches(t *testing.T) {
	ds := GeneratedDatasets([]Scale{{Name: "small", Fanout: 3, Depth: 3, DirectRelationships: 10}})
	a, err := Audit(t.Context(), ds, AuditConfig{Policy: DefaultPolicy(), Repetitions: 1, DatasetPattern: "^generated/direct/small$", CasePattern: ".*"})
	require.NoError(t, err)
	for _, r := range a.Results {
		require.Empty(t, r.Differences, r.Case.ID)
	}
}

func TestWildcardReadCoalescing(t *testing.T) {
	ds, err := FixtureDatasets("../..")
	require.NoError(t, err)
	a, err := Audit(t.Context(), ds, AuditConfig{Policy: DefaultPolicy(), Repetitions: 1, DatasetPattern: "^consistency/public.yaml$", CasePattern: "^allow/"})
	require.NoError(t, err)
	require.NotEmpty(t, a.Results)
	for _, r := range a.Results {
		require.Len(t, relationshipEvents(r.Engines[1].Work[0]), len(relationshipEvents(r.Engines[0].Work[0])), r.Dataset.ID+"/"+r.Case.ID)
	}
}

func TestCompareWorkCaveatEvaluationsAndErrors(t *testing.T) {
	a := Work{Events: []*WorkEvent{{Operation: "query"}}}
	b := Work{Events: []*WorkEvent{{Operation: "query"}, {Operation: "caveat-leaf"}}}
	require.NotEmpty(t, CompareWork(a, b))
	b = Work{Events: []*WorkEvent{{Operation: "query", Error: "read failed"}}}
	require.NotEmpty(t, CompareWork(a, b))
}

func TestSetOperationScales(t *testing.T) {
	ds := GeneratedDatasets([]Scale{{Name: "small", DirectRelationships: 2}, {Name: "large", DirectRelationships: 10}})
	a, err := Audit(t.Context(), ds, AuditConfig{Policy: DefaultPolicy(), Repetitions: 1, DatasetPattern: "^generated/union/", CasePattern: "hit"})
	require.NoError(t, err)
	require.Len(t, a.Results, 2)
	require.Greater(t, a.Results[1].Dataset.Relationships, a.Results[0].Dataset.Relationships)
	require.NotEqual(t, a.Results[0].Dataset.Hash, a.Results[1].Dataset.Hash)
}

func TestLateWorkInvalidatesReturnedSnapshot(t *testing.T) {
	rec := NewRecorder()
	ctx := WithRecorder(t.Context(), rec)
	event(ctx, "query", nil, nil, 0)
	a := Artifact{Results: []Result{{Valid: true, Engines: []EngineResult{{Name: "fake", Work: []Work{rec.Seal()}}}}}}
	event(ctx, "query", nil, nil, 0)
	require.Equal(t, 1, finalizeRecordings(&a, []auditRecording{{0, 0, 0, rec}}))
	require.False(t, a.Results[0].Valid)
	require.Equal(t, 1, a.Results[0].Engines[0].Work[0].LateEvents)
	require.Len(t, a.Results[0].Engines[0].Work[0].Events, 2)
}

func TestIndirectSubjectWorkFullyMatches(t *testing.T) {
	ds := GeneratedDatasets([]Scale{{Name: "indirect", Fanout: 4, Depth: 3, DirectRelationships: 10}})
	a, err := Audit(t.Context(), ds, AuditConfig{SchemaMode: "read-new-write-new", Policy: DefaultPolicy(), Repetitions: 2, DatasetPattern: "^generated/(arrow|all|groups)/indirect$", CasePattern: ".*"})
	require.NoError(t, err)
	require.Len(t, a.Results, 9)
	for _, r := range a.Results {
		require.Equal(t, "relationship work matched", r.Status, r.Dataset.ID+"/"+r.Case.ID+": "+fmt.Sprint(r.Differences))
	}
}

// Exercise adapter strategies against the full report catalog, including
// heterogeneous usersets and caveats.
func TestCatalogCorrectness(t *testing.T) {
	datasets, err := Catalog("../..")
	require.NoError(t, err)
	artifact, err := Audit(t.Context(), datasets, AuditConfig{
		Policy: DefaultPolicy(), Repetitions: 1, RepoRoot: "../..",
		DatasetPattern: ".*", CasePattern: ".*", SchemaMode: "read-new-write-new",
	})
	require.NoError(t, err)
	require.NotEmpty(t, artifact.Results)
	for _, result := range artifact.Results {
		require.True(t, result.Valid, result.Dataset.ID+"/"+result.Case.ID)
		require.Len(t, result.Engines, 2)
		for _, engine := range result.Engines {
			require.Empty(t, engine.Error)
		}
	}
}

func TestAuditRejectsInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*AuditConfig)
	}{
		{"repetitions", func(c *AuditConfig) { c.Repetitions = 0 }},
		{"samples", func(c *AuditConfig) { c.Samples = 1 }},
		{"dataset expression", func(c *AuditConfig) { c.DatasetPattern = "[" }},
		{"case expression", func(c *AuditConfig) { c.CasePattern = "[" }},
		{"profile", func(c *AuditConfig) { c.Profiles = []string{"invalid"} }},
		{"schema mode", func(c *AuditConfig) { c.SchemaMode = "invalid" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := AuditConfig{Policy: DefaultPolicy(), Repetitions: 1}
			tc.change(&cfg)
			_, err := Audit(t.Context(), nil, cfg)
			require.Error(t, err)
		})
	}
}
