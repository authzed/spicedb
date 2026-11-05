//go:build integration

package queryconsistency_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	schematesting "github.com/authzed/spicedb/pkg/schema/v2/testing"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestQueryPlanScenarioProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		scenario := schematesting.DrawScenario(t, schematesting.DrawShape(t, 3))
		checkScenarioTransitions(t, scenario)
	})
}

// Assert hand-derived memberships as well as parity between implementations:
// a generator that produces only empty results must not make this test pass.
func TestQueryPlanShapeMembership(t *testing.T) {
	for _, tc := range []struct {
		name  string
		shape *schematesting.Shape
		want  []string
	}{
		{"direct", schematesting.Direct(), []string{"shared", "right"}},
		{"empty", schematesting.Empty(), nil},
		{"alias", schematesting.Alias(schematesting.Alias(schematesting.Direct())), []string{"shared", "right"}},
		{"union", schematesting.Union(schematesting.Direct(), schematesting.Direct()), []string{"shared", "left", "right"}},
		{"intersection", schematesting.Intersection(schematesting.Direct(), schematesting.Direct()), []string{"shared"}},
		{"exclusion", schematesting.Exclusion(schematesting.Direct(), schematesting.Direct()), []string{"right"}},
		{"userset", schematesting.Userset(schematesting.Direct()), []string{"shared", "left", "right"}},
		{"arrow", schematesting.Arrow(schematesting.Direct()), []string{"shared", "left", "right"}},
		{"all_arrow", schematesting.AllArrow(schematesting.Direct()), []string{"shared"}},
		{"userset_intersection", schematesting.Userset(schematesting.Intersection(schematesting.Direct(), schematesting.Direct())), []string{"shared"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rapid.Check(t, func(t *rapid.T) {
				scenario := schematesting.DrawScenario(t, tc.shape)
				checkpoints := checkScenarioTransitions(t, scenario)
				initial := checkpoints[0]
				var got []string
				for _, subject := range scenario.Subjects[:4] {
					if initial[tuple.RelationshipReference{Resource: scenario.Resources[1], Subject: subject}] {
						got = append(got, subject.ObjectID)
					}
				}
				require.ElementsMatch(t, tc.want, got)
				if tc.name == "direct" {
					require.NotEqual(t, initial, checkpoints[2], "removing direct memberships must revoke access")
				}
			})
		})
	}
}

func checkScenarioTransitions(t *rapid.T, scenario schematesting.Scenario) []map[tuple.RelationshipReference]bool {
	ds, err := memdb.NewMemdbDatastore(0, time.Second, memdb.DisableGC)
	require.NoError(t, err)
	defer ds.Close()
	definitions, _, err := scenario.Schema.ToDefinitions()
	require.NoError(t, err)
	revision, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		if err := tx.LegacyWriteNamespaces(ctx, definitions...); err != nil {
			return err
		}
		updates := make([]tuple.RelationshipUpdate, 0, len(scenario.Relationships))
		for _, rel := range scenario.Relationships {
			updates = append(updates, tuple.Create(rel))
		}
		return tx.WriteRelationships(ctx, updates)
	})
	require.NoError(t, err)
	dispatcher, err := graph.NewLocalOnlyDispatcher(graph.MustNewDefaultDispatcherParametersForTesting())
	require.NoError(t, err)
	defer dispatcher.Close()
	dl := datalayer.NewDataLayer(ds)
	dispatchCtx := datalayer.ContextWithDataLayer(t.Context(), dl)

	// Reuse the compiled iterators across revisions to exercise stale-state bugs.
	root := scenario.Resources[0].RelationReference()
	iterators := make([]query.Iterator, 0, 2)
	for _, optimized := range []bool{false, true} {
		outline, err := query.BuildOutlineFromSchema(scenario.Schema, root.ObjectType, root.Relation)
		require.NoError(t, err)
		if optimized {
			params := queryopt.RequestParams{Operation: query.OperationCheck, SubjectType: "user", SubjectRelation: tuple.Ellipsis}
			outline, err = queryopt.ApplyOptimizations(outline, queryopt.OptimizersForRequest(params), params)
			require.NoError(t, err)
		}
		iterator, err := outline.Compile()
		require.NoError(t, err)
		iterators = append(iterators, iterator)
	}
	check := func(step int) map[tuple.RelationshipReference]bool {
		results := map[tuple.RelationshipReference]bool{}
		for _, resource := range scenario.Resources {
			for _, subject := range scenario.Subjects {
				response, err := dispatcher.DispatchCheck(dispatchCtx, &dispatchv1.DispatchCheckRequest{
					ResourceRelation: root.ToCoreRR(),
					ResourceIds:      []string{resource.ObjectID},
					ResultsSetting:   dispatchv1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
					Subject:          subject.ToCoreONR(),
					Metadata: &dispatchv1.ResolverMeta{
						AtRevision: revision.String(), DepthRemaining: 50,
						SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
						TraversalBloom: dispatchv1.MustNewTraversalBloomFilter(50),
					},
				})
				require.NoError(t, err)
				classic := response.ResultsByResourceId[resource.ObjectID] != nil && response.ResultsByResourceId[resource.ObjectID].Membership == dispatchv1.ResourceCheckResult_MEMBER
				key := tuple.RelationshipReference{Resource: resource, Subject: subject}
				results[key] = classic
				for mode, iterator := range iterators {
					qctx := query.NewLocalContext(t.Context(), query.WithRevisionedReader(dl.SnapshotReader(revision, datalayer.NoSchemaHashForTesting)))
					path, err := qctx.Check(iterator, query.GetObject(resource), subject)
					require.NoError(t, err)
					require.Equal(t, classic, path != nil, "step=%d optimized=%t check=%s initial=%v batches=%v", step, mode == 1, key, scenario.Relationships, scenario.UpdateBatches)
				}
			}
		}
		return results
	}
	initial := check(-1)
	initialRevision := revision
	checkpoints := make([]map[tuple.RelationshipReference]bool, 0, 1+len(scenario.UpdateBatches))
	checkpoints = append(checkpoints, initial)
	for step, batch := range scenario.UpdateBatches {
		revision, err = ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
			return tx.WriteRelationships(ctx, batch)
		})
		require.NoError(t, err, "batch %d: %v", step, batch)
		results := check(step)
		if step == 2 {
			require.Equal(t, checkpoints[len(checkpoints)-1], results, "deleting again must be idempotent")
		}
		if step == 0 || step == 3 {
			require.Equal(t, initial, results, "touch/restore must preserve initial permissions")
		}
		checkpoints = append(checkpoints, results)
	}
	revision = initialRevision
	require.Equal(t, initial, check(len(scenario.UpdateBatches)), "later updates must not affect the original revision")
	return checkpoints
}
