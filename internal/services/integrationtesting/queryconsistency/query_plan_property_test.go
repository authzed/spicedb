//go:build integration

package queryconsistency_test

import (
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/schema/v2"
	schematesting "github.com/authzed/spicedb/pkg/schema/v2/testing"
	"github.com/authzed/spicedb/pkg/tuple"
)

// TestQueryPlanCheckProperty compares both implementations on the same generated
// schema, relationships, revision, and check requests. Rapid reports a seed and
// shrinks the generated case when a discrepancy is found.
func TestQueryPlanCheckProperty(t *testing.T) {
	for _, mode := range []struct {
		name      string
		optimized bool
	}{
		{"plain", false},
		{"optimized", true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			schematesting.CheckWithSchema(t, func(t *rapid.T, generated *schema.Schema, relGenerator schematesting.RelationshipGenerator) {
				definitions, _, err := generated.ToDefinitions()
				require.NoError(t, err)

				ds, err := memdb.NewMemdbDatastore(0, time.Second, memdb.DisableGC)
				require.NoError(t, err)
				defer ds.Close()

				ids := []string{"o_a", "o_b", "o_c"}
				relationships := make([]tuple.Relationship, 0, 20)
				for rel := range relGenerator.GenerateRelationships(t) {
					rel.Resource.ObjectID = rapid.SampledFrom(ids).Draw(t, "resourceID")
					rel.Subject.ObjectID = rapid.SampledFrom(ids).Draw(t, "subjectID")
					relationships = append(relationships, rel)
					if len(relationships) == 20 {
						break
					}
				}

				revision, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
					if err := tx.LegacyWriteNamespaces(ctx, definitions...); err != nil {
						return err
					}
					updates := make([]tuple.RelationshipUpdate, 0, len(relationships))
					for _, rel := range relationships {
						updates = append(updates, tuple.Touch(rel))
					}
					return tx.WriteRelationships(ctx, updates)
				})
				require.NoError(t, err)

				dispatcher, err := graph.NewLocalOnlyDispatcher(graph.MustNewDefaultDispatcherParametersForTesting())
				require.NoError(t, err)
				defer dispatcher.Close()
				dispatchCtx := datalayer.ContextWithDataLayer(t.Context(), datalayer.NewDataLayer(ds))

				subjectTypes := make([]string, 0)
				for name, def := range generated.Definitions() {
					if len(def.Permissions()) == 0 {
						subjectTypes = append(subjectTypes, name)
					}
				}
				slices.Sort(subjectTypes)
				resourceTypes := slices.Collect(maps.Keys(generated.Definitions()))
				slices.Sort(resourceTypes)
				for _, resourceType := range resourceTypes {
					def := generated.Definitions()[resourceType]
					permissions := slices.Collect(maps.Keys(def.Permissions()))
					slices.Sort(permissions)
					for _, permission := range permissions {
						outline, err := query.BuildOutlineFromSchema(generated, resourceType, permission)
						require.NoError(t, err)
						for _, subjectType := range subjectTypes {
							plan := outline
							if mode.optimized {
								params := queryopt.RequestParams{Operation: query.OperationCheck, SubjectType: subjectType, SubjectRelation: tuple.Ellipsis}
								plan, err = queryopt.ApplyOptimizations(outline, queryopt.OptimizersForRequest(params), params)
								require.NoError(t, err)
							}
							iterator, err := plan.Compile()
							require.NoError(t, err)
							for _, resourceID := range ids {
								for _, subjectID := range ids {
									subject := tuple.ObjectAndRelation{ObjectType: subjectType, ObjectID: subjectID, Relation: tuple.Ellipsis}
									bloom, err := dispatchv1.NewTraversalBloomFilter(50)
									require.NoError(t, err)
									response, err := dispatcher.DispatchCheck(dispatchCtx, &dispatchv1.DispatchCheckRequest{
										ResourceRelation: &core.RelationReference{Namespace: resourceType, Relation: permission},
										ResourceIds:      []string{resourceID},
										ResultsSetting:   dispatchv1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
										Subject:          subject.ToCoreONR(),
										Metadata: &dispatchv1.ResolverMeta{
											AtRevision: revision.String(), DepthRemaining: 50,
											SchemaHash: []byte(datalayer.NoSchemaHashForTesting), TraversalBloom: bloom,
										},
									})
									require.NoError(t, err)
									classic := response.ResultsByResourceId[resourceID] != nil && response.ResultsByResourceId[resourceID].Membership == dispatchv1.ResourceCheckResult_MEMBER
									qctx := query.NewLocalContext(t.Context(), query.WithRevisionedReader(datalayer.NewDataLayer(ds).SnapshotReader(revision, datalayer.NoSchemaHashForTesting)))
									path, err := qctx.Check(iterator, query.NewObject(resourceType, resourceID), subject)
									require.NoError(t, err)
									require.Equal(t, classic, path != nil, "check mismatch: %s:%s#%s@%s (optimized=%t); relationships=%v",
										resourceType, resourceID, permission, tuple.StringONR(subject), mode.optimized, relationships)
								}
							}
						}
					}
				}
			})
		})
	}
}
