//go:build !skipintegrationtests

package integrationtesting_test

import (
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/services/integrationtesting/consistencytestutil"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

func TestQueryPlanConsistency(t *testing.T) { // nolint:tparallel
	t.Parallel()
	consistencyTestFiles, err := consistencytestutil.ListTestConfigs()
	require.NoError(t, err)
	for _, filePath := range consistencyTestFiles {
		filePath := filePath

		t.Run(filepath.Base(filePath), func(t *testing.T) {
			runQueryPlanConsistencyForFile(t, filePath)
		})
	}
}

type queryPlanConsistencyHandle struct {
	populated *validationfile.PopulatedValidationFile
	schema    *schema.Schema
	revision  datastore.Revision
	ds        datastore.Datastore
}

func (q *queryPlanConsistencyHandle) buildContext(t *testing.T) *query.Context {
	return &query.Context{
		Context:      t.Context(),
		Executor:     query.LocalExecutor{},
		Reader:       q.ds.SnapshotReader(q.revision),
		CaveatRunner: caveats.NewCaveatRunner(caveattypes.Default.TypeSet),
		TraceLogger:  query.NewTraceLogger(), // Enable tracing for debugging
	}
}

func runQueryPlanConsistencyForFile(t *testing.T, filePath string) {
	require := require.New(t)

	ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, testTimedelta, memdb.DisableGC)
	require.NoError(err)
	populated, _, err := validationfile.PopulateFromFiles(t.Context(), ds, caveattypes.Default.TypeSet, []string{filePath})
	require.NoError(err)

	headRevision, err := ds.HeadRevision(t.Context())
	require.NoError(err)

	schemaView, err := schema.BuildSchemaFromDefinitions(populated.NamespaceDefinitions, populated.CaveatDefinitions)
	require.NoError(err)

	handle := &queryPlanConsistencyHandle{
		populated: populated,
		schema:    schemaView,
		revision:  headRevision,
		ds:        ds,
	}
	runQueryPlanAssertions(t, handle)
	runQueryPlanLookupResources(t, handle)
}

func runQueryPlanAssertions(t *testing.T, handle *queryPlanConsistencyHandle) {
	t.Run("assertions", func(t *testing.T) {
		for _, parsedFile := range handle.populated.ParsedFiles {
			for _, entry := range []struct {
				name                   string
				assertions             []blocks.Assertion
				expectedPermissionship v1.CheckPermissionResponse_Permissionship
			}{
				{
					"true",
					parsedFile.Assertions.AssertTrue,
					v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
				},
				{
					"caveated",
					parsedFile.Assertions.AssertCaveated,
					v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION,
				},
				{
					"false",
					parsedFile.Assertions.AssertFalse,
					v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
				},
			} {
				t.Run(entry.name, func(t *testing.T) {
					for _, assertion := range entry.assertions {
						t.Run(assertion.RelationshipWithContextString, func(t *testing.T) {
							// Run both unoptimized and optimized versions
							for _, optimizationMode := range []struct {
								name     string
								optimize bool
							}{
								{"unoptimized", false},
								{"optimized", true},
							} {
								t.Run(optimizationMode.name, func(t *testing.T) {
									require := require.New(t)

									rel := assertion.Relationship
									it, err := query.BuildIteratorFromSchema(handle.schema, rel.Resource.ObjectType, rel.Resource.Relation)
									require.NoError(err)

									// Apply static optimizations if requested
									if optimizationMode.optimize {
										it, _, err = query.ApplyOptimizations(it, query.StaticOptimizations)
										require.NoError(err)
									}

									qctx := handle.buildContext(t)

									// Add caveat context from assertion if available
									if len(assertion.CaveatContext) > 0 {
										qctx.CaveatContext = assertion.CaveatContext
									}

									seq, err := qctx.Check(it, []query.Object{query.GetObject(rel.Resource)}, rel.Subject)
									require.NoError(err)

									rels, err := query.CollectAll(seq)
									require.NoError(err)

									// Print trace if test fails
									if qctx.TraceLogger != nil {
										defer func() {
											if t.Failed() {
												t.Logf("Trace for %s:\n%s", entry.name, qctx.TraceLogger.DumpTrace())
												// Also print the tree structure for debugging
												if it != nil {
													t.Logf("Tree structure:\n%s", it.Explain().IndentString(0))
												}
											}
										}()
									}

									switch entry.expectedPermissionship {
									case v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION:
										require.Len(rels, 1)
										require.NotNil(rels[0].Caveat)
									case v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION:
										require.Len(rels, 1)
										require.Nil(rels[0].Caveat)
									case v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION:
										if len(rels) != 0 && qctx.TraceLogger != nil {
											t.Logf("Expected 0 relations but got %d. Trace:\n%s", len(rels), qctx.TraceLogger.DumpTrace())
										}
										require.Empty(rels)
									}
								})
							}
						})
					}
				})
			}
		}
	})
}

func runQueryPlanLookupResources(t *testing.T, handle *queryPlanConsistencyHandle) {
	dsCtx := datastoremw.ContextWithHandle(t.Context())
	require.NoError(t, datastoremw.SetInContext(dsCtx, handle.ds))
	accessibilitySet := consistencytestutil.BuildAccessibilitySet(t, dsCtx, handle.populated, handle.ds)
	// Run a lookup resources for each resource type and ensure that the returned objects are those
	// that are accessible to the subject.
	testQueryPlanForEachResourceType(t, handle.populated, "validate_lookup_resources",
		func(t *testing.T, resourceRelation tuple.RelationReference) {
			t.Parallel()
			for _, subject := range accessibilitySet.AllSubjectsNoWildcards() {
				subject := subject
				t.Run(tuple.StringONR(subject), func(t *testing.T) {
					accessibleResources := accessibilitySet.LookupAccessibleResources(resourceRelation, subject)
					queryCtx := handle.buildContext(t)
					it, err := query.BuildIteratorFromSchema(handle.schema, resourceRelation.ObjectType, resourceRelation.Relation)
					require.NoError(t, err)

					// Perform a lookup call and ensure it returns the at least the same set of object IDs.
					// Loop until all resources have been found or we've hit max iterations.
					resolvedResources := make(map[string]bool)
					paths, err := queryCtx.IterResources(it, subject)
					require.NoError(t, err)

					for path, err := range paths {
						require.NoError(t, err)
						resolvedResources[path.Resource.ObjectID] = true
					}

					// Print trace if test fails
					if queryCtx.TraceLogger != nil {
						defer func() {
							if t.Failed() {
								t.Logf("Trace for %s:\n%s", subject, queryCtx.TraceLogger.DumpTrace())
								// Also print the tree structure for debugging
								if it != nil {
									t.Logf("Tree structure:\n%s", it.Explain().IndentString(0))
								}
							}
						}()
					}

					requireSameSets(t,
						slices.Collect(maps.Keys(accessibleResources)),
						slices.Collect(maps.Keys(resolvedResources)),
					)
				})
			}
		})
}

func testQueryPlanForEachResourceType(
	t *testing.T,
	populated *validationfile.PopulatedValidationFile,
	prefix string,
	handler func(t *testing.T, resourceType tuple.RelationReference),
) {
	for _, resourceType := range populated.NamespaceDefinitions {
		resourceType := resourceType
		for _, relation := range resourceType.Relation {
			relation := relation
			t.Run(fmt.Sprintf("%s_%s_%s_", prefix, resourceType.Name, relation.Name),
				func(t *testing.T) {
					handler(t, tuple.RelationReference{
						ObjectType: resourceType.Name,
						Relation:   relation.Name,
					})
				})
		}
	}
}
