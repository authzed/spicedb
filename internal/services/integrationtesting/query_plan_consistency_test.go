//go:build !skipintegrationtests

package integrationtesting_test

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/services/integrationtesting/consistencytestutil"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
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
	return query.NewLocalContext(t.Context(),
		query.WithReader(datalayer.NewDataLayer(q.ds).SnapshotReader(q.revision)),
		query.WithCaveatRunner(caveats.NewCaveatRunner(caveattypes.Default.TypeSet)),
		query.WithTraceLogger(query.NewTraceLogger())) // Enable tracing for debugging
}

func runQueryPlanConsistencyForFile(t *testing.T, filePath string) {
	require := require.New(t)

	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, testTimedelta, memdb.DisableGC)
	require.NoError(err)
	populated, _, err := validationfile.PopulateFromFiles(t.Context(), datalayer.NewDataLayer(ds), caveattypes.Default.TypeSet, []string{filePath})
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

	t.Run("lookup_resources", func(t *testing.T) {
		if os.Getenv("TEST_QUERY_PLAN_RESOURCES") == "" {
			t.Skip("Skipping IterResources tests: set TEST_QUERY_PLAN_RESOURCES=true to enable")
		}
		runQueryPlanLookupResources(t, handle)
	})

	t.Run("lookup_subjects", func(t *testing.T) {
		if os.Getenv("TEST_QUERY_PLAN_SUBJECTS") == "" {
			t.Skip("Skipping IterSubjects tests: set TEST_QUERY_PLAN_SUBJECTS=true to enable")
		}
		runQueryPlanLookupSubjects(t, handle)
	})
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
	dsCtx := datalayer.ContextWithHandle(t.Context())
	require.NoError(t, datalayer.SetInContext(dsCtx, datalayer.NewDataLayer(handle.ds)))
	accessibilitySet := consistencytestutil.BuildAccessibilitySet(t, dsCtx, handle.populated, handle.ds)
	// Run a lookup resources for each resource type and ensure that the returned objects are those
	// that are accessible to the subject.
	testQueryPlanForEachResourceType(t, handle.populated, "validate_lookup_resources",
		func(t *testing.T, resourceRelation tuple.RelationReference) {
			t.Parallel()
			for _, subject := range accessibilitySet.AllSubjectsNoWildcards() {
				t.Run(tuple.StringONR(subject), func(t *testing.T) {
					accessibleResources := accessibilitySet.LookupAccessibleResources(resourceRelation, subject)
					queryCtx := handle.buildContext(t)
					it, err := query.BuildIteratorFromSchema(handle.schema, resourceRelation.ObjectType, resourceRelation.Relation)
					require.NoError(t, err)

					// Perform a lookup call and ensure it returns the at least the same set of object IDs.
					// Loop until all resources have been found or we've hit max iterations.
					resolvedResources := make(map[string]bool)
					paths, err := queryCtx.IterResources(it, subject, query.NoObjectFilter())
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

					require.ElementsMatch(t,
						slices.Collect(maps.Keys(accessibleResources)),
						slices.Collect(maps.Keys(resolvedResources)),
						"expected accessibleResources in list A don't match actual resolvedResources in list B",
					)
				})
			}
		})
}

func runQueryPlanLookupSubjects(t *testing.T, handle *queryPlanConsistencyHandle) {
	dsCtx := datalayer.ContextWithHandle(t.Context())
	require.NoError(t, datalayer.SetInContext(dsCtx, datalayer.NewDataLayer(handle.ds)))
	accessibilitySet := consistencytestutil.BuildAccessibilitySet(t, dsCtx, handle.populated, handle.ds)
	// Run a lookup subjects for each resource type and ensure that the returned subjects are those
	// that have access to the resource.
	testQueryPlanForEachResourceType(t, handle.populated, "validate_lookup_subjects",
		func(t *testing.T, resourceRelation tuple.RelationReference) {
			t.Parallel()
			for _, resource := range accessibilitySet.AllResourcesNoWildcards() {
				// Only test resources that match the current resource type
				if resource.ObjectType != resourceRelation.ObjectType || resource.Relation != resourceRelation.Relation {
					continue
				}
				t.Run(tuple.StringONR(resource), func(t *testing.T) {
					accessibleSubjects := accessibilitySet.LookupAccessibleSubjects(resource)
					queryCtx := handle.buildContext(t)
					it, err := query.BuildIteratorFromSchema(handle.schema, resourceRelation.ObjectType, resourceRelation.Relation)
					require.NoError(t, err)

					// Perform a lookup call and ensure it returns the at least the same set of subject IDs.
					// Loop until all subjects have been found or we've hit max iterations.
					resolvedSubjects := make(map[string]bool)
					resourceObj := query.Object{
						ObjectType: resource.ObjectType,
						ObjectID:   resource.ObjectID,
					}
					paths, err := queryCtx.IterSubjects(it, resourceObj, query.NoObjectFilter())
					require.NoError(t, err)

					for path, err := range paths {
						require.NoError(t, err)
						subjectKey := tuple.StringONR(path.Subject)
						resolvedSubjects[subjectKey] = true
					}

					// Print trace if test fails
					if queryCtx.TraceLogger != nil {
						defer func() {
							if t.Failed() {
								t.Logf("Trace for %s:\n%s", resource, queryCtx.TraceLogger.DumpTrace())
								// Also print the tree structure for debugging
								if it != nil {
									t.Logf("Tree structure:\n%s", it.Explain().IndentString(0))
								}
							}
						}()
					}

					require.ElementsMatch(t,
						slices.Collect(maps.Keys(accessibleSubjects)),
						slices.Collect(maps.Keys(resolvedSubjects)),
						"expected accessibleSubjects in list A don't match actual resolvedSubjects in list B",
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
		for _, relation := range resourceType.Relation {
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

// TestAccessibilitySetMethods tests the various methods of the AccessibilitySet
// to ensure they work correctly and provide code coverage.
//
// NOTE: This test exists to provide coverage for AccessibilitySet methods
// that are otherwise only exercised when TEST_QUERY_PLAN_RESOURCES or TEST_QUERY_PLAN_SUBJECTS
// environment flags are set. This test should be removed when those flags are removed and
// the corresponding tests run unconditionally.
func TestAccessibilitySetMethods(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, testTimedelta, memdb.DisableGC)
	require.NoError(err)

	// Use a simple test config
	testConfigPath := filepath.Join("testconfigs", "document.yaml")
	populated, _, err := validationfile.PopulateFromFiles(t.Context(), datalayer.NewDataLayer(ds), caveattypes.Default.TypeSet, []string{testConfigPath})
	require.NoError(err)

	dsCtx := datalayer.ContextWithHandle(t.Context())
	require.NoError(datalayer.SetInContext(dsCtx, datalayer.NewDataLayer(ds)))

	// Build the accessibility set
	accessibilitySet := consistencytestutil.BuildAccessibilitySet(t, dsCtx, populated, ds)
	require.NotNil(accessibilitySet)

	// Test SubjectTypes - should return all defined subject types
	subjectTypes := accessibilitySet.SubjectTypes()
	require.NotEmpty(subjectTypes)

	// Test AllSubjectsNoWildcards - should return all subjects without wildcards
	subjects := accessibilitySet.AllSubjectsNoWildcards()
	require.NotEmpty(subjects)
	for _, subject := range subjects {
		require.NotEqual(tuple.PublicWildcard, subject.ObjectID)
	}

	// Test AllResourcesNoWildcards - should return all resources
	resources := accessibilitySet.AllResourcesNoWildcards()
	require.NotEmpty(resources)

	// Test UncomputedPermissionshipFor and AccessibiliyAndPermissionshipFor
	// Pick the first resource and subject to test with
	if len(resources) > 0 && len(subjects) > 0 {
		resource := resources[0]
		subject := subjects[0]

		// Test UncomputedPermissionshipFor
		uncomputed, ok := accessibilitySet.UncomputedPermissionshipFor(resource, subject)
		require.True(ok)
		require.NotEqual(0, uncomputed)

		// Test AccessibiliyAndPermissionshipFor
		accessibility, permissionship, ok := accessibilitySet.AccessibilityAndPermissionshipFor(resource, subject)
		require.True(ok)
		require.NotEqual(0, accessibility)
		require.NotEqual(0, permissionship)

		// Test DirectlyAccessibleDefinedSubjects
		directSubjects := accessibilitySet.DirectlyAccessibleDefinedSubjects(resource)
		require.NotNil(directSubjects)

		// Test DirectlyAccessibleDefinedSubjectsOfType
		if len(subjectTypes) > 0 {
			subjectType := subjectTypes[0]
			typedSubjects := accessibilitySet.DirectlyAccessibleDefinedSubjectsOfType(resource, subjectType)
			require.NotNil(typedSubjects)
		}

		// Test LookupAccessibleResources
		resourceType := tuple.RelationReference{
			ObjectType: resource.ObjectType,
			Relation:   resource.Relation,
		}
		accessibleResources := accessibilitySet.LookupAccessibleResources(resourceType, subject)
		require.NotNil(accessibleResources)

		// Test LookupAccessibleSubjects
		accessibleSubjects := accessibilitySet.LookupAccessibleSubjects(resource)
		require.NotNil(accessibleSubjects)
	}
}
