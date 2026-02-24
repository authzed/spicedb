//go:build integration

package queryconsistency_test

import (
	"fmt"
	"maps"
	"path"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/services/integrationtesting/consistencytestutil"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

// TestQueryPlanConsistencyGRPC verifies that the experimental query plan engine
// produces consistent results for LookupResources and LookupSubjects when those
// operations are routed through the permissionServer gRPC handlers
// (lookupResourcesWithQueryPlan / lookupSubjectsWithQueryPlan).
//
// It iterates the same validation files as TestConsistency, spins up a server
// with both "lr" and "ls" query-plan flags enabled, and then verifies that the
// set of returned resource/subject IDs matches the expected accessibility set.
//
// Note: the query plan handlers do not yet support cursor pagination or wildcard
// exclusions, so this test intentionally uses unpaginated single-shot calls and
// only checks set membership rather than running the full validateLookupResources
// / validateLookupSubjects suite.
func TestQueryPlanConsistencyGRPC(t *testing.T) { // nolint:tparallel
	consistencyTestFiles, err := consistencytestutil.ListTestConfigs()
	require.NoError(t, err)

	for _, filePath := range consistencyTestFiles {
		t.Run(path.Base(filePath), func(t *testing.T) {
			t.Parallel()
			runQueryPlanConsistencyGRPCForFile(t, filePath)
		})
	}
}

// runQueryPlanConsistencyGRPCForFile spins up a full gRPC server with both the
// "lr" and "ls" experimental query plan flags enabled, then validates that
// LookupResources and LookupSubjects return the correct set of IDs for every
// resource-type × subject combination in the validation file.
func runQueryPlanConsistencyGRPCForFile(t *testing.T, filePath string) {
	options := []server.ConfigOption{
		server.WithDispatchChunkSize(10),
		server.WithExperimentalQueryPlan("check"),
		server.WithExperimentalQueryPlan("lr"),
		server.WithExperimentalQueryPlan("ls"),
	}

	cad := consistencytestutil.LoadDataAndCreateClusterForTesting(t, filePath, testTimedelta, options...)

	headRevision, err := cad.DataStore.HeadRevision(cad.Ctx)
	require.NoError(t, err)

	accessibilitySet := consistencytestutil.BuildAccessibilitySet(t, cad.Ctx, cad.Populated, cad.DataStore)

	tester := consistencytestutil.NewServiceTester(cad.Conn)
	t.Run(tester.Name(), func(t *testing.T) {
		validateQueryPlanCheck(t, cad, tester, headRevision.Revision)
		validateQueryPlanLookupResources(t, cad, tester, headRevision.Revision, accessibilitySet)
		validateQueryPlanLookupSubjects(t, cad, tester, headRevision.Revision, accessibilitySet)
	})
}

// validateQueryPlanCheck runs every assertion defined in the validation files
// through the gRPC CheckPermission handler (which is routed to the query plan
// engine via the "check" experimental flag) and verifies the returned
// permissionship matches the asserted value.
func validateQueryPlanCheck(
	t *testing.T,
	cad consistencytestutil.ConsistencyClusterAndData,
	tester consistencytestutil.ServiceTester,
	revision datastore.Revision,
) {
	t.Run("check", func(t *testing.T) {
		for _, parsedFile := range cad.Populated.ParsedFiles {
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
						assertion := assertion
						t.Run(assertion.RelationshipWithContextString, func(t *testing.T) {
							rel := assertion.Relationship
							permissionship, err := tester.Check(t.Context(), rel.Resource, rel.Subject, revision, assertion.CaveatContext)
							require.NoError(t, err)
							require.Equal(t, entry.expectedPermissionship, permissionship,
								"query plan Check assertion `%s` returned %s; expected %s",
								tuple.MustString(rel), permissionship, entry.expectedPermissionship)
						})
					}
				})
			}
		}
	})
}

// validateQueryPlanLookupResources checks that for each resource-type × subject
// pair the query plan handler returns exactly the same resource IDs as the
// accessibility set expects. Pagination is not tested here because the query
// plan handlers do not yet implement cursor-based pagination.
func validateQueryPlanLookupResources(
	t *testing.T,
	cad consistencytestutil.ConsistencyClusterAndData,
	tester consistencytestutil.ServiceTester,
	revision datastore.Revision,
	accessibilitySet *consistencytestutil.AccessibilitySet,
) {
	t.Run("lookup_resources", func(t *testing.T) {
		testForEachResourceTypeInPopulated(t, cad.Populated,
			func(t *testing.T, resourceRelation tuple.RelationReference) {
				for _, subject := range accessibilitySet.AllSubjectsNoWildcards() {
					t.Run(tuple.StringONR(subject), func(t *testing.T) {
						accessibleResources := accessibilitySet.LookupAccessibleResources(resourceRelation, subject)

						// Single unpaginated call (limit=0 means no limit).
						foundResources, _, err := tester.LookupResources(t.Context(), resourceRelation, subject, revision, nil, 0, nil)
						require.NoError(t, err)

						resolvedIDs := make([]string, 0, len(foundResources))
						for _, r := range foundResources {
							resolvedIDs = append(resolvedIDs, r.ResourceObjectId)
						}

						require.ElementsMatch(t,
							slices.Collect(maps.Keys(accessibleResources)),
							resolvedIDs,
							"query plan LookupResources mismatch for %s#%s / subject %s",
							resourceRelation.ObjectType, resourceRelation.Relation, tuple.StringONR(subject),
						)
					})
				}
			})
	})
}

// validateQueryPlanLookupSubjects checks that for each resource × subject-type
// pair the query plan handler returns at least the subjects the accessibility
// set defines as directly accessible. Wildcard exclusion checking is omitted
// because the query plan handler does not yet populate ExcludedSubjects.
func validateQueryPlanLookupSubjects(
	t *testing.T,
	cad consistencytestutil.ConsistencyClusterAndData,
	tester consistencytestutil.ServiceTester,
	revision datastore.Revision,
	accessibilitySet *consistencytestutil.AccessibilitySet,
) {
	t.Run("lookup_subjects", func(t *testing.T) {
		testForEachResourceInPopulated(t, cad.Populated, accessibilitySet,
			func(t *testing.T, resource tuple.ObjectAndRelation) {
				for _, subjectType := range accessibilitySet.SubjectTypes() {
					t.Run(fmt.Sprintf("%s#%s", subjectType.ObjectType, subjectType.Relation),
						func(t *testing.T) {
							resolvedSubjects, err := tester.LookupSubjects(t.Context(), resource, subjectType, revision, nil)
							require.NoError(t, err)

							// The accessibility set only covers directly-accessible defined subjects;
							// it does not include inferred subjects or wildcards, so we check subset.
							expectedDefinedSubjects := accessibilitySet.DirectlyAccessibleDefinedSubjectsOfType(resource, subjectType)
							requireSubsetOf(t,
								slices.Collect(maps.Keys(resolvedSubjects)),
								slices.Collect(maps.Keys(expectedDefinedSubjects)),
							)
						})
				}
			})
	})
}

// testForEachResourceTypeInPopulated runs a subtest for every relation on every
// namespace defined in the populated validation file. Each resource type × relation
// pair gets its own "<namespace>_<relation>" subtest.
func testForEachResourceTypeInPopulated(
	t *testing.T,
	populated *validationfile.PopulatedValidationFile,
	handler func(t *testing.T, resourceRelation tuple.RelationReference),
) {
	t.Helper()
	for _, resourceType := range populated.NamespaceDefinitions {
		for _, relation := range resourceType.Relation {
			t.Run(fmt.Sprintf("%s_%s", resourceType.Name, relation.Name),
				func(t *testing.T) {
					handler(t, tuple.RelationReference{
						ObjectType: resourceType.Name,
						Relation:   relation.Name,
					})
				})
		}
	}
}

// testForEachResourceInPopulated runs a subtest for every resource instance
// present in the accessibility set, filtered by namespace definitions. Each
// resource instance gets its own "<namespace>:<id>#<relation>" subtest.
func testForEachResourceInPopulated(
	t *testing.T,
	populated *validationfile.PopulatedValidationFile,
	accessibilitySet *consistencytestutil.AccessibilitySet,
	handler func(t *testing.T, resource tuple.ObjectAndRelation),
) {
	t.Helper()
	for _, resourceType := range populated.NamespaceDefinitions {
		resources, ok := accessibilitySet.ResourcesByNamespace.Get(resourceType.Name)
		if !ok {
			continue
		}
		for _, relation := range resourceType.Relation {
			for _, resource := range resources {
				t.Run(fmt.Sprintf("%s:%s#%s", resourceType.Name, resource.ObjectID, relation.Name),
					func(t *testing.T) {
						handler(t, tuple.ObjectAndRelation{
							ObjectType: resourceType.Name,
							ObjectID:   resource.ObjectID,
							Relation:   relation.Name,
						})
					})
			}
		}
	}
}

func requireSubsetOf(t *testing.T, found []string, expected []string) {
	if len(expected) == 0 {
		return
	}

	foundSet := mapz.NewSet(found...)
	for _, expectedObjectID := range expected {
		require.True(t, foundSet.Has(expectedObjectID), "missing expected object ID %s", expectedObjectID)
	}
}
