//go:build !skipintegrationtests
// +build !skipintegrationtests

package integrationtesting_test

import (
	"context"
	"fmt"
	"path"
	"sort"
	"testing"
	"time"

	"github.com/jzelinskie/stringz"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/types/known/structpb"
	yamlv2 "gopkg.in/yaml.v2"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/developmentmembership"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/services/integrationtesting/consistencytestutil"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/development"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

const testTimedelta = 1 * time.Second

// TestConsistency is a system-wide consistency test suite that reads in various
// validation files in the testconfigs directory, and executes a full set of APIs
// against the data within, ensuring that all results of the various APIs are
// consistent with one another.
//
// This test suite acts as essentially a full integration test for the API,
// dispatching, caching, computation and datastore layers. It should reflect
// both real-world schemas, as well as the full set of hand-constructed corner
// cases so that the system can be fully exercised.
func TestConsistency(t *testing.T) {
	t.Parallel()

	// List all the defined consistency test files.
	consistencyTestFiles, err := consistencytestutil.ListTestConfigs()
	require.NoError(t, err)

	for _, filePath := range consistencyTestFiles {
		filePath := filePath

		t.Run(path.Base(filePath), func(t *testing.T) {
			t.Parallel()
			for _, dispatcherKind := range []string{"local", "caching"} {
				dispatcherKind := dispatcherKind

				t.Run(dispatcherKind, func(t *testing.T) {
					for _, chunkSize := range []uint16{5, 10} {
						t.Run(fmt.Sprintf("chunk-size-%d", chunkSize), func(t *testing.T) {
							t.Parallel()
							runConsistencyTestSuiteForFile(t, filePath, dispatcherKind == "caching", chunkSize)
						})
					}
				})
			}
		})
	}
}

func runConsistencyTestSuiteForFile(t *testing.T, filePath string, useCachingDispatcher bool, chunkSize uint16) {
	options := []server.ConfigOption{server.WithDispatchChunkSize(chunkSize)}

	cad := consistencytestutil.LoadDataAndCreateClusterForTesting(t, filePath, testTimedelta, options...)

	// Validate the type system for each namespace.
	headRevision, err := cad.DataStore.HeadRevision(cad.Ctx)
	require.NoError(t, err)

	ts := schema.NewTypeSystem(schema.ResolverForDatastoreReader(cad.DataStore.SnapshotReader(headRevision)))

	for _, nsDef := range cad.Populated.NamespaceDefinitions {
		_, err := ts.GetValidatedDefinition(cad.Ctx, nsDef.Name)
		require.NoError(t, err)
	}

	// Run consistency tests.
	testers := consistencytestutil.ServiceTesters(cad.Conn)
	for _, tester := range testers {
		tester := tester

		t.Run(tester.Name(), func(t *testing.T) {
			runConsistencyTestsWithServiceTester(t, cad, tester, headRevision, useCachingDispatcher)
		})
	}
}

type validationContext struct {
	clusterAndData   consistencytestutil.ConsistencyClusterAndData
	accessibilitySet *consistencytestutil.AccessibilitySet
	serviceTester    consistencytestutil.ServiceTester
	revision         datastore.Revision
	dispatcher       dispatch.Dispatcher
}

func runConsistencyTestsWithServiceTester(
	t *testing.T,
	cad consistencytestutil.ConsistencyClusterAndData,
	tester consistencytestutil.ServiceTester,
	revision datastore.Revision,
	useCachingDispatcher bool,
) {
	// Build an accessibility set.
	accessibilitySet := consistencytestutil.BuildAccessibilitySet(t, cad)

	dispatcher := consistencytestutil.CreateDispatcherForTesting(t, useCachingDispatcher)

	vctx := validationContext{
		clusterAndData:   cad,
		accessibilitySet: accessibilitySet,
		serviceTester:    tester,
		revision:         revision,
		dispatcher:       dispatcher,
	}

	// Call a write on each relationship to make sure it type checks.
	ensureRelationshipWrites(t, vctx)

	// Call a read on each relationship resource type and ensure it finds all expected relationships.
	validateRelationshipReads(t, vctx)

	// Run the assertions defined in the file.
	runAssertions(t, vctx)

	// Run basic expansion on each relation and ensure no errors are raised.
	ensureNoExpansionErrors(t, vctx)

	// Run a fully recursive expand on each relation and ensure all terminal subjects are reached.
	validateExpansionSubjects(t, vctx)

	// For each relation in each namespace, for each subject, collect the resources accessible
	// to that subject and then verify the lookup resources returns the same set of subjects.
	validateLookupResources(t, vctx)

	// For each object accessible, validate that the subjects that can access it are found.
	validateLookupSubjects(t, vctx)

	// Run the development system over the full set of context and ensure they also return the expected information.
	validateDevelopment(t, vctx)
}

// testForEachRelationship runs a subtest for each relationship defined.
func testForEachRelationship(
	t *testing.T,
	vctx validationContext,
	prefix string,
	handler func(t *testing.T, relationship tuple.Relationship),
) {
	t.Helper()

	for _, relationship := range vctx.clusterAndData.Populated.Relationships {
		relationship := relationship
		t.Run(fmt.Sprintf("%s_%s", prefix, tuple.MustString(relationship)),
			func(t *testing.T) {
				handler(t, relationship)
			})
	}
}

// testForEachResource runs a subtest for each possible resource+relation in the schema.
func testForEachResource(
	t *testing.T,
	vctx validationContext,
	prefix string,
	handler func(t *testing.T, resource tuple.ObjectAndRelation),
) {
	t.Helper()

	for _, resourceType := range vctx.clusterAndData.Populated.NamespaceDefinitions {
		resources, ok := vctx.accessibilitySet.ResourcesByNamespace.Get(resourceType.Name)
		if !ok {
			continue
		}

		resourceType := resourceType
		for _, relation := range resourceType.Relation {
			relation := relation
			for _, resource := range resources {
				resource := resource
				t.Run(fmt.Sprintf("%s_%s_%s_%s", prefix, resourceType.Name, resource.ObjectID, relation.Name),
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

// testForEachResourceType runs a subtest for each possible resource type+relation in the schema.
func testForEachResourceType(
	t *testing.T,
	vctx validationContext,
	prefix string,
	handler func(t *testing.T, resourceType tuple.RelationReference),
) {
	for _, resourceType := range vctx.clusterAndData.Populated.NamespaceDefinitions {
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

// ensureRelationshipWrites ensures that all relationships can be written via the API.
func ensureRelationshipWrites(t *testing.T, vctx validationContext) {
	for _, nsDef := range vctx.clusterAndData.Populated.NamespaceDefinitions {
		relationships, ok := vctx.accessibilitySet.RelationshipsByResourceNamespace.Get(nsDef.Name)
		if !ok {
			continue
		}

		for _, relationship := range relationships {
			err := vctx.serviceTester.Write(context.Background(), relationship)
			require.NoError(t, err, "failed to write %s", tuple.MustString(relationship))
		}
	}
}

// validateRelationshipReads ensures that all defined relationships are returned by the Read API.
func validateRelationshipReads(t *testing.T, vctx validationContext) {
	testForEachRelationship(t, vctx, "read", func(t *testing.T, relationship tuple.Relationship) {
		foundRelationships, err := vctx.serviceTester.Read(context.Background(),
			relationship.Resource.ObjectType,
			vctx.revision,
		)
		require.NoError(t, err)

		foundRelationshipsSet := mapz.NewSet[string]()
		for _, rel := range foundRelationships {
			foundRelationshipsSet.Insert(tuple.MustString(rel))
		}

		if relationship.OptionalExpiration != nil && relationship.OptionalExpiration.Before(time.Now()) {
			require.False(t, foundRelationshipsSet.Has(tuple.MustString(relationship)), "found unexpected expired relationship %s in read results: %s", tuple.MustString(relationship), foundRelationshipsSet.AsSlice())
		} else {
			require.True(t, foundRelationshipsSet.Has(tuple.MustString(relationship)), "missing expected relationship %s in read results: %s", tuple.MustString(relationship), foundRelationshipsSet.AsSlice())
		}
	})
}

// ensureNoExpansionErrors runs basic expansion on each relation and ensures no errors are raised.
func ensureNoExpansionErrors(t *testing.T, vctx validationContext) {
	testForEachResource(t, vctx, "run_expand",
		func(t *testing.T, resource tuple.ObjectAndRelation) {
			_, err := vctx.serviceTester.Expand(context.Background(),
				resource,
				vctx.revision,
			)
			require.NoError(t, err)
		})
}

// validateExpansionSubjects runs a fully recursive expand on each relation and ensures that all expected terminal subjects are reached.
func validateExpansionSubjects(t *testing.T, vctx validationContext) {
	testForEachResource(t, vctx, "validate_expand",
		func(t *testing.T, resource tuple.ObjectAndRelation) {
			// Run a *recursive* expansion to collect all the reachable subjects.
			resp, err := vctx.dispatcher.DispatchExpand(
				vctx.clusterAndData.Ctx,
				&dispatchv1.DispatchExpandRequest{
					ResourceAndRelation: resource.ToCoreONR(),
					Metadata: &dispatchv1.ResolverMeta{
						AtRevision:     vctx.revision.String(),
						DepthRemaining: 100,
						TraversalBloom: dispatchv1.MustNewTraversalBloomFilter(100),
					},
					ExpansionMode: dispatchv1.DispatchExpandRequest_RECURSIVE,
				})
			require.NoError(t, err)

			// Build an accessible subject set from the expansion tree.
			subjectsFoundSet, err := developmentmembership.AccessibleExpansionSubjects(resp.TreeNode)
			require.NoError(t, err)

			// Ensure all non-wildcard terminal subjects that were found in the expansion are accessible.
			for _, foundSubject := range subjectsFoundSet.ToSlice() {
				if foundSubject.GetSubjectId() != tuple.PublicWildcard {
					accessiblity, permissionship, ok := vctx.accessibilitySet.AccessibiliyAndPermissionshipFor(resource, foundSubject.Subject())
					require.True(t, ok, "missing accessibility for resource %s and subject %s", tuple.StringONR(resource), tuple.StringONR(foundSubject.Subject()))

					// NOTE: an expanded subject must either be accessible directly (e.g. not via a wildcard)
					// OR must be removed due to a static caveat context removing it from the set.
					require.True(t,
						accessiblity == consistencytestutil.AccessibleDirectly ||
							accessiblity == consistencytestutil.NotAccessibleDueToPrespecifiedCaveat,
						"mismatch between expand and accessibility for resource %s and subject %s. accessibility: %v, permissionship: %v",
						tuple.StringONR(resource),
						tuple.StringONR(foundSubject.Subject()),
						accessiblity,
						permissionship,
					)
				}
			}

			// Ensure all terminal subjects are found in the expansion.
			for _, expectedSubject := range vctx.accessibilitySet.DirectlyAccessibleDefinedSubjects(resource) {
				found := subjectsFoundSet.Contains(expectedSubject)
				require.True(t, found, "missing expected subject %s in expand for resource %s", tuple.StringONR(expectedSubject), tuple.StringONR(resource))
			}
		})
}

func requireSameSets(t *testing.T, expected []string, found []string) {
	expectedSet := mapz.NewSet(expected...)
	foundSet := mapz.NewSet(found...)

	orderedExpected := expectedSet.AsSlice()
	orderedFound := foundSet.AsSlice()

	sort.Strings(orderedExpected)
	sort.Strings(orderedFound)

	require.Equal(t, orderedExpected, orderedFound)
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

// validateLookupResources ensures that a lookup resources call returns the expected objects and
// only those expected.
func validateLookupResources(t *testing.T, vctx validationContext) {
	// Run a lookup resources for each resource type and ensure that the returned objects are those
	// that are accessible to the subject.
	testForEachResourceType(t, vctx, "validate_lookup_resources",
		func(t *testing.T, resourceRelation tuple.RelationReference) {
			for _, subject := range vctx.accessibilitySet.AllSubjectsNoWildcards() {
				subject := subject
				t.Run(tuple.StringONR(subject), func(t *testing.T) {
					for _, pageSize := range []uint32{0, 2} {
						pageSize := pageSize
						t.Run(fmt.Sprintf("pagesize-%d", pageSize), func(t *testing.T) {
							accessibleResources := vctx.accessibilitySet.LookupAccessibleResources(resourceRelation, subject)

							// Perform a lookup call and ensure it returns the at least the same set of object IDs.
							// Loop until all resources have been found or we've hit max iterations.
							var currentCursor *v1.Cursor
							resolvedResources := map[string]*v1.LookupResourcesResponse{}
							for i := 0; i < 100; i++ {
								foundResources, lastCursor, err := vctx.serviceTester.LookupResources(context.Background(), resourceRelation, subject, vctx.revision, currentCursor, pageSize, nil)
								require.NoError(t, err)

								if pageSize > 0 {
									require.LessOrEqual(t, len(foundResources), int(pageSize))
								}

								currentCursor = lastCursor

								for _, resource := range foundResources {
									resolvedResources[resource.ResourceObjectId] = resource
								}

								if pageSize == 0 || len(foundResources) < int(pageSize) {
									break
								}
							}

							requireSameSets(t, maps.Keys(accessibleResources), maps.Keys(resolvedResources))

							// Ensure that every returned concrete object Checks directly.
							checkBulkItems := make([]*v1.CheckBulkPermissionsRequestItem, 0, len(resolvedResources))
							expectedBulkPermissions := map[string]v1.CheckPermissionResponse_Permissionship{}

							for _, resolvedResource := range resolvedResources {
								permissionship, err := vctx.serviceTester.Check(context.Background(),
									tuple.ObjectAndRelation{
										ObjectType: resourceRelation.ObjectType,
										ObjectID:   resolvedResource.ResourceObjectId,
										Relation:   resourceRelation.Relation,
									},
									subject,
									vctx.revision,
									nil,
								)

								expectedPermissionship := v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION
								if resolvedResource.Permissionship == v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION {
									expectedPermissionship = v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION
								}

								expectedBulkPermissions[resolvedResource.ResourceObjectId] = expectedPermissionship

								require.NoError(t, err)
								require.Equal(t,
									expectedPermissionship,
									permissionship,
									"Found Check failure for relation %s:%s#%s and subject %s in lookup resources; expected %v, found %v",
									resourceRelation.ObjectType,
									resolvedResource.ResourceObjectId,
									resourceRelation.Relation,
									tuple.StringONR(subject),
									expectedPermissionship,
									permissionship,
								)

								checkBulkItems = append(checkBulkItems, &v1.CheckBulkPermissionsRequestItem{
									Resource: &v1.ObjectReference{
										ObjectType: resourceRelation.ObjectType,
										ObjectId:   resolvedResource.ResourceObjectId,
									},
									Permission: resourceRelation.Relation,
									Subject: &v1.SubjectReference{
										Object: &v1.ObjectReference{
											ObjectType: subject.ObjectType,
											ObjectId:   subject.ObjectID,
										},
										OptionalRelation: stringz.Default(subject.Relation, "", tuple.Ellipsis),
									},
								})
							}

							// Ensure they are all found via bulk check as well.
							results, err := vctx.serviceTester.CheckBulk(context.Background(),
								checkBulkItems,
								vctx.revision,
							)
							require.NoError(t, err)
							for _, result := range results {
								require.Equal(t, expectedBulkPermissions[result.Request.Resource.ObjectId], result.GetItem().Permissionship)
							}
						})
					}
				})
			}
		})
}

// validateLookupSubjects validates that the subjects that can access it are those expected.
func validateLookupSubjects(t *testing.T, vctx validationContext) {
	testForEachResource(t, vctx, "validate_lookup_subjects",
		func(t *testing.T, resource tuple.ObjectAndRelation) {
			for _, subjectType := range vctx.accessibilitySet.SubjectTypes() {
				subjectType := subjectType
				t.Run(fmt.Sprintf("%s#%s", subjectType.ObjectType, subjectType.Relation),
					func(t *testing.T) {
						resolvedSubjects, err := vctx.serviceTester.LookupSubjects(context.Background(), resource, subjectType, vctx.revision, nil)
						require.NoError(t, err)

						// Ensure the subjects found include those defined as expected. Since the
						// accessibility set does not include "inferred" subjects (e.g. those with
						// permissions as their subject relation, or wildcards), this should be a
						// subset.
						expectedDefinedSubjects := vctx.accessibilitySet.DirectlyAccessibleDefinedSubjectsOfType(resource, subjectType)
						requireSubsetOf(t, maps.Keys(resolvedSubjects), maps.Keys(expectedDefinedSubjects))

						// Ensure all subjects in true and caveated assertions for the subject type are found
						// in the LookupSubject result, except those added via wildcard.
						for _, parsedFile := range vctx.clusterAndData.Populated.ParsedFiles {
							for _, entry := range []struct {
								assertions         []blocks.Assertion
								requiresPermission bool
							}{
								{
									assertions:         parsedFile.Assertions.AssertTrue,
									requiresPermission: true,
								},
								{
									assertions:         parsedFile.Assertions.AssertCaveated,
									requiresPermission: false,
								},
							} {
								for _, assertion := range entry.assertions {
									assertionRel := assertion.Relationship
									if !tuple.ONREqual(assertionRel.Resource, resource) {
										continue
									}

									if assertionRel.Subject.ObjectType != subjectType.ObjectType ||
										assertionRel.Subject.Relation != subjectType.Relation {
										continue
									}

									// For subjects found solely via wildcard, check that a wildcard instead exists in
									// the result and that the subject is not excluded.
									accessibility, _, ok := vctx.accessibilitySet.AccessibiliyAndPermissionshipFor(resource, assertionRel.Subject)
									if !ok || accessibility == consistencytestutil.AccessibleViaWildcardOnly {
										resolvedSubjectsToCheck := resolvedSubjects

										// If the assertion has caveat context, rerun LookupSubjects with the context to ensure the returned subject
										// matches the context given.
										if len(assertion.CaveatContext) > 0 {
											resolvedSubjectsWithContext, err := vctx.serviceTester.LookupSubjects(context.Background(), resource, subjectType, vctx.revision, assertion.CaveatContext)
											require.NoError(t, err)

											resolvedSubjectsToCheck = resolvedSubjectsWithContext
										}

										resolvedSubject, ok := resolvedSubjectsToCheck[tuple.PublicWildcard]
										require.True(t, ok, "expected wildcard in lookupsubjects response for assertion `%s`", assertion.RelationshipWithContextString)

										if entry.requiresPermission {
											require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION, resolvedSubject.Subject.Permissionship)
										}

										// Ensure that the subject is not excluded. If a caveated assertion, then the exclusion
										// can be caveated.
										for _, excludedSubject := range resolvedSubject.ExcludedSubjects {
											if entry.requiresPermission {
												require.NotEqual(t, excludedSubject.SubjectObjectId, assertionRel.Subject.ObjectID, "wildcard excludes the asserted subject ID: %s", assertionRel.Subject.ObjectID)
											} else if excludedSubject.SubjectObjectId == assertionRel.Subject.ObjectID {
												require.NotEqual(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION, excludedSubject.Permissionship, "wildcard concretely excludes the asserted subject ID: %s", assertionRel.Subject.ObjectID)
											}
										}
										continue
									}

									_, ok = resolvedSubjects[assertionRel.Subject.ObjectID]
									require.True(t, ok, "missing expected subject %s from assertion %s", assertionRel.Subject.ObjectID, assertion.RelationshipWithContextString)
								}
							}
						}

						// Ensure that all excluded subjects from wildcards do not have access.
						for _, resolvedSubject := range resolvedSubjects {
							if resolvedSubject.Subject.SubjectObjectId != tuple.PublicWildcard {
								continue
							}

							for _, excludedSubject := range resolvedSubject.ExcludedSubjects {
								permissionship, err := vctx.serviceTester.Check(context.Background(),
									resource,
									tuple.ObjectAndRelation{
										ObjectType: subjectType.ObjectType,
										ObjectID:   excludedSubject.SubjectObjectId,
										Relation:   subjectType.Relation,
									},
									vctx.revision,
									nil,
								)
								require.NoError(t, err)

								expectedPermissionship := v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION
								if resolvedSubject.Subject.Permissionship == v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION {
									expectedPermissionship = v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION
								}
								if excludedSubject.Permissionship == v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION {
									expectedPermissionship = v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION
								}

								require.Equal(t,
									expectedPermissionship,
									permissionship,
									"Found Check failure for resource %s and excluded subject %s in lookup subjects",
									tuple.StringONR(resource),
									excludedSubject.SubjectObjectId,
								)
							}
						}

						// Ensure that every returned defined, non-wildcard subject found checks as expected.
						for _, resolvedSubject := range resolvedSubjects {
							if resolvedSubject.Subject.SubjectObjectId == tuple.PublicWildcard {
								continue
							}

							subject := tuple.ObjectAndRelation{
								ObjectType: subjectType.ObjectType,
								ObjectID:   resolvedSubject.Subject.SubjectObjectId,
								Relation:   subjectType.Relation,
							}

							permissionship, err := vctx.serviceTester.Check(context.Background(),
								resource,
								subject,
								vctx.revision,
								nil,
							)
							require.NoError(t, err)

							expectedPermissionship := v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION
							if resolvedSubject.Subject.Permissionship == v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION {
								expectedPermissionship = v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION
							}

							require.Equal(t,
								expectedPermissionship,
								permissionship,
								"Found Check failure for resource %s and subject %s in lookup subjects",
								tuple.StringONR(resource),
								tuple.StringONR(subject),
							)
						}
					})
			}
		})
}

// runAssertions runs all assertions defined in the validation files and ensures they
// return the expected results.
func runAssertions(t *testing.T, vctx validationContext) {
	t.Run("assertions", func(t *testing.T) {
		for _, parsedFile := range vctx.clusterAndData.Populated.ParsedFiles {
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
				entry := entry
				t.Run(entry.name, func(t *testing.T) {
					bulkCheckItems := make([]*v1.BulkCheckPermissionRequestItem, 0, len(entry.assertions))

					for _, assertion := range entry.assertions {
						var caveatContext *structpb.Struct
						if assertion.CaveatContext != nil {
							built, err := structpb.NewStruct(assertion.CaveatContext)
							require.NoError(t, err)
							caveatContext = built
						}

						rel := tuple.ToV1Relationship(assertion.Relationship)

						bulkCheckItems = append(bulkCheckItems, &v1.BulkCheckPermissionRequestItem{
							Resource:   rel.Resource,
							Permission: rel.Relation,
							Subject:    rel.Subject,
							Context:    caveatContext,
						})

						// Run each individual assertion.
						assertion := assertion
						t.Run(assertion.RelationshipWithContextString, func(t *testing.T) {
							rel := assertion.Relationship
							permissionship, err := vctx.serviceTester.Check(context.Background(), rel.Resource, rel.Subject, vctx.revision, assertion.CaveatContext)
							require.NoError(t, err)
							require.Equal(t, entry.expectedPermissionship, permissionship, "Assertion `%s` returned %s; expected %s", tuple.MustString(rel), permissionship, entry.expectedPermissionship)

							// Ensure the assertion passes LookupResources with context, directly.
							resolvedDirectResources, _, err := vctx.serviceTester.LookupResources(context.Background(), rel.Resource.RelationReference(), rel.Subject, vctx.revision, nil, 0, assertion.CaveatContext)
							require.NoError(t, err)

							resolvedDirectResourcesMap := map[string]*v1.LookupResourcesResponse{}
							for _, resource := range resolvedDirectResources {
								resolvedDirectResourcesMap[resource.ResourceObjectId] = resource
							}

							// Ensure the assertion passes LookupResources without context, indirectly.
							resolvedIndirectResources, _, err := vctx.serviceTester.LookupResources(context.Background(), rel.Resource.RelationReference(), rel.Subject, vctx.revision, nil, 0, nil)
							require.NoError(t, err)

							resolvedIndirectResourcesMap := map[string]*v1.LookupResourcesResponse{}
							for _, resource := range resolvedIndirectResources {
								resolvedIndirectResourcesMap[resource.ResourceObjectId] = resource
							}

							// Check the assertion was returned for a direct (with context) lookup.
							resolvedDirectResourceIds := maps.Keys(resolvedDirectResourcesMap)
							switch permissionship {
							case v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION:
								require.NotContains(t, resolvedDirectResourceIds, rel.Resource.ObjectID, "Found unexpected object %s in direct lookup for assertion %s", rel.Resource, rel)

							case v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION:
								require.Contains(t, resolvedDirectResourceIds, rel.Resource.ObjectID, "Missing object %s in lookup for assertion %s", rel.Resource, rel)
								require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION, resolvedDirectResourcesMap[rel.Resource.ObjectID].Permissionship)

							case v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION:
								require.Contains(t, resolvedDirectResourceIds, rel.Resource.ObjectID, "Missing object %s in lookup for assertion %s", rel.Resource, rel)
								require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION, resolvedDirectResourcesMap[rel.Resource.ObjectID].Permissionship)
							}

							// Check the assertion was returned for an indirect (without context) lookup.
							resolvedIndirectResourceIds := maps.Keys(resolvedIndirectResourcesMap)
							accessibility, _, _ := vctx.accessibilitySet.AccessibiliyAndPermissionshipFor(rel.Resource, rel.Subject)

							switch permissionship {
							case v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION:
								// If the caveat context given is empty, then the lookup result must not exist at all.
								// Otherwise, it *could* be caveated or not exist, depending on the context given.
								if len(assertion.CaveatContext) == 0 {
									require.NotContains(t, resolvedIndirectResourceIds, rel.Resource.ObjectID, "Found unexpected object %s in indirect lookup for assertion %s", rel.Resource, rel)
								} else if accessibility == consistencytestutil.NotAccessible {
									found, ok := resolvedIndirectResourcesMap[rel.Resource.ObjectID]
									require.True(t, !ok || found.Permissionship != v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION) // LookupResources can be caveated, since we didn't rerun LookupResources with the context
								} else if accessibility != consistencytestutil.NotAccessibleDueToPrespecifiedCaveat {
									require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION, resolvedIndirectResourcesMap[rel.Resource.ObjectID].Permissionship)
								}

							case v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION:
								require.Contains(t, resolvedIndirectResourceIds, rel.Resource.ObjectID, "Missing object %s in lookup for assertion %s", rel.Resource, rel)
								// If the caveat context given is empty, then the lookup result must be fully permissioned.
								// Otherwise, it *could* be caveated or fully permissioned, depending on the context given.
								if len(assertion.CaveatContext) == 0 {
									require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION, resolvedIndirectResourcesMap[rel.Resource.ObjectID].Permissionship)
								}

							case v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION:
								require.Contains(t, resolvedIndirectResourceIds, rel.Resource.ObjectID, "Missing object %s in lookup for assertion %s", rel.Resource, rel)
								require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION, resolvedIndirectResourcesMap[rel.Resource.ObjectID].Permissionship)

							default:
								panic("unknown permissionship")
							}
						})

						// Run all assertions under bulk check and ensure they match as well.
						results, err := vctx.serviceTester.BulkCheck(context.Background(), bulkCheckItems, vctx.revision)
						require.NoError(t, err)

						for _, result := range results {
							require.Equal(t, entry.expectedPermissionship, result.GetItem().Permissionship, "Bulk check for assertion request `%s` returned %s; expected %s", result.GetRequest(), result.GetItem().Permissionship, entry.expectedPermissionship)
						}
					}
				})
			}
		}
	})
}

// validateDevelopment runs the development package against the validation context and
// ensures its output matches that expected.
func validateDevelopment(t *testing.T, vctx validationContext) {
	rels := make([]*core.RelationTuple, 0, len(vctx.clusterAndData.Populated.Relationships))
	for _, rel := range vctx.clusterAndData.Populated.Relationships {
		rels = append(rels, rel.ToCoreTuple())
	}

	reqContext := &devinterface.RequestContext{
		Schema:        vctx.clusterAndData.Populated.Schema,
		Relationships: rels,
	}

	devContext, devErr, err := development.NewDevContext(context.Background(), reqContext)
	require.NoError(t, err)
	require.Nil(t, devErr, "dev error: %v", devErr)
	require.NotNil(t, devContext)

	// Validate checks.
	validateDevelopmentChecks(t, devContext, vctx)

	// Validate assertions.
	validateDevelopmentAssertions(t, devContext, vctx)

	// Validate expected relationships.
	validateDevelopmentExpectedRels(t, devContext, vctx)
}

// validateDevelopmentChecks validates that the Check operation in the development package
// returns the expected permissionship.
func validateDevelopmentChecks(t *testing.T, devContext *development.DevContext, vctx validationContext) {
	testForEachResource(t, vctx, "validate_check_watch",
		func(t *testing.T, resource tuple.ObjectAndRelation) {
			for _, subject := range vctx.accessibilitySet.AllSubjectsNoWildcards() {
				subject := subject
				t.Run(tuple.StringONR(subject), func(t *testing.T) {
					require.NotNil(t, devContext)
					cr, err := development.RunCheck(devContext, resource, subject, nil)
					require.NoError(t, err, "Got unexpected error from development check")

					_, permissionship, ok := vctx.accessibilitySet.AccessibiliyAndPermissionshipFor(resource, subject)
					require.True(t, ok)
					require.Equal(t, permissionship, cr.Permissionship,
						"Found unexpected membership difference for %s@%s. Expected %v, Found: %v",
						tuple.StringONR(resource),
						tuple.StringONR(subject),
						permissionship,
						cr.Permissionship)
				})
			}
		})
}

// validateDevelopmentAssertions validates that Assertions in the development package return
// the expected results.
func validateDevelopmentAssertions(t *testing.T, devContext *development.DevContext, vctx validationContext) {
	// Build the assertions YAML.
	var trueAssertions []string
	var caveatedAssertions []string
	var falseAssertions []string

	for relString, permissionship := range vctx.accessibilitySet.PermissionshipByRelationship {
		switch permissionship {
		case dispatchv1.ResourceCheckResult_MEMBER:
			trueAssertions = append(trueAssertions, relString)
		case dispatchv1.ResourceCheckResult_CAVEATED_MEMBER:
			caveatedAssertions = append(caveatedAssertions, relString)
		case dispatchv1.ResourceCheckResult_NOT_MEMBER:
			falseAssertions = append(falseAssertions, relString)
		default:
			require.Fail(t, "unknown permissionship")
		}
	}

	assertionsMap := map[string]interface{}{
		"assertTrue":     trueAssertions,
		"assertCaveated": caveatedAssertions,
		"assertFalse":    falseAssertions,
	}
	assertions, err := yamlv2.Marshal(assertionsMap)
	require.NoError(t, err, "Could not marshal assertions map")

	// Run validation with the assertions and the updated YAML.
	parsedAssertions, devErr := development.ParseAssertionsYAML(string(assertions))
	require.NoError(t, err, "Got unexpected error from assertions")
	require.Nil(t, devErr, "Got unexpected request error from assertions: %v", devErr)

	devErrs, err := development.RunAllAssertions(devContext, parsedAssertions)
	require.NoError(t, err, "Got unexpected error from assertions")
	require.Equal(t, 0, len(devErrs), "Got unexpected errors from validation: %v", devErrs)
}

// validateDevelopmentExpectedRels validates that the generated expected relationships matches
// that expected.
func validateDevelopmentExpectedRels(t *testing.T, devContext *development.DevContext, vctx validationContext) {
	// Build the Expected Relations (inputs only).
	expectedMap := map[string]interface{}{}
	for relString, permissionship := range vctx.accessibilitySet.PermissionshipByRelationship {
		if permissionship == dispatchv1.ResourceCheckResult_NOT_MEMBER {
			continue
		}

		relationship := tuple.MustParse(relString)
		expectedMap[tuple.StringONR(relationship.Resource)] = []string{}
	}

	expectedRelations, err := yamlv2.Marshal(expectedMap)
	require.NoError(t, err, "Could not marshal expected relations map")

	expectedRelationsMap, devErr := development.ParseExpectedRelationsYAML(string(expectedRelations))
	require.Nil(t, devErr)

	// NOTE: We are using this to generate, so we ignore any errors.
	membershipSet, _, err := development.RunValidation(devContext, expectedRelationsMap)
	require.NoError(t, err, "Got unexpected error from validation")

	// Parse the full validation YAML, and ensure every referenced subject is, in fact, allowed.
	updatedValidationYaml, gerr := development.GenerateValidation(membershipSet)
	require.NoError(t, gerr)

	validationMap, err := validationfile.ParseExpectedRelationsBlock([]byte(updatedValidationYaml))
	require.NoError(t, err)

	for resourceKey, expectedSubjects := range validationMap.ValidationMap {
		for _, expectedSubject := range expectedSubjects {
			resourceAndRelation := resourceKey.ObjectAndRelation
			subjectWithExceptions := expectedSubject.SubjectWithExceptions
			require.NotNil(t, subjectWithExceptions, "Found expected relation without subject: %s", expectedSubject.ValidationString)

			// For non-wildcard subjects, ensure they are accessible.
			if subjectWithExceptions.Subject.Subject.ObjectID != tuple.PublicWildcard {
				accessibility, permissionship, ok := vctx.accessibilitySet.AccessibiliyAndPermissionshipFor(resourceAndRelation, subjectWithExceptions.Subject.Subject)
				require.True(t, ok, "missing expected subject %s in accessibility set", tuple.StringONR(subjectWithExceptions.Subject.Subject))

				switch permissionship {
				case dispatchv1.ResourceCheckResult_MEMBER:
					// May be caveated or uncaveated, so check the uncomputed permissionship.
					uncomputed, ok := vctx.accessibilitySet.UncomputedPermissionshipFor(resourceAndRelation, subjectWithExceptions.Subject.Subject)
					require.True(t, ok, "missing expected subject in accessibility set")
					require.True(t, subjectWithExceptions.Subject.IsCaveated == (uncomputed == dispatchv1.ResourceCheckResult_CAVEATED_MEMBER), "found mismatch in uncomputed permissionship")

				case dispatchv1.ResourceCheckResult_CAVEATED_MEMBER:
					require.True(t, subjectWithExceptions.Subject.IsCaveated, "found uncaveated expected subject for caveated subject")

				case dispatchv1.ResourceCheckResult_NOT_MEMBER:
					// May be caveated or uncaveated, so check the accessibility.
					if accessibility == consistencytestutil.NotAccessibleDueToPrespecifiedCaveat {
						require.True(t, subjectWithExceptions.Subject.IsCaveated, "found uncaveated expected subject for caveated subject")
					} else {
						require.Failf(t, "found unexpected subject", "%s", tuple.StringONR(subjectWithExceptions.Subject.Subject))
					}

				default:
					require.Fail(t, "expected valid permissionship")
				}
			}
		}
	}
}
