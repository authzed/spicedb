package consistencytestutil

import (
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/jzelinskie/stringz"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/developmentmembership"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

// ValidationContext holds everything needed to run the consistency suite against a
// single populated cluster.
type ValidationContext struct {
	ClusterAndData   ConsistencyClusterAndData
	AccessibilitySet *AccessibilitySet
	ServiceTester    ServiceTester
	Revision         datastore.Revision
	Dispatcher       dispatch.Dispatcher
}

// RunConsistencyTestSuiteForCluster runs the full consistency suite against the cluster
// and data described by vctx.
func RunConsistencyTestSuiteForCluster(t *testing.T, vctx ValidationContext) {
	// Call a write on each relationship to make sure it type checks.
	EnsureRelationshipWrites(t, vctx)

	// Call a read on each relationship resource type and ensure it finds all expected relationships.
	ValidateRelationshipReads(t, vctx)

	// Run the assertions defined in the file.
	RunAssertions(t, vctx)

	// Run basic expansion on each relation and ensure no errors are raised.
	EnsureNoExpansionErrors(t, vctx)

	// Run a fully recursive expand on each relation and ensure all terminal subjects are reached.
	ValidateExpansionSubjects(t, vctx)

	// For each relation in each namespace, for each subject, collect the resources accessible
	// to that subject and then verify the lookup resources returns the same set of subjects.
	ValidateLookupResources(t, vctx)

	// For each object accessible, validate that the subjects that can access it are found.
	ValidateLookupSubjects(t, vctx)

	// Ensure that the set of reachable subject types matches the actual reachable subjects.
	ValidateReachableSubjectTypes(t, vctx)
}

// testForEachRelationship runs a subtest for each relationship defined.
func testForEachRelationship(
	t *testing.T,
	vctx ValidationContext,
	prefix string,
	handler func(t *testing.T, relationship tuple.Relationship),
) {
	t.Helper()

	for _, relationship := range vctx.ClusterAndData.Populated.Relationships {
		t.Run(fmt.Sprintf("%s_%s", prefix, tuple.MustString(relationship)),
			func(t *testing.T) {
				handler(t, relationship)
			})
	}
}

// testForEachResource runs a subtest for each possible resource+relation in the schema.
func testForEachResource(
	t *testing.T,
	vctx ValidationContext,
	prefix string,
	handler func(t *testing.T, resource tuple.ObjectAndRelation),
) {
	t.Helper()

	for _, resourceType := range vctx.ClusterAndData.Populated.NamespaceDefinitions {
		resources, ok := vctx.AccessibilitySet.ResourcesByNamespace.Get(resourceType.Name)
		if !ok {
			continue
		}

		resourceType := resourceType
		for _, relation := range resourceType.Relation {
			for _, resource := range resources {
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
	vctx ValidationContext,
	prefix string,
	handler func(t *testing.T, resourceType tuple.RelationReference),
) {
	for _, resourceType := range vctx.ClusterAndData.Populated.NamespaceDefinitions {
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

// EnsureRelationshipWrites ensures that all relationships can be written via the API.
func EnsureRelationshipWrites(t *testing.T, vctx ValidationContext) {
	for _, nsDef := range vctx.ClusterAndData.Populated.NamespaceDefinitions {
		relationships, ok := vctx.AccessibilitySet.RelationshipsByResourceNamespace.Get(nsDef.Name)
		if !ok {
			continue
		}

		for _, relationship := range relationships {
			err := vctx.ServiceTester.Write(t.Context(), relationship)
			require.NoError(t, err, "failed to write %s", tuple.MustString(relationship))
		}
	}
}

// ValidateRelationshipReads ensures that all defined relationships are returned by the Read API.
func ValidateRelationshipReads(t *testing.T, vctx ValidationContext) {
	testForEachRelationship(t, vctx, "read", func(t *testing.T, relationship tuple.Relationship) {
		foundRelationships, err := vctx.ServiceTester.Read(t.Context(),
			relationship.Resource.ObjectType,
			vctx.Revision,
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

// EnsureNoExpansionErrors runs basic expansion on each relation and ensures no errors are raised.
func EnsureNoExpansionErrors(t *testing.T, vctx ValidationContext) {
	testForEachResource(t, vctx, "run_expand",
		func(t *testing.T, resource tuple.ObjectAndRelation) {
			_, err := vctx.ServiceTester.Expand(t.Context(),
				resource,
				vctx.Revision,
			)
			require.NoError(t, err)
		})
}

// ValidateExpansionSubjects runs a fully recursive expand on each relation and ensures that all expected terminal subjects are reached.
func ValidateExpansionSubjects(t *testing.T, vctx ValidationContext) {
	testForEachResource(t, vctx, "validate_expand",
		func(t *testing.T, resource tuple.ObjectAndRelation) {
			// Run a *recursive* expansion to collect all the reachable subjects.
			// Note: we don't use vctx.ServiceTester.Expand here because we are asking for RECURSIVE mode
			resp, err := vctx.Dispatcher.DispatchExpand(
				vctx.ClusterAndData.Ctx,
				&dispatchv1.DispatchExpandRequest{
					ResourceAndRelation: resource.ToCoreONR(),
					Metadata: &dispatchv1.ResolverMeta{
						AtRevision:     vctx.Revision.String(),
						DepthRemaining: 100,
						TraversalBloom: dispatchv1.MustNewTraversalBloomFilter(100),
						SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
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
					accessiblity, permissionship, ok := vctx.AccessibilitySet.AccessibilityAndPermissionshipFor(resource, foundSubject.Subject())
					require.True(t, ok, "missing accessibility for resource %s and subject %s", tuple.StringONR(resource), tuple.StringONR(foundSubject.Subject()))

					// NOTE: an expanded subject must either be accessible directly (e.g. not via a wildcard)
					// OR must be removed due to a static caveat context removing it from the set.
					require.True(t,
						accessiblity == AccessibleDirectly ||
							accessiblity == NotAccessibleDueToPrespecifiedCaveat,
						"mismatch between expand and accessibility for resource %s and subject %s. accessibility: %v, permissionship: %v",
						tuple.StringONR(resource),
						tuple.StringONR(foundSubject.Subject()),
						accessiblity,
						permissionship,
					)
				}
			}

			// Ensure all terminal subjects are found in the expansion.
			for _, expectedSubject := range vctx.AccessibilitySet.DirectlyAccessibleDefinedSubjects(resource) {
				found := subjectsFoundSet.Contains(expectedSubject)
				require.True(t, found, "missing expected subject %s in expand for resource %s", tuple.StringONR(expectedSubject), tuple.StringONR(resource))
			}
		})
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

// ValidateLookupResources ensures that a lookup resources call returns the expected objects and
// only those expected.
func ValidateLookupResources(t *testing.T, vctx ValidationContext) {
	// Run a lookup resources for each resource type and ensure that the returned objects are those
	// that are accessible to the subject.
	testForEachResourceType(t, vctx, "validate_lookup_resources",
		func(t *testing.T, resourceRelation tuple.RelationReference) {
			for _, subject := range vctx.AccessibilitySet.AllSubjectsNoWildcards() {
				t.Run(tuple.StringONR(subject), func(t *testing.T) {
					for _, pageSize := range []uint32{0, 2} {
						t.Run(fmt.Sprintf("pagesize-%d", pageSize), func(t *testing.T) {
							accessibleResources := vctx.AccessibilitySet.LookupAccessibleResources(resourceRelation, subject)

							// Perform a lookup call and ensure it returns the at least the same set of object IDs.
							// Loop until all resources have been found or we've hit max iterations.
							var currentCursor *v1.Cursor
							resolvedResources := map[string]*v1.LookupResourcesResponse{}
							for range 100 {
								foundResources, lastCursor, err := vctx.ServiceTester.LookupResources(t.Context(), resourceRelation, subject, vctx.Revision, currentCursor, pageSize, nil)
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

							require.ElementsMatch(t,
								slices.Collect(maps.Keys(accessibleResources)),
								slices.Collect(maps.Keys(resolvedResources)),
								"expected accessibleResources in list A don't match actual resolvedResources in list B",
							)

							// Ensure that every returned concrete object Checks directly.
							checkBulkItems := make([]*v1.CheckBulkPermissionsRequestItem, 0, len(resolvedResources))
							expectedBulkPermissions := map[string]v1.CheckPermissionResponse_Permissionship{}

							for _, resolvedResource := range resolvedResources {
								permissionship, err := vctx.ServiceTester.Check(t.Context(),
									tuple.ObjectAndRelation{
										ObjectType: resourceRelation.ObjectType,
										ObjectID:   resolvedResource.ResourceObjectId,
										Relation:   resourceRelation.Relation,
									},
									subject,
									vctx.Revision,
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
									"Found Check failure for relation %s:%s#%s and subject %s in lookup resources; LR permission %v, Check Permission %v",
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
							results, err := vctx.ServiceTester.CheckBulk(t.Context(),
								checkBulkItems,
								vctx.Revision,
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

// ValidateLookupSubjects validates that the subjects that can access it are those expected.
func ValidateLookupSubjects(t *testing.T, vctx ValidationContext) {
	testForEachResource(t, vctx, "validate_lookup_subjects",
		func(t *testing.T, resource tuple.ObjectAndRelation) {
			for _, subjectType := range vctx.AccessibilitySet.SubjectTypes() {
				t.Run(fmt.Sprintf("%s#%s", subjectType.ObjectType, subjectType.Relation),
					func(t *testing.T) {
						resolvedSubjects, err := vctx.ServiceTester.LookupSubjects(t.Context(), resource, subjectType, vctx.Revision, nil)
						require.NoError(t, err)

						// Ensure the subjects found include those defined as expected. Since the
						// accessibility set does not include "inferred" subjects (e.g. those with
						// permissions as their subject relation, or wildcards), this should be a
						// subset.
						expectedDefinedSubjects := vctx.AccessibilitySet.DirectlyAccessibleDefinedSubjectsOfType(resource, subjectType)
						requireSubsetOf(t,
							slices.Collect(maps.Keys(resolvedSubjects)),
							slices.Collect(maps.Keys(expectedDefinedSubjects)),
						)

						// Ensure all subjects in true and caveated assertions for the subject type are found
						// in the LookupSubject result, except those added via wildcard.
						for _, parsedFile := range vctx.ClusterAndData.Populated.ParsedFiles {
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
									accessibility, _, ok := vctx.AccessibilitySet.AccessibilityAndPermissionshipFor(resource, assertionRel.Subject)
									if !ok || accessibility == AccessibleViaWildcardOnly {
										resolvedSubjectsToCheck := resolvedSubjects

										// If the assertion has caveat context, rerun LookupSubjects with the context to ensure the returned subject
										// matches the context given.
										if len(assertion.CaveatContext) > 0 {
											resolvedSubjectsWithContext, err := vctx.ServiceTester.LookupSubjects(t.Context(), resource, subjectType, vctx.Revision, assertion.CaveatContext)
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
								permissionship, err := vctx.ServiceTester.Check(t.Context(),
									resource,
									tuple.ObjectAndRelation{
										ObjectType: subjectType.ObjectType,
										ObjectID:   excludedSubject.SubjectObjectId,
										Relation:   subjectType.Relation,
									},
									vctx.Revision,
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

							permissionship, err := vctx.ServiceTester.Check(t.Context(),
								resource,
								subject,
								vctx.Revision,
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

// RunAssertions runs all assertions defined in the validation files and ensures they
// return the expected results.
func RunAssertions(t *testing.T, vctx ValidationContext) {
	t.Run("assertions", func(t *testing.T) {
		for _, parsedFile := range vctx.ClusterAndData.Populated.ParsedFiles {
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
							permissionship, err := vctx.ServiceTester.Check(t.Context(), rel.Resource, rel.Subject, vctx.Revision, assertion.CaveatContext)
							require.NoError(t, err)
							require.Equal(t, entry.expectedPermissionship, permissionship, "Assertion `%s` returned %s; expected %s", tuple.MustString(rel), permissionship, entry.expectedPermissionship)

							// Ensure the assertion passes LookupResources with context, directly.
							resolvedDirectResources, _, err := vctx.ServiceTester.LookupResources(t.Context(), rel.Resource.RelationReference(), rel.Subject, vctx.Revision, nil, 0, assertion.CaveatContext)
							require.NoError(t, err)

							resolvedDirectResourcesMap := map[string]*v1.LookupResourcesResponse{}
							for _, resource := range resolvedDirectResources {
								resolvedDirectResourcesMap[resource.ResourceObjectId] = resource
							}

							// Ensure the assertion passes LookupResources without context, indirectly.
							resolvedIndirectResources, _, err := vctx.ServiceTester.LookupResources(t.Context(), rel.Resource.RelationReference(), rel.Subject, vctx.Revision, nil, 0, nil)
							require.NoError(t, err)

							resolvedIndirectResourcesMap := map[string]*v1.LookupResourcesResponse{}
							for _, resource := range resolvedIndirectResources {
								resolvedIndirectResourcesMap[resource.ResourceObjectId] = resource
							}

							// Check the assertion was returned for a direct (with context) lookup.
							resolvedDirectResourceIds := slices.Collect(maps.Keys(resolvedDirectResourcesMap))
							switch permissionship {
							case v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION:
								require.NotContains(t, resolvedDirectResourceIds, rel.Resource.ObjectID, "Found unexpected object %s in direct lookup for assertion %s", rel.Resource, rel)

							case v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION:
								require.Contains(t, resolvedDirectResourceIds, rel.Resource.ObjectID, "Missing object %s in lookup for assertion %s", rel.Resource, rel)
								require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION, resolvedDirectResourcesMap[rel.Resource.ObjectID].Permissionship)

							case v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION:
								require.Contains(t, resolvedDirectResourceIds, rel.Resource.ObjectID, "Missing object %s in lookup for assertion %s", rel.Resource, rel)
								require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION, resolvedDirectResourcesMap[rel.Resource.ObjectID].Permissionship,
									"Expected caveated permission in direct lookup for assertion %s", rel,
								)
							}

							// Check the assertion was returned for an indirect (without context) lookup.
							resolvedIndirectResourceIds := slices.Collect(maps.Keys(resolvedIndirectResourcesMap))
							accessibility, _, _ := vctx.AccessibilitySet.AccessibilityAndPermissionshipFor(rel.Resource, rel.Subject)

							switch permissionship {
							case v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION:
								// If the caveat context given is empty, then the lookup result must not exist at all.
								// Otherwise, it *could* be caveated or not exist, depending on the context given.
								switch {
								case len(assertion.CaveatContext) == 0:
									require.NotContains(t, resolvedIndirectResourceIds, rel.Resource.ObjectID, "Found unexpected object %s in indirect lookup for assertion %s", rel.Resource, rel)
								case accessibility == NotAccessible:
									found, ok := resolvedIndirectResourcesMap[rel.Resource.ObjectID]
									require.True(t, !ok || found.Permissionship != v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION) // LookupResources can be caveated, since we didn't rerun LookupResources with the context
								case accessibility != NotAccessibleDueToPrespecifiedCaveat:
									found, ok := resolvedIndirectResourcesMap[rel.Resource.ObjectID]
									require.True(t, ok, "Missing expected object %s in indirect lookup for assertion %s", rel.Resource, rel)
									require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION, found.Permissionship,
										"Expected caveated permission in indirect lookup for assertion %s", rel,
									)
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
						results, err := vctx.ServiceTester.BulkCheck(t.Context(), bulkCheckItems, vctx.Revision)
						require.NoError(t, err)

						for _, result := range results {
							require.Equal(t, entry.expectedPermissionship, result.GetItem().GetPermissionship(), "Bulk check for assertion request `%s` returned %s; expected %s", result.GetRequest(), result.GetItem().GetPermissionship(), entry.expectedPermissionship)
						}
					}
				})
			}
		}
	})
}

// ValidateReachableSubjectTypes validates that the reachable subject types are those expected.
func ValidateReachableSubjectTypes(t *testing.T, vctx ValidationContext) {
	testForEachResource(t, vctx, "validate_reachable_subject_types", func(t *testing.T, resource tuple.ObjectAndRelation) {
		headRevResult, err := vctx.ClusterAndData.DataStore.HeadRevision(t.Context())
		require.NoError(t, err)
		headRev := headRevResult.Revision

		reader := vctx.ClusterAndData.DataStore.SnapshotReader(headRev)
		ts := schema.NewTypeSystem(schema.ResolverForDatastoreReader(reader))

		reachableSubjectTypes, err := ts.GetFullRecursiveSubjectTypesForRelation(t.Context(), resource.ObjectType, resource.Relation)
		require.NoError(t, err)

		reachableSubjectTypesSet := mapz.NewSet(reachableSubjectTypes...)

		for _, member := range vctx.AccessibilitySet.DirectlyAccessibleDefinedSubjects(resource) {
			subjectTypeRef := member.ObjectType
			if member.Relation != tuple.Ellipsis {
				subjectTypeRef = subjectTypeRef + "#" + member.Relation
			}

			require.True(t, reachableSubjectTypesSet.Has(subjectTypeRef),
				"Found unexpected subject type %s#%s in lookup subjects for resource %s; expected one of %s",
				member.ObjectType,
				member.Relation,
				tuple.StringONR(resource),
				reachableSubjectTypesSet.AsSlice(),
			)
		}
	})
}
