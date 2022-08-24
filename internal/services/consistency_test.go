package services_test

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jwangsadinata/go-multimap/setmultimap"
	"github.com/jwangsadinata/go-multimap/slicemultimap"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	yamlv2 "gopkg.in/yaml.v2"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/internal/membership"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	v0svc "github.com/authzed/spicedb/internal/services/v0"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
)

var testTimedeltas = []time.Duration{1 * time.Second}

func TestConsistency(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	consistencyTestFiles := []string{}
	err := filepath.Walk(path.Join(path.Dir(filename), "testconfigs"), func(path string, info os.FileInfo, err error) error {
		if info == nil || info.IsDir() {
			return nil
		}

		if strings.HasSuffix(info.Name(), ".yaml") {
			consistencyTestFiles = append(consistencyTestFiles, path)
		}

		return nil
	})

	rrequire := require.New(t)
	rrequire.NoError(err)

	for _, delta := range testTimedeltas {
		t.Run(fmt.Sprintf("fuzz%d", delta/time.Millisecond), func(t *testing.T) {
			for _, filePath := range consistencyTestFiles {
				t.Run(path.Base(filePath), func(t *testing.T) {
					for _, dispatcherKind := range []string{"local", "caching"} {
						t.Run(dispatcherKind, func(t *testing.T) {
							t.Parallel()
							lrequire := require.New(t)

							ds, err := memdb.NewMemdbDatastore(0, delta, memdb.DisableGC)
							require.NoError(t, err)

							fullyResolved, revision, err := validationfile.PopulateFromFiles(ds, []string{filePath})
							require.NoError(t, err)
							conn, cleanup := testserver.TestClusterWithDispatch(t, 1, ds)
							t.Cleanup(cleanup)

							dsCtx := datastoremw.ContextWithHandle(context.Background())
							lrequire.NoError(datastoremw.SetInContext(dsCtx, ds))

							// Validate the type system for each namespace.
							for _, nsDef := range fullyResolved.NamespaceDefinitions {
								_, ts, err := namespace.ReadNamespaceAndTypes(
									dsCtx,
									nsDef.Name,
									ds.SnapshotReader(revision),
								)
								lrequire.NoError(err)

								_, err = ts.Validate(dsCtx)
								lrequire.NoError(err)
							}

							// Build the list of tuples per namespace.
							tuplesPerNamespace := slicemultimap.New()
							for _, tpl := range fullyResolved.Tuples {
								tuplesPerNamespace.Put(tpl.ResourceAndRelation.Namespace, tpl)
							}

							// Run the consistency tests for each service.
							dispatcher := graph.NewLocalOnlyDispatcher(10)
							if dispatcherKind == "caching" {
								cachingDispatcher, err := caching.NewCachingDispatcher(nil, "", &keys.CanonicalKeyHandler{})
								lrequire.NoError(err)

								localDispatcher := graph.NewDispatcher(cachingDispatcher, 10)
								defer localDispatcher.Close()
								cachingDispatcher.SetDelegate(localDispatcher)
								dispatcher = cachingDispatcher
							}
							defer dispatcher.Close()

							testers := []serviceTester{
								v1ServiceTester{v1.NewPermissionsServiceClient(conn[0])},
							}

							runCrossVersionTests(t, testers, fullyResolved, revision)

							for _, tester := range testers {
								t.Run(tester.Name(), func(t *testing.T) {
									runConsistencyTests(t, tester, ds, dispatcher, fullyResolved, tuplesPerNamespace, revision)
									runAssertions(t, tester, dispatcher, fullyResolved, revision)
								})
							}
						})
					}
				})
			}
		})
	}
}

func runAssertions(t *testing.T,
	tester serviceTester,
	dispatch dispatch.Dispatcher,
	fullyResolved *validationfile.PopulatedValidationFile,
	revision decimal.Decimal,
) {
	for _, parsedFile := range fullyResolved.ParsedFiles {
		for _, assertTrue := range parsedFile.Assertions.AssertTrue {
			// Ensure the assertion passes Check.
			rel := tuple.MustFromRelationship(assertTrue.Relationship)
			result, err := tester.Check(context.Background(), rel.ResourceAndRelation, rel.Subject, revision)
			require.NoError(t, err)
			require.True(t, result, "Assertion `%s` returned false; true expected", tuple.String(rel))

			// Ensure the assertion passes Lookup.
			resolvedObjectIds, err := tester.Lookup(context.Background(), &core.RelationReference{
				Namespace: rel.ResourceAndRelation.Namespace,
				Relation:  rel.ResourceAndRelation.Relation,
			}, rel.Subject, revision)
			require.NoError(t, err)
			require.Contains(t, resolvedObjectIds, rel.ResourceAndRelation.ObjectId, "Missing object %s in lookup for assertion %s", rel.ResourceAndRelation, rel)
		}

		for _, assertFalse := range parsedFile.Assertions.AssertFalse {
			// Ensure the assertion passes Check.
			rel := tuple.MustFromRelationship(assertFalse.Relationship)

			// Ensure the assertion does not pass Check.
			result, err := tester.Check(context.Background(), rel.ResourceAndRelation, rel.Subject, revision)
			require.NoError(t, err)
			require.False(t, result, "Assertion `%s` returned true; false expected", tuple.String(rel))

			// Ensure the assertion does not pass Lookup.
			resolvedObjectIds, err := tester.Lookup(context.Background(), &core.RelationReference{
				Namespace: rel.ResourceAndRelation.Namespace,
				Relation:  rel.ResourceAndRelation.Relation,
			}, rel.Subject, revision)
			require.NoError(t, err)
			require.NotContains(t, resolvedObjectIds, rel.ResourceAndRelation.ObjectId, "Found unexpected object %s in lookup for false assertion %s", rel.ResourceAndRelation, rel)
		}
	}
}

func runCrossVersionTests(t *testing.T,
	testers []serviceTester,
	fullyResolved *validationfile.PopulatedValidationFile,
	revision decimal.Decimal,
) {
	// NOTE: added to skip tests when there is only one version defined.
	if len(testers) < 2 {
		return
	}

	for _, nsDef := range fullyResolved.NamespaceDefinitions {
		for _, relation := range nsDef.Relation {
			verifyCrossVersion(t, "read", testers, func(tester serviceTester) (interface{}, error) {
				return tester.Read(context.Background(), nsDef.Name, revision)
			})

			for _, tpl := range fullyResolved.Tuples {
				if tpl.ResourceAndRelation.Namespace != nsDef.Name {
					continue
				}

				verifyCrossVersion(t, "expand", testers, func(tester serviceTester) (interface{}, error) {
					return tester.Expand(context.Background(), &core.ObjectAndRelation{
						Namespace: nsDef.Name,
						Relation:  relation.Name,
						ObjectId:  tpl.ResourceAndRelation.ObjectId,
					}, revision)
				})

				verifyCrossVersion(t, "lookup", testers, func(tester serviceTester) (interface{}, error) {
					return tester.Lookup(context.Background(), &core.RelationReference{
						Namespace: nsDef.Name,
						Relation:  relation.Name,
					}, &core.ObjectAndRelation{
						Namespace: tpl.ResourceAndRelation.Namespace,
						Relation:  tpl.ResourceAndRelation.Relation,
						ObjectId:  tpl.ResourceAndRelation.ObjectId,
					}, revision)
				})
			}
		}
	}
}

type apiRunner func(tester serviceTester) (interface{}, error)

func verifyCrossVersion(t *testing.T, name string, testers []serviceTester, runAPI apiRunner) {
	t.Run(fmt.Sprintf("crossversion_%s", name), func(t *testing.T) {
		var result interface{}
		for _, tester := range testers {
			value, err := runAPI(tester)
			require.NoError(t, err)
			if result == nil {
				result = value
			} else {
				testutil.RequireEqualEmptyNil(t, result, value, "found mismatch between versions")
			}
		}
	})
}

func runConsistencyTests(t *testing.T,
	tester serviceTester,
	ds datastore.Datastore,
	dispatch dispatch.Dispatcher,
	fullyResolved *validationfile.PopulatedValidationFile,
	tuplesPerNamespace *slicemultimap.MultiMap,
	revision decimal.Decimal,
) {
	lrequire := require.New(t)

	// Read all tuples defined in the namespaces.
	for _, nsDef := range fullyResolved.NamespaceDefinitions {
		tuples, err := tester.Read(context.Background(), nsDef.Name, revision)
		lrequire.NoError(err)

		expected, _ := tuplesPerNamespace.Get(nsDef.Name)
		lrequire.Equal(len(expected), len(tuples))
	}

	// Call a write on each tuple to make sure it type checks.
	for _, nsDef := range fullyResolved.NamespaceDefinitions {
		tuples, ok := tuplesPerNamespace.Get(nsDef.Name)
		if !ok {
			continue
		}

		for _, itpl := range tuples {
			tpl := itpl.(*core.RelationTuple)
			err := tester.Write(context.Background(), tpl)
			lrequire.NoError(err, "failed to write %s", tuple.String(tpl))
		}
	}

	// Collect the set of objects and subjects.
	objectsPerNamespace := setmultimap.New()
	subjects := tuple.NewONRSet()
	subjectsNoWildcard := tuple.NewONRSet()
	for _, tpl := range fullyResolved.Tuples {
		objectsPerNamespace.Put(tpl.ResourceAndRelation.Namespace, tpl.ResourceAndRelation.ObjectId)
		subjects.Add(tpl.Subject)

		if tpl.Subject.ObjectId != tuple.PublicWildcard {
			objectsPerNamespace.Put(tpl.Subject.Namespace, tpl.Subject.ObjectId)
			subjectsNoWildcard.Add(tpl.Subject)
		}
	}

	// Collect the set of accessible objects for each namespace and subject.
	accessibilitySet := newAccessibilitySet()

	for _, nsDef := range fullyResolved.NamespaceDefinitions {
		for _, relation := range nsDef.Relation {
			for _, subject := range subjects.AsSlice() {
				allObjectIds, ok := objectsPerNamespace.Get(nsDef.Name)
				if !ok {
					continue
				}

				for _, objectID := range allObjectIds {
					objectIDStr := objectID.(string)

					onr := &core.ObjectAndRelation{
						Namespace: nsDef.Name,
						Relation:  relation.Name,
						ObjectId:  objectIDStr,
					}

					if subject.ObjectId == tuple.PublicWildcard {
						accessibilitySet.Set(onr, subject, isWildcard)
						continue
					}

					hasPermission, err := tester.Check(context.Background(), onr, subject, revision)
					require.NoError(t, err)

					// If a member, check if due to a wildcard only.
					if hasPermission && accessibleViaWildcardOnly(t, ds, dispatch, onr, subject, revision) {
						accessibilitySet.Set(onr, subject, isMemberViaWildcard)
						continue
					}

					if hasPermission {
						accessibilitySet.Set(onr, subject, isMember)
					} else {
						accessibilitySet.Set(onr, subject, isNotMember)
					}
				}
			}
		}
	}

	vctx := &validationContext{
		fullyResolved:       fullyResolved,
		objectsPerNamespace: objectsPerNamespace,
		accessibilitySet:    accessibilitySet,
		dispatch:            dispatch,
		subjects:            subjects,
		subjectsNoWildcard:  subjectsNoWildcard,
		tester:              tester,
		revision:            revision,
	}

	// Run basic expansion on each relation and ensure it matches the structure of the relation.
	validateExpansion(t, vctx)

	// Run a fully recursive expand on each relation and ensure all terminal subjects are reached.
	validateExpansionSubjects(t, ds, vctx)

	// For each relation in each namespace, for each user, collect the objects accessible
	// to that user and then verify the lookup resources returns the same set of objects.
	validateLookupResources(t, vctx)

	// For each object accessible, validate that the subjects that can access it are found.
	validateLookupSubject(t, vctx)

	// Run the developer APIs over the full set of context and ensure they also return the expected information.
	store := v0svc.NewInMemoryShareStore("flavored")
	dev := v0svc.NewDeveloperServer(store)

	validateDeveloper(t, dev, vctx)
}

func accessibleViaWildcardOnly(t *testing.T, ds datastore.Datastore, dispatch dispatch.Dispatcher, onr *core.ObjectAndRelation, subject *core.ObjectAndRelation, revision decimal.Decimal) bool {
	ctx := datastoremw.ContextWithHandle(context.Background())
	require.NoError(t, datastoremw.SetInContext(ctx, ds))

	resp, err := dispatch.DispatchExpand(ctx, &dispatchv1.DispatchExpandRequest{
		ResourceAndRelation: onr,
		Metadata: &dispatchv1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 100,
		},
		ExpansionMode: dispatchv1.DispatchExpandRequest_RECURSIVE,
	})
	require.NoError(t, err)

	subjectsFound, err := membership.AccessibleExpansionSubjects(resp.TreeNode)
	require.NoError(t, err)
	return !subjectsFound.Contains(subject)
}

type validationContext struct {
	fullyResolved *validationfile.PopulatedValidationFile

	objectsPerNamespace *setmultimap.MultiMap
	subjects            *tuple.ONRSet
	subjectsNoWildcard  *tuple.ONRSet
	accessibilitySet    *accessibilitySet

	dispatch dispatch.Dispatcher

	tester   serviceTester
	revision decimal.Decimal
}

func validateDeveloper(t *testing.T, dev v0.DeveloperServiceServer, vctx *validationContext) {
	schema := vctx.fullyResolved.Schema
	reqContext := &v0.RequestContext{
		Schema:        schema,
		Relationships: core.ToV0RelationTuples(vctx.fullyResolved.Tuples),
	}

	// Validate edit checks (check watches).
	validateEditChecks(t, dev, reqContext, vctx)

	// Validate assertions and expected relations.
	validateValidation(t, dev, reqContext, vctx)
}

func validateValidation(t *testing.T, dev v0.DeveloperServiceServer, reqContext *v0.RequestContext, vctx *validationContext) {
	// Build the Expected Relations (inputs only).
	expectedMap := map[string]interface{}{}
	for _, result := range vctx.accessibilitySet.results {
		if result.isMember == isMember {
			expectedMap[tuple.StringONR(result.object)] = []string{}
		}
	}

	expectedRelations, err := yamlv2.Marshal(expectedMap)
	require.NoError(t, err, "Could not marshal expected relations map")

	// Run validation with the expected map, to generate the full expected relations YAML string.
	resp, err := dev.Validate(context.Background(), &v0.ValidateRequest{
		Context:              reqContext,
		ValidationYaml:       string(expectedRelations),
		UpdateValidationYaml: true,
	})
	require.NoError(t, err, "Got unexpected error from validation")
	require.Equal(t, 0, len(resp.RequestErrors), "Got unexpected request error from validation: %s", resp.RequestErrors)

	// Parse the full validation YAML, and ensure every referenced subject is, in fact, allowed.
	updatedValidationYaml := resp.UpdatedValidationYaml
	validationMap, err := validationfile.ParseExpectedRelationsBlock([]byte(updatedValidationYaml))
	require.NoError(t, err)

	for onrKey, expectedSubjects := range validationMap.ValidationMap {
		for _, expectedSubject := range expectedSubjects {
			onr := onrKey.ObjectAndRelation
			subjectWithExceptions := expectedSubject.SubjectWithExceptions
			require.NotNil(t, subjectWithExceptions, "Found expected relation without subject: %s", expectedSubject.ValidationString)
			require.Nil(t, err)
			require.True(t,
				(vctx.accessibilitySet.GetIsMember(onr, subjectWithExceptions.Subject) == isMember ||
					vctx.accessibilitySet.GetIsMember(onr, subjectWithExceptions.Subject) == isWildcard),
				"Generated expected relations returned inaccessible member %s for %s in `%s`",
				tuple.StringONR(subjectWithExceptions.Subject),
				tuple.StringONR(onr),
				updatedValidationYaml)
		}
	}

	// Build the assertions YAML.
	var trueAssertions []string
	var falseAssertions []string

	for _, result := range vctx.accessibilitySet.results {
		if result.isMember == isMember || result.isMember == isMemberViaWildcard {
			trueAssertions = append(trueAssertions, fmt.Sprintf("%s@%s", tuple.StringONR(result.object), tuple.StringONR(result.subject)))
		} else if result.isMember == isNotMember {
			falseAssertions = append(falseAssertions, fmt.Sprintf("%s@%s", tuple.StringONR(result.object), tuple.StringONR(result.subject)))
		}
	}

	assertionsMap := map[string]interface{}{
		"assertTrue":  trueAssertions,
		"assertFalse": falseAssertions,
	}
	assertions, err := yamlv2.Marshal(assertionsMap)
	require.NoError(t, err, "Could not marshal assertions map")

	// Run validation with the assertions and the updated YAML.
	resp, err = dev.Validate(context.Background(), &v0.ValidateRequest{
		Context:        reqContext,
		AssertionsYaml: string(assertions),
		ValidationYaml: updatedValidationYaml,
	})
	require.NoError(t, err, "Got unexpected error from validation")
	require.Equal(t, 0, len(resp.RequestErrors), "Got unexpected request error from validation: %s", resp.RequestErrors)
	require.Equal(t, 0, len(resp.ValidationErrors), "Got unexpected validation error from validation: %s", resp.ValidationErrors)
}

func validateEditChecks(t *testing.T, dev v0.DeveloperServiceServer, reqContext *v0.RequestContext, vctx *validationContext) {
	for _, nsDef := range vctx.fullyResolved.NamespaceDefinitions {
		for _, relation := range nsDef.Relation {
			for _, subject := range vctx.subjectsNoWildcard.AsSlice() {
				objectRelation := &v0.RelationReference{
					Namespace: nsDef.Name,
					Relation:  relation.Name,
				}

				// Run EditCheck to validate checks for each object under the namespace.
				t.Run(fmt.Sprintf("editcheck_%s_%s_to_%s_%s_%s", objectRelation.Namespace, objectRelation.Relation, subject.Namespace, subject.ObjectId, subject.Relation), func(t *testing.T) {
					vrequire := require.New(t)

					allObjectIds, ok := vctx.objectsPerNamespace.Get(nsDef.Name)
					if !ok {
						return
					}

					// Add a check relationship for each object ID.
					var checkRelationships []*core.RelationTuple
					for _, objectID := range allObjectIds {
						objectIDStr := objectID.(string)
						checkRelationships = append(checkRelationships, &core.RelationTuple{
							ResourceAndRelation: &core.ObjectAndRelation{
								Namespace: nsDef.Name,
								Relation:  relation.Name,
								ObjectId:  objectIDStr,
							},
							Subject: subject,
						})
					}

					// Ensure that all Checks assert true via the developer API.
					req := &v0.EditCheckRequest{
						Context:            reqContext,
						CheckRelationships: core.ToV0RelationTuples(checkRelationships),
					}

					resp, err := dev.EditCheck(context.Background(), req)
					vrequire.NoError(err, "Got unexpected error from edit check")
					vrequire.Equal(len(checkRelationships), len(resp.CheckResults))
					vrequire.Equal(0, len(resp.RequestErrors), "Got unexpected request error from edit check")
					for _, result := range resp.CheckResults {
						expectedMember := vctx.accessibilitySet.GetIsMember(core.ToCoreObjectAndRelation(result.Relationship.ObjectAndRelation), subject)
						vrequire.Equal(expectedMember == isMember || expectedMember == isMemberViaWildcard, result.IsMember, "Found unexpected membership difference for %s. Expected %v, Found: %v", tuple.String(core.ToCoreRelationTuple(result.Relationship)), expectedMember, result.IsMember)
					}
				})
			}
		}
	}
}

func validateLookupResources(t *testing.T, vctx *validationContext) {
	for _, nsDef := range vctx.fullyResolved.NamespaceDefinitions {
		for _, relation := range nsDef.Relation {
			for _, subject := range vctx.subjectsNoWildcard.AsSlice() {
				objectRelation := &core.RelationReference{
					Namespace: nsDef.Name,
					Relation:  relation.Name,
				}

				t.Run(fmt.Sprintf("lookupresources_%s_%s_to_%s_%s_%s", objectRelation.Namespace, objectRelation.Relation, subject.Namespace, subject.ObjectId, subject.Relation), func(t *testing.T) {
					vrequire := require.New(t)
					accessibleObjectIds := vctx.accessibilitySet.AccessibleObjectIDs(objectRelation.Namespace, objectRelation.Relation, subject)

					// Perform a lookup call and ensure it returns the at least the same set of object IDs.
					resolvedObjectIds, err := vctx.tester.Lookup(context.Background(), objectRelation, subject, vctx.revision)
					vrequire.NoError(err)

					sort.Strings(accessibleObjectIds)
					sort.Strings(resolvedObjectIds)

					for _, accessibleObjectID := range accessibleObjectIds {
						vrequire.True(
							contains(resolvedObjectIds, accessibleObjectID),
							"Object `%s` missing in lookup results for %s#%s@%s: Expected: %v. Found: %v",
							accessibleObjectID,
							nsDef.Name,
							relation.Name,
							tuple.StringONR(subject),
							accessibleObjectIds,
							resolvedObjectIds,
						)
					}

					// Ensure that every returned object Checks.
					for _, resolvedObjectID := range resolvedObjectIds {
						isMember, err := vctx.tester.Check(context.Background(),
							&core.ObjectAndRelation{
								Namespace: nsDef.Name,
								Relation:  relation.Name,
								ObjectId:  resolvedObjectID,
							},
							subject,
							vctx.revision,
						)
						vrequire.NoError(err)
						vrequire.True(
							isMember,
							"Found Check failure for relation %s:%s#%s and subject %s",
							nsDef.Name,
							resolvedObjectID,
							relation.Name,
							tuple.StringONR(subject),
						)
					}
				})
			}
		}
	}
}

func validateLookupSubject(t *testing.T, vctx *validationContext) {
	for _, nsDef := range vctx.fullyResolved.NamespaceDefinitions {
		allObjectIds, ok := vctx.objectsPerNamespace.Get(nsDef.Name)
		if !ok {
			continue
		}

		for _, relation := range nsDef.Relation {
			for _, objectID := range allObjectIds {
				objectIDStr := objectID.(string)

				accessibleSubjectsByType := vctx.accessibilitySet.AccessibleSubjectsByType(nsDef.Name, relation.Name, objectIDStr)
				accessibleSubjectsByType.ForEachType(func(subjectType *core.RelationReference, expectedObjectIds []string) {
					t.Run(fmt.Sprintf("lookupsubjects_%s_%s_%s_to_%s_%s", nsDef.Name, relation.Name, objectID, subjectType.Namespace, subjectType.Relation), func(t *testing.T) {
						vrequire := require.New(t)

						// Perform a lookup call and ensure it returns the at least the same set of object IDs.
						resource := &core.ObjectAndRelation{
							Namespace: nsDef.Name,
							ObjectId:  objectIDStr,
							Relation:  relation.Name,
						}
						resolvedObjectIds, err := vctx.tester.LookupSubjects(context.Background(), resource, subjectType, vctx.revision)
						vrequire.NoError(err)

						sort.Strings(expectedObjectIds)
						sort.Strings(resolvedObjectIds)

						// Ensure the object IDs match.
						for _, expectedObjectID := range expectedObjectIds {
							vrequire.True(
								contains(resolvedObjectIds, expectedObjectID),
								"Object `%s` missing in lookup subjects results for subjects of %s under %s: Expected: %v. Found: %v",
								expectedObjectID,
								tuple.StringRR(subjectType),
								tuple.StringONR(resource),
								expectedObjectIds,
								resolvedObjectIds,
							)
						}

						// Ensure that every returned object Checks.
						for _, resolvedObjectID := range resolvedObjectIds {
							if resolvedObjectID == tuple.PublicWildcard {
								continue
							}

							subject := &core.ObjectAndRelation{
								Namespace: subjectType.Namespace,
								ObjectId:  resolvedObjectID,
								Relation:  subjectType.Relation,
							}
							isMember, err := vctx.tester.Check(context.Background(),
								resource,
								subject,
								vctx.revision,
							)
							vrequire.NoError(err)
							vrequire.True(
								isMember,
								"Found Check failure for resource %s and subject %s",
								nsDef.Name,
								tuple.StringONR(resource),
								tuple.StringONR(subject),
							)
						}
					})
				})
			}
		}
	}
}

func validateExpansion(t *testing.T, vctx *validationContext) {
	for _, nsDef := range vctx.fullyResolved.NamespaceDefinitions {
		allObjectIds, ok := vctx.objectsPerNamespace.Get(nsDef.Name)
		if !ok {
			continue
		}

		for _, relation := range nsDef.Relation {
			for _, objectID := range allObjectIds {
				objectIDStr := objectID.(string)
				t.Run(fmt.Sprintf("expand_%s_%s_%s", nsDef.Name, objectIDStr, relation.Name), func(t *testing.T) {
					vrequire := require.New(t)

					_, err := vctx.tester.Expand(context.Background(),
						&core.ObjectAndRelation{
							Namespace: nsDef.Name,
							Relation:  relation.Name,
							ObjectId:  objectIDStr,
						},
						vctx.revision,
					)
					vrequire.NoError(err)
				})
			}
		}
	}
}

func validateExpansionSubjects(t *testing.T, ds datastore.Datastore, vctx *validationContext) {
	ctx := datastoremw.ContextWithHandle(context.Background())
	require.NoError(t, datastoremw.SetInContext(ctx, ds))
	for _, nsDef := range vctx.fullyResolved.NamespaceDefinitions {
		allObjectIds, ok := vctx.objectsPerNamespace.Get(nsDef.Name)
		if !ok {
			continue
		}

		for _, relation := range nsDef.Relation {
			for _, objectID := range allObjectIds {
				objectIDStr := objectID.(string)
				t.Run(fmt.Sprintf("expand_subjects_%s_%s_%s", nsDef.Name, objectIDStr, relation.Name), func(t *testing.T) {
					vrequire := require.New(t)
					accessibleTerminalSubjects := vctx.accessibilitySet.AccessibleTerminalSubjects(nsDef.Name, relation.Name, objectIDStr)

					// Run a non-recursive expansion to verify no errors are raised.
					_, err := vctx.dispatch.DispatchExpand(ctx, &dispatchv1.DispatchExpandRequest{
						ResourceAndRelation: &core.ObjectAndRelation{
							Namespace: nsDef.Name,
							Relation:  relation.Name,
							ObjectId:  objectIDStr,
						},
						Metadata: &dispatchv1.ResolverMeta{
							AtRevision:     vctx.revision.String(),
							DepthRemaining: 100,
						},
						ExpansionMode: dispatchv1.DispatchExpandRequest_SHALLOW,
					})
					vrequire.NoError(err)

					// Run a *recursive* expansion and ensure that the subjects found matches those found via Check.
					resp, err := vctx.dispatch.DispatchExpand(ctx, &dispatchv1.DispatchExpandRequest{
						ResourceAndRelation: &core.ObjectAndRelation{
							Namespace: nsDef.Name,
							Relation:  relation.Name,
							ObjectId:  objectIDStr,
						},
						Metadata: &dispatchv1.ResolverMeta{
							AtRevision:     vctx.revision.String(),
							DepthRemaining: 100,
						},
						ExpansionMode: dispatchv1.DispatchExpandRequest_RECURSIVE,
					})
					vrequire.NoError(err)

					subjectsFoundSet, err := membership.AccessibleExpansionSubjects(resp.TreeNode)
					vrequire.NoError(err)

					// Ensure all terminal subjects were found in the expansion.
					vrequire.EqualValues(0, len(accessibleTerminalSubjects.Exclude(subjectsFoundSet).ToSlice()), "Expected %s, Found: %s", accessibleTerminalSubjects.ToSlice(), subjectsFoundSet.ToSlice())

					// Ensure every subject found matches Check.
					for _, foundSubject := range subjectsFoundSet.ToSlice() {
						excludedSubjects, isWildcard := foundSubject.ExcludedSubjectsFromWildcard()

						// If the subject is a wildcard, then check every matching subject.
						if isWildcard {
							excludedSubjectsSet := tuple.NewONRSet(excludedSubjects...)

							allSubjectObjectIds, ok := vctx.objectsPerNamespace.Get(foundSubject.Subject().Namespace)
							if !ok {
								continue
							}

							for _, subjectID := range allSubjectObjectIds {
								subjectIDStr := subjectID.(string)
								localSubject := &core.ObjectAndRelation{
									Namespace: foundSubject.Subject().Namespace,
									Relation:  foundSubject.Subject().Relation,
									ObjectId:  subjectIDStr,
								}
								isMember, err := vctx.tester.Check(context.Background(),
									&core.ObjectAndRelation{
										Namespace: nsDef.Name,
										Relation:  relation.Name,
										ObjectId:  objectIDStr,
									},
									localSubject,
									vctx.revision,
								)
								vrequire.NoError(err)
								vrequire.Equal(
									!excludedSubjectsSet.Has(localSubject),
									isMember,
									"Found Check under Expand failure for relation %s:%s#%s and subject %s (checked because of wildcard %s). Expected: %v, Found: %v",
									nsDef.Name,
									objectIDStr,
									relation.Name,
									tuple.StringONR(localSubject),
									tuple.StringONR(foundSubject.Subject()),
									!excludedSubjectsSet.Has(localSubject),
									isMember,
								)
							}
						} else {
							// Otherwise, check directly.
							isMember, err := vctx.tester.Check(context.Background(),
								&core.ObjectAndRelation{
									Namespace: nsDef.Name,
									Relation:  relation.Name,
									ObjectId:  objectIDStr,
								},
								foundSubject.Subject(),
								vctx.revision,
							)
							vrequire.NoError(err)
							vrequire.True(
								isMember,
								"Found Check under Expand failure for relation %s:%s#%s and subject %s",
								nsDef.Name,
								objectIDStr,
								relation.Name,
								tuple.StringONR(foundSubject.Subject()),
							)
						}
					}
				})
			}
		}
	}
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

type isMemberStatus int

const (
	isNotMember         isMemberStatus = 0
	isMember            isMemberStatus = 1
	isMemberViaWildcard isMemberStatus = 2
	isWildcard          isMemberStatus = 3
)

type checkResult struct {
	object   *core.ObjectAndRelation
	subject  *core.ObjectAndRelation
	isMember isMemberStatus
}

// TODO(jschorr): optimize the accessibility set if the consistency tests ever become slow enough
// that it matters.
type accessibilitySet struct {
	results []checkResult
}

func newAccessibilitySet() *accessibilitySet {
	return &accessibilitySet{
		results: []checkResult{},
	}
}

func (rs *accessibilitySet) Set(object *core.ObjectAndRelation, subject *core.ObjectAndRelation, isMember isMemberStatus) {
	rs.results = append(rs.results, checkResult{object: object, subject: subject, isMember: isMember})
}

func (rs *accessibilitySet) GetIsMember(object *core.ObjectAndRelation, subject *core.ObjectAndRelation) isMemberStatus {
	objectStr := tuple.StringONR(object)
	subjectStr := tuple.StringONR(subject)

	for _, result := range rs.results {
		if tuple.StringONR(result.object) == objectStr && tuple.StringONR(result.subject) == subjectStr {
			return result.isMember
		}
	}

	panic(fmt.Sprintf("Missing matching result for %s %s", object, subject))
}

// AccessibleObjectIDs returns the set of object IDs accessible for the given subject from the given relation on the namespace.
func (rs *accessibilitySet) AccessibleObjectIDs(namespaceName string, relationName string, subject *core.ObjectAndRelation) []string {
	var accessibleObjectIDs []string
	subjectStr := tuple.StringONR(subject)
	for _, result := range rs.results {
		if result.isMember == isNotMember {
			continue
		}

		if result.object.Namespace == namespaceName && result.object.Relation == relationName && tuple.StringONR(result.subject) == subjectStr {
			accessibleObjectIDs = append(accessibleObjectIDs, result.object.ObjectId)
		}
	}
	return accessibleObjectIDs
}

// AccessibleSubjects returns the set of subjects with accessible for the given object on the given relation on the namespace
func (rs *accessibilitySet) AccessibleSubjectsByType(namespaceName string, relationName string, objectIDStr string) *tuple.ONRByTypeSet {
	accessibleSubjects := tuple.NewONRByTypeSet()
	for _, result := range rs.results {
		if result.isMember == isNotMember || result.isMember == isWildcard || result.isMember == isMemberViaWildcard {
			continue
		}

		if result.object.Namespace == namespaceName && result.object.Relation == relationName && result.object.ObjectId == objectIDStr {
			accessibleSubjects.Add(result.subject)
		}
	}
	return accessibleSubjects
}

// AccessibleTerminalSubjects returns the set of terminal subjects with accessible for the given object on the given relation on the namespace
func (rs *accessibilitySet) AccessibleTerminalSubjects(namespaceName string, relationName string, objectIDStr string) *membership.TrackingSubjectSet {
	accessibleSubjects := membership.NewTrackingSubjectSet()
	for _, result := range rs.results {
		if result.isMember == isNotMember || result.isMember == isWildcard {
			continue
		}

		if result.object.Namespace == namespaceName && result.object.Relation == relationName && result.object.ObjectId == objectIDStr && result.subject.Relation == "..." {
			accessibleSubjects.Add(membership.NewFoundSubject(result.subject, result.object))
		}
	}
	return accessibleSubjects
}
