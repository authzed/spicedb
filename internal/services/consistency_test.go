package services

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
	"github.com/jwangsadinata/go-multimap/setmultimap"
	"github.com/jwangsadinata/go-multimap/slicemultimap"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/namespace"
	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
	v0svc "github.com/authzed/spicedb/internal/services/v0"
	v1svc "github.com/authzed/spicedb/internal/services/v1"
	"github.com/authzed/spicedb/internal/testfixtures"
	graphpkg "github.com/authzed/spicedb/pkg/graph"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
)

var testTimedeltas = []time.Duration{0, 1 * time.Second}

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
							lrequire := require.New(t)

							unvalidated, err := memdb.NewMemdbDatastore(0, delta, memdb.DisableGC, 0)
							lrequire.NoError(err)

							ds := testfixtures.NewValidatingDatastore(unvalidated)

							fullyResolved, revision, err := validationfile.PopulateFromFiles(ds, []string{filePath})
							lrequire.NoError(err)

							ns, err := namespace.NewCachingNamespaceManager(ds, 1*time.Second, nil)
							lrequire.NoError(err)

							// Validate the type system for each namespace.
							for _, nsDef := range fullyResolved.NamespaceDefinitions {
								_, ts, err := ns.ReadNamespaceAndTypes(context.Background(), nsDef.Name, revision)
								lrequire.NoError(err)

								err = ts.Validate(context.Background())
								lrequire.NoError(err)
							}

							// Build the list of tuples per namespace.
							tuplesPerNamespace := slicemultimap.New()
							for _, tpl := range fullyResolved.Tuples {
								tuplesPerNamespace.Put(tpl.ObjectAndRelation.Namespace, tpl)
							}

							// Run the consistency tests for each service.
							dispatcher := graph.NewLocalOnlyDispatcher(ns, ds)
							if dispatcherKind == "caching" {
								cachingDispatcher, err := caching.NewCachingDispatcher(nil, "")
								lrequire.NoError(err)

								localDispatcher := graph.NewDispatcher(cachingDispatcher, ns, ds)
								defer localDispatcher.Close()
								cachingDispatcher.SetDelegate(localDispatcher)
								dispatcher = cachingDispatcher
							}
							defer dispatcher.Close()

							v1permclient, _ := v1svc.RunForTesting(t, ds, ns, dispatcher, 50)
							testers := []serviceTester{
								v0ServiceTester{v0svc.NewACLServer(ds, ns, dispatcher, 50)},
								v1ServiceTester{v1permclient},
							}

							runCrossVersionTests(t, testers, dispatcher, fullyResolved, tuplesPerNamespace, revision)

							for _, tester := range testers {
								t.Run(tester.Name(), func(t *testing.T) {
									runConsistencyTests(t, tester, dispatcher, fullyResolved, tuplesPerNamespace, revision)
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
	fullyResolved *validationfile.FullyParsedValidationFile,
	revision decimal.Decimal) {
	for _, parsedFile := range fullyResolved.ParsedFiles {
		for _, assertTrueRel := range parsedFile.Assertions.AssertTrue {
			rel := tuple.Parse(assertTrueRel)
			require.NotNil(t, rel)

			result, err := tester.Check(context.Background(), rel.ObjectAndRelation, rel.User.GetUserset(), revision)
			require.NoError(t, err)
			require.True(t, result, "Assertion `%s` returned false; true expected", tuple.String(rel))
		}

		for _, assertFalseRel := range parsedFile.Assertions.AssertFalse {
			rel := tuple.Parse(assertFalseRel)
			require.NotNil(t, rel)

			result, err := tester.Check(context.Background(), rel.ObjectAndRelation, rel.User.GetUserset(), revision)
			require.NoError(t, err)
			require.False(t, result, "Assertion `%s` returned true; false expected", tuple.String(rel))
		}
	}
}

func runCrossVersionTests(t *testing.T,
	testers []serviceTester,
	dispatch dispatch.Dispatcher,
	fullyResolved *validationfile.FullyParsedValidationFile,
	tuplesPerNamespace *slicemultimap.MultiMap,
	revision decimal.Decimal) {

	for _, nsDef := range fullyResolved.NamespaceDefinitions {
		for _, relation := range nsDef.Relation {
			verifyCrossVersion(t, "read", testers, func(tester serviceTester) (interface{}, error) {
				return tester.Read(context.Background(), nsDef.Name, revision)
			})

			for _, tpl := range fullyResolved.Tuples {
				if tpl.ObjectAndRelation.Namespace != nsDef.Name {
					continue
				}

				verifyCrossVersion(t, "expand", testers, func(tester serviceTester) (interface{}, error) {
					return tester.Expand(context.Background(), &v0.ObjectAndRelation{
						Namespace: nsDef.Name,
						Relation:  relation.Name,
						ObjectId:  tpl.ObjectAndRelation.ObjectId,
					}, revision)
				})

				verifyCrossVersion(t, "lookup", testers, func(tester serviceTester) (interface{}, error) {
					return tester.Lookup(context.Background(), &v0.RelationReference{
						Namespace: nsDef.Name,
						Relation:  relation.Name,
					}, &v0.ObjectAndRelation{
						Namespace: tpl.ObjectAndRelation.Namespace,
						Relation:  tpl.ObjectAndRelation.Relation,
						ObjectId:  tpl.ObjectAndRelation.ObjectId,
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
				require.Equal(t, result, value, "Found mismatch between versions")
			}
		}
	})
}

func runConsistencyTests(t *testing.T,
	tester serviceTester,
	dispatch dispatch.Dispatcher,
	fullyResolved *validationfile.FullyParsedValidationFile,
	tuplesPerNamespace *slicemultimap.MultiMap,
	revision decimal.Decimal) {
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
			tpl := itpl.(*v0.RelationTuple)
			err := tester.Write(context.Background(), tpl)
			lrequire.NoError(err, "failed to write %s", tuple.String(tpl))
		}
	}

	// Collect the set of objects and subjects.
	objectsPerNamespace := setmultimap.New()
	subjects := tuple.NewONRSet()
	for _, tpl := range fullyResolved.Tuples {
		objectsPerNamespace.Put(tpl.ObjectAndRelation.Namespace, tpl.ObjectAndRelation.ObjectId)

		switch m := tpl.User.UserOneof.(type) {
		case *v0.User_Userset:
			subjects.Add(m.Userset)
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
					onr := &v0.ObjectAndRelation{
						Namespace: nsDef.Name,
						Relation:  relation.Name,
						ObjectId:  objectIDStr,
					}
					isMember, err := tester.Check(context.Background(), onr, subject, revision)
					require.NoError(t, err)
					accessibilitySet.Set(onr, subject, isMember)
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
		tester:              tester,
		revision:            revision,
	}

	// Run basic expansion on each relation and ensure it matches the structure of the relation.
	validateExpansion(t, vctx)

	// Run a fully recursive expand on each relation and ensure all terminal subjects are reached.
	validateExpansionSubjects(t, vctx)

	// For each relation in each namespace, for each user, collect the objects accessible
	// to that user and then verify the lookup returns the same set of objects.
	validateLookup(t, vctx)

	// Run the developer APIs over the full set of context and ensure they also return the expected information.
	store := v0svc.NewInMemoryShareStore("flavored")
	dev := v0svc.NewDeveloperServer(store)

	validateDeveloper(t, dev, vctx)
}

type validationContext struct {
	fullyResolved *validationfile.FullyParsedValidationFile

	objectsPerNamespace *setmultimap.MultiMap
	subjects            *tuple.ONRSet
	accessibilitySet    *accessibilitySet

	dispatch dispatch.Dispatcher

	tester   serviceTester
	revision decimal.Decimal
}

func validateDeveloper(t *testing.T, dev v0.DeveloperServiceServer, vctx *validationContext) {
	reqContext := &v0.RequestContext{
		LegacyNsConfigs: vctx.fullyResolved.NamespaceDefinitions,
		Relationships:   vctx.fullyResolved.Tuples,
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
		if result.isMember {
			expectedMap[tuple.StringONR(result.object)] = []string{}
		}
	}

	expectedRelations, err := yaml.Marshal(expectedMap)
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
	validationMap, err := validationfile.ParseValidationBlock([]byte(updatedValidationYaml))
	require.NoError(t, err)

	for onrStr, validationStrings := range validationMap {
		onr, err := onrStr.ONR()
		require.Nil(t, err)

		for _, validationStr := range validationStrings {
			subjectONR, err := validationStr.Subject()
			require.Nil(t, err)
			require.True(t,
				vctx.accessibilitySet.IsMember(onr, subjectONR),
				"Generated expected relations returned inaccessible member %s for %s",
				tuple.StringONR(subjectONR),
				tuple.StringONR(onr))
		}
	}

	// Build the assertions YAML.
	var trueAssertions []string
	var falseAssertions []string

	for _, result := range vctx.accessibilitySet.results {
		if result.isMember {
			trueAssertions = append(trueAssertions, fmt.Sprintf("%s@%s", tuple.StringONR(result.object), tuple.StringONR(result.subject)))
		} else {
			falseAssertions = append(falseAssertions, fmt.Sprintf("%s@%s", tuple.StringONR(result.object), tuple.StringONR(result.subject)))
		}
	}

	assertionsMap := map[string]interface{}{
		"assertTrue":  trueAssertions,
		"assertFalse": falseAssertions,
	}
	assertions, err := yaml.Marshal(assertionsMap)
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
			for _, subject := range vctx.subjects.AsSlice() {
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
					var checkRelationships []*v0.RelationTuple
					for _, objectID := range allObjectIds {
						objectIDStr := objectID.(string)
						checkRelationships = append(checkRelationships, &v0.RelationTuple{
							ObjectAndRelation: &v0.ObjectAndRelation{
								Namespace: nsDef.Name,
								Relation:  relation.Name,
								ObjectId:  objectIDStr,
							},
							User: &v0.User{
								UserOneof: &v0.User_Userset{
									Userset: subject,
								},
							},
						})
					}

					// Ensure that all Checks assert true via the developer API.
					req := &v0.EditCheckRequest{
						Context:            reqContext,
						CheckRelationships: checkRelationships,
					}

					resp, err := dev.EditCheck(context.Background(), req)
					vrequire.NoError(err, "Got unexpected error from edit check")
					vrequire.Equal(len(checkRelationships), len(resp.CheckResults))
					vrequire.Equal(0, len(resp.RequestErrors), "Got unexpected request error from edit check")
					for _, result := range resp.CheckResults {
						expectedMember := vctx.accessibilitySet.IsMember(result.Relationship.ObjectAndRelation, subject)
						vrequire.Equal(expectedMember, result.IsMember, "Found unexpected membership difference for %s", tuple.String(result.Relationship))
					}
				})
			}
		}
	}
}

func validateLookup(t *testing.T, vctx *validationContext) {
	for _, nsDef := range vctx.fullyResolved.NamespaceDefinitions {
		for _, relation := range nsDef.Relation {
			for _, subject := range vctx.subjects.AsSlice() {
				objectRelation := &v0.RelationReference{
					Namespace: nsDef.Name,
					Relation:  relation.Name,
				}

				t.Run(fmt.Sprintf("lookup_%s_%s_to_%s_%s_%s", objectRelation.Namespace, objectRelation.Relation, subject.Namespace, subject.ObjectId, subject.Relation), func(t *testing.T) {
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
							&v0.ObjectAndRelation{
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
						&v0.ObjectAndRelation{
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

func validateExpansionSubjects(t *testing.T, vctx *validationContext) {
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
					_, err := vctx.dispatch.DispatchExpand(context.Background(), &v1.DispatchExpandRequest{
						ObjectAndRelation: &v0.ObjectAndRelation{
							Namespace: nsDef.Name,
							Relation:  relation.Name,
							ObjectId:  objectIDStr,
						},
						Metadata: &v1.ResolverMeta{
							AtRevision:     vctx.revision.String(),
							DepthRemaining: 100,
						},
						ExpansionMode: v1.DispatchExpandRequest_SHALLOW,
					})
					vrequire.NoError(err)

					// Run a *recursive* expansion and ensure that the subjects found matches those found via Check.
					resp, err := vctx.dispatch.DispatchExpand(context.Background(), &v1.DispatchExpandRequest{
						ObjectAndRelation: &v0.ObjectAndRelation{
							Namespace: nsDef.Name,
							Relation:  relation.Name,
							ObjectId:  objectIDStr,
						},
						Metadata: &v1.ResolverMeta{
							AtRevision:     vctx.revision.String(),
							DepthRemaining: 100,
						},
						ExpansionMode: v1.DispatchExpandRequest_RECURSIVE,
					})
					vrequire.NoError(err)

					subjectsFound := graphpkg.Simplify(resp.TreeNode)
					subjectsFoundSet := tuple.NewONRSet()

					for _, subjectUser := range subjectsFound {
						subjectsFoundSet.Add(subjectUser.GetUserset())
					}

					// Ensure all terminal subjects were found in the expansion.
					vrequire.EqualValues(0, accessibleTerminalSubjects.Subtract(subjectsFoundSet).Length())

					// Ensure every subject found matches Check.
					for _, subjectUser := range subjectsFound {
						subject := subjectUser.GetUserset()

						isMember, err := vctx.tester.Check(context.Background(),
							&v0.ObjectAndRelation{
								Namespace: nsDef.Name,
								Relation:  relation.Name,
								ObjectId:  objectIDStr,
							},
							subject,
							vctx.revision,
						)
						vrequire.NoError(err)
						vrequire.True(
							isMember,
							"Found Check under Expand failure for relation %s:%s#%s and subject %s",
							nsDef.Name,
							objectIDStr,
							relation.Name,
							tuple.StringONR(subject),
						)
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

type checkResult struct {
	object   *v0.ObjectAndRelation
	subject  *v0.ObjectAndRelation
	isMember bool
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

func (rs *accessibilitySet) Set(object *v0.ObjectAndRelation, subject *v0.ObjectAndRelation, isMember bool) {
	rs.results = append(rs.results, checkResult{object: object, subject: subject, isMember: isMember})
}

func (rs *accessibilitySet) IsMember(object *v0.ObjectAndRelation, subject *v0.ObjectAndRelation) bool {
	objectStr := tuple.StringONR(object)
	subjectStr := tuple.StringONR(subject)

	for _, result := range rs.results {
		if tuple.StringONR(result.object) == objectStr && tuple.StringONR(result.subject) == subjectStr {
			return result.isMember
		}
	}

	panic("Missing matching result")
}

func (rs *accessibilitySet) AccessibleObjectIDs(namespaceName string, relationName string, subject *v0.ObjectAndRelation) []string {
	var accessibleObjectIDs []string
	subjectStr := tuple.StringONR(subject)
	for _, result := range rs.results {
		if !result.isMember {
			continue
		}

		if result.object.Namespace == namespaceName && result.object.Relation == relationName && tuple.StringONR(result.subject) == subjectStr {
			accessibleObjectIDs = append(accessibleObjectIDs, result.object.ObjectId)
		}
	}
	return accessibleObjectIDs
}

func (rs *accessibilitySet) AccessibleTerminalSubjects(namespaceName string, relationName string, objectIDStr string) *tuple.ONRSet {
	accessibleSubjects := tuple.NewONRSet()
	for _, result := range rs.results {
		if !result.isMember {
			continue
		}

		if result.object.Namespace == namespaceName && result.object.Relation == relationName && result.object.ObjectId == objectIDStr && result.subject.Relation == "..." {
			accessibleSubjects.Add(result.subject)
		}
	}
	return accessibleSubjects
}
