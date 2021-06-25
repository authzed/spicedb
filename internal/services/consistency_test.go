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

	"github.com/jwangsadinata/go-multimap/setmultimap"
	"github.com/jwangsadinata/go-multimap/slicemultimap"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/namespace"
	graphpkg "github.com/authzed/spicedb/pkg/graph"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
	"github.com/authzed/spicedb/pkg/zookie"
)

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
					lrequire := require.New(t)

					ds, err := memdb.NewMemdbDatastore(0, delta, memdb.DisableGC, 0)
					lrequire.NoError(err)

					fullyResolved, revision, err := validationfile.PopulateFromFiles(ds, []string{filePath})
					lrequire.NoError(err)

					ns, err := namespace.NewCachingNamespaceManager(ds, 1*time.Second, nil)
					lrequire.NoError(err)

					dispatch, err := graph.NewLocalDispatcher(ns, ds)
					lrequire.NoError(err)

					srv := NewACLServer(ds, ns, dispatch, 50)

					// Validate the type system for each namespace.
					for _, nsDef := range fullyResolved.NamespaceDefinitions {
						_, ts, _, err := ns.ReadNamespaceAndTypes(context.Background(), nsDef.Name)
						lrequire.NoError(err)

						err = ts.Validate(context.Background())
						lrequire.NoError(err)
					}

					// Build the list of tuples per namespace.
					tuplesPerNamespace := slicemultimap.New()
					for _, tpl := range fullyResolved.Tuples {
						tuplesPerNamespace.Put(tpl.ObjectAndRelation.Namespace, tpl)
					}

					// Read all tuples defined in the namespaces.
					for _, nsDef := range fullyResolved.NamespaceDefinitions {
						result, err := srv.Read(context.Background(), &v0.ReadRequest{
							Tuplesets: []*v0.RelationTupleFilter{
								&v0.RelationTupleFilter{Namespace: nsDef.Name},
							},
							AtRevision: zookie.NewFromRevision(revision),
						})
						lrequire.NoError(err)

						expected, _ := tuplesPerNamespace.Get(nsDef.Name)
						lrequire.Equal(len(expected), len(result.Tuplesets[0].Tuples))
					}

					// Call a write on each tuple to make sure it type checks.
					for _, nsDef := range fullyResolved.NamespaceDefinitions {
						tuples, ok := tuplesPerNamespace.Get(nsDef.Name)
						if !ok {
							continue
						}

						for _, itpl := range tuples {
							tpl := itpl.(*v0.RelationTuple)
							_, err = srv.Write(context.Background(), &v0.WriteRequest{
								WriteConditions: []*v0.RelationTuple{tpl},
								Updates:         []*v0.RelationTupleUpdate{tuple.Touch(tpl)},
							})
							lrequire.NoError(err)
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

					// Run a fully recursive expand on each relation and ensure all terminal subjects are reached.
					for _, nsDef := range fullyResolved.NamespaceDefinitions {
						allObjectIds, ok := objectsPerNamespace.Get(nsDef.Name)
						if !ok {
							return
						}

						for _, relation := range nsDef.Relation {
							for _, objectId := range allObjectIds {
								objectIdStr := objectId.(string)
								t.Run(fmt.Sprintf("expand_%s_%s_%s", nsDef.Name, objectIdStr, relation.Name), func(t *testing.T) {
									vrequire := require.New(t)

									// Collect all accessible terminal subjects.
									accessibleTerminalSubjects := tuple.NewONRSet()
									for _, subject := range subjects.AsSlice() {
										if subject.Relation != "..." {
											continue
										}

										objectIdStr := objectId.(string)
										checkResp, err := srv.Check(context.Background(), &v0.CheckRequest{
											TestUserset: &v0.ObjectAndRelation{
												Namespace: nsDef.Name,
												Relation:  relation.Name,
												ObjectId:  objectIdStr,
											},
											User: &v0.User{
												UserOneof: &v0.User_Userset{
													Userset: subject,
												},
											},
											AtRevision: zookie.NewFromRevision(revision),
										})
										vrequire.NoError(err)
										if checkResp.IsMember {
											accessibleTerminalSubjects.Add(subject)
										}
									}

									// Run a *recursive* expansion and ensure that the subjects found matches those found via Check.
									resp := dispatch.Expand(context.Background(), graph.ExpandRequest{
										Start: &v0.ObjectAndRelation{
											Namespace: nsDef.Name,
											Relation:  relation.Name,
											ObjectId:  objectIdStr,
										},
										AtRevision:     revision,
										DepthRemaining: 100,
										ExpansionMode:  graph.RecursiveExpansion,
									})

									vrequire.NoError(resp.Err)

									subjectsFound := graphpkg.Simplify(resp.Tree)
									subjectsFoundSet := tuple.NewONRSet()

									for _, subjectUser := range subjectsFound {
										subjectsFoundSet.Add(subjectUser.GetUserset())
									}

									// Ensure all terminal subjects were found in the expansion.
									vrequire.Equal(0, accessibleTerminalSubjects.Subtract(subjectsFoundSet).Length())

									// Ensure every subject found matches Check.
									for _, subjectUser := range subjectsFound {
										subject := subjectUser.GetUserset()

										checkResp, err := srv.Check(context.Background(), &v0.CheckRequest{
											TestUserset: &v0.ObjectAndRelation{
												Namespace: nsDef.Name,
												Relation:  relation.Name,
												ObjectId:  objectIdStr,
											},
											User: &v0.User{
												UserOneof: &v0.User_Userset{
													Userset: subject,
												},
											},
											AtRevision: zookie.NewFromRevision(revision),
										})
										vrequire.NoError(err)
										vrequire.True(
											checkResp.IsMember,
											"Found Check under Expand failure for relation %s:%s#%s and subject %s",
											nsDef.Name,
											objectIdStr,
											relation.Name,
											tuple.StringONR(subject),
										)
									}
								})
							}
						}
					}

					// For each relation in each namespace, for each user, collect the objects accessible
					// to that user and then verify the lookup returns the same set of objects.
					for _, nsDef := range fullyResolved.NamespaceDefinitions {
						for _, relation := range nsDef.Relation {
							for _, subject := range subjects.AsSlice() {
								objectRelation := &v0.RelationReference{
									Namespace: nsDef.Name,
									Relation:  relation.Name,
								}

								t.Run(fmt.Sprintf("lookup_%s_%s_to_%s_%s_%s", objectRelation.Namespace, objectRelation.Relation, subject.Namespace, subject.ObjectId, subject.Relation), func(t *testing.T) {
									vrequire := require.New(t)

									accessibleObjects := []string{}
									allObjectIds, ok := objectsPerNamespace.Get(nsDef.Name)
									if !ok {
										return
									}

									// Collect all accessible objects.
									for _, objectId := range allObjectIds {
										objectIdStr := objectId.(string)
										checkResp, err := srv.Check(context.Background(), &v0.CheckRequest{
											TestUserset: &v0.ObjectAndRelation{
												Namespace: nsDef.Name,
												Relation:  relation.Name,
												ObjectId:  objectIdStr,
											},
											User: &v0.User{
												UserOneof: &v0.User_Userset{
													Userset: subject,
												},
											},
											AtRevision: zookie.NewFromRevision(revision),
										})
										vrequire.NoError(err)
										if checkResp.IsMember {
											accessibleObjects = append(accessibleObjects, objectIdStr)
										}
									}

									// Perform a lookup call and ensure it returns the at least the same set of object IDs.
									result, err := srv.Lookup(context.Background(), &v0.LookupRequest{
										User:           subject,
										ObjectRelation: objectRelation,
										Limit:          uint32(len(accessibleObjects) + 100),
										AtRevision:     zookie.NewFromRevision(revision),
									})
									vrequire.NoError(err)

									sort.Strings(accessibleObjects)
									sort.Strings(result.ResolvedObjectIds)

									for _, accessibleObjectID := range accessibleObjects {
										vrequire.True(
											contains(result.ResolvedObjectIds, accessibleObjectID),
											"Object `%s` missing in lookup results for %s#%s@%s: Expected: %v. Found: %v",
											accessibleObjectID,
											nsDef.Name,
											relation.Name,
											tuple.StringONR(subject),
											accessibleObjects,
											result.ResolvedObjectIds,
										)
									}

									// Ensure that every returned object Checks.
									for _, resolvedObjectId := range result.ResolvedObjectIds {
										checkResp, err := srv.Check(context.Background(), &v0.CheckRequest{
											TestUserset: &v0.ObjectAndRelation{
												Namespace: nsDef.Name,
												Relation:  relation.Name,
												ObjectId:  resolvedObjectId,
											},
											User: &v0.User{
												UserOneof: &v0.User_Userset{
													Userset: subject,
												},
											},
											AtRevision: zookie.NewFromRevision(revision),
										})
										vrequire.NoError(err)
										vrequire.True(
											checkResp.IsMember,
											"Found Check failure for relation %s:%s#%s and subject %s",
											nsDef.Name,
											resolvedObjectId,
											relation.Name,
											tuple.StringONR(subject),
										)
									}
								})
							}
						}
					}
				})
			}
		})
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
