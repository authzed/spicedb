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
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/namespace"
	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
	"github.com/authzed/spicedb/pkg/zookie"
	"github.com/stretchr/testify/require"
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
					require := require.New(t)

					ds, err := memdb.NewMemdbDatastore(0, delta, memdb.DisableGC, 0)
					require.NoError(err)

					fullyResolved, revision, err := validationfile.PopulateFromFiles(ds, []string{filePath})
					require.NoError(err)

					ns, err := namespace.NewCachingNamespaceManager(ds, 1*time.Second, nil)
					require.NoError(err)

					dispatch, err := graph.NewLocalDispatcher(ns, ds)
					require.NoError(err)

					srv := NewACLServer(ds, ns, dispatch, 50)

					// Validate the type system for each namespace.
					for _, nsDef := range fullyResolved.NamespaceDefinitions {
						_, ts, _, err := ns.ReadNamespaceAndTypes(context.Background(), nsDef.Name)
						require.NoError(err)

						err = ts.Validate(context.Background())
						require.NoError(err)
					}

					// Build the list of tuples per namespace.
					tuplesPerNamespace := slicemultimap.New()
					for _, tpl := range fullyResolved.Tuples {
						tuplesPerNamespace.Put(tpl.ObjectAndRelation.Namespace, tpl)
					}

					// Read all tuples defined in the namespaces.
					for _, nsDef := range fullyResolved.NamespaceDefinitions {
						result, err := srv.Read(context.Background(), &api.ReadRequest{
							Tuplesets: []*api.RelationTupleFilter{
								&api.RelationTupleFilter{Namespace: nsDef.Name},
							},
							AtRevision: zookie.NewFromRevision(revision),
						})
						require.NoError(err)

						expected, _ := tuplesPerNamespace.Get(nsDef.Name)
						require.Equal(len(expected), len(result.Tuplesets[0].Tuples))
					}

					// Call a write on each tuple to make sure it type checks.
					for _, nsDef := range fullyResolved.NamespaceDefinitions {
						tuples, ok := tuplesPerNamespace.Get(nsDef.Name)
						if !ok {
							continue
						}

						for _, itpl := range tuples {
							tpl := itpl.(*api.RelationTuple)
							_, err = srv.Write(context.Background(), &api.WriteRequest{
								WriteConditions: []*api.RelationTuple{tpl},
								Updates:         []*api.RelationTupleUpdate{tuple.Touch(tpl)},
							})
							require.NoError(err)
						}
					}

					// Collect the set of objects and subjects.
					objectsPerNamespace := setmultimap.New()
					subjects := namespace.NewONRSet()
					for _, tpl := range fullyResolved.Tuples {
						objectsPerNamespace.Put(tpl.ObjectAndRelation.Namespace, tpl.ObjectAndRelation.ObjectId)

						switch m := tpl.User.UserOneof.(type) {
						case *api.User_Userset:
							subjects.Add(m.Userset)
						}
					}

					// For each relation in each namespace, for each user, collect the objects accessible
					// to that user and then verify the lookup returns the same set of objects.
					for _, nsDef := range fullyResolved.NamespaceDefinitions {
						for _, relation := range nsDef.Relation {
							for _, subject := range subjects.AsSlice() {
								accessibleObjects := []string{}
								allObjectIds, ok := objectsPerNamespace.Get(nsDef.Name)
								if !ok {
									continue
								}

								// Collect all accessible objects.
								for _, objectId := range allObjectIds {
									objectIdStr := objectId.(string)
									checkResp, err := srv.Check(context.Background(), &api.CheckRequest{
										TestUserset: &api.ObjectAndRelation{
											Namespace: nsDef.Name,
											Relation:  relation.Name,
											ObjectId:  objectIdStr,
										},
										User: &api.User{
											UserOneof: &api.User_Userset{
												Userset: subject,
											},
										},
										AtRevision: zookie.NewFromRevision(revision),
									})
									require.NoError(err)
									if checkResp.IsMember {
										accessibleObjects = append(accessibleObjects, objectIdStr)
									}
								}

								// Perform a lookup call and ensure it returns the at least the same set of object IDs.
								result, err := srv.Lookup(context.Background(), &api.LookupRequest{
									User: subject,
									ObjectRelation: &api.RelationReference{
										Namespace: nsDef.Name,
										Relation:  relation.Name,
									},
									Limit:      uint32(len(accessibleObjects) + 100),
									AtRevision: zookie.NewFromRevision(revision),
								})
								require.NoError(err)

								sort.Strings(accessibleObjects)
								sort.Strings(result.ResolvedObjectIds)

								for _, accessibleObjectID := range accessibleObjects {
									require.True(
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
									// TODO: REMOVE ME ONCE WE FIGURE OUT THE #manager
									if subject.Relation != "..." {
										continue
									}

									checkResp, err := srv.Check(context.Background(), &api.CheckRequest{
										TestUserset: &api.ObjectAndRelation{
											Namespace: nsDef.Name,
											Relation:  relation.Name,
											ObjectId:  resolvedObjectId,
										},
										User: &api.User{
											UserOneof: &api.User_Userset{
												Userset: subject,
											},
										},
										AtRevision: zookie.NewFromRevision(revision),
									})
									require.NoError(err)
									require.True(
										checkResp.IsMember,
										"Found Check failure for relation %s:%s#%s and subject %s",
										nsDef.Name,
										resolvedObjectId,
										relation.Name,
										tuple.StringONR(subject),
									)
								}
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
