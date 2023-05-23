//go:build ci && docker
// +build ci,docker

package benchmark

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/crdb"
	"github.com/authzed/spicedb/internal/datastore/mysql"
	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/datastore/spanner"
	"github.com/authzed/spicedb/internal/testfixtures"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/internal/testserver/datastore/config"
	dsconfig "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	numDocuments = 1000
	usersPerDoc  = 5

	revisionQuantization = 5 * time.Second
	gcWindow             = 2 * time.Hour
	gcInterval           = 1 * time.Hour
	watchBufferLength    = 1000
)

var drivers = []struct {
	name        string
	suffix      string
	extraConfig []dsconfig.ConfigOption
}{
	{"memory", "", nil},
	{postgres.Engine, "", nil},
	{crdb.Engine, "-overlap-static", []dsconfig.ConfigOption{dsconfig.WithOverlapStrategy("static")}},
	{crdb.Engine, "-overlap-insecure", []dsconfig.ConfigOption{dsconfig.WithOverlapStrategy("insecure")}},
	{mysql.Engine, "", nil},
}

var skipped = []string{
	spanner.Engine, // Not useful to benchmark a simulator
}

var sortOrders = map[string]options.SortOrder{
	"ByResource": options.ByResource,
	"BySubject":  options.BySubject,
}

func BenchmarkDatastoreDriver(b *testing.B) {
	for _, driver := range drivers {
		b.Run(driver.name+driver.suffix, func(b *testing.B) {
			engine := testdatastore.RunDatastoreEngine(b, driver.name)
			ds := engine.NewDatastore(b, config.DatastoreConfigInitFunc(
				b,
				append(driver.extraConfig,
					dsconfig.WithRevisionQuantization(revisionQuantization),
					dsconfig.WithGCWindow(gcWindow),
					dsconfig.WithGCInterval(gcInterval),
					dsconfig.WithWatchBufferLength(watchBufferLength))...,
			))

			ctx := context.Background()

			// Write the standard schema
			ds, _ = testfixtures.StandardDatastoreWithSchema(ds, require.New(b))

			// Write a fair amount of data, much more than a functional test
			for docNum := 0; docNum < numDocuments; docNum++ {
				_, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
					var updates []*core.RelationTupleUpdate
					for userNum := 0; userNum < usersPerDoc; userNum++ {
						updates = append(updates, &core.RelationTupleUpdate{
							Operation: core.RelationTupleUpdate_CREATE,
							Tuple:     docViewer(strconv.Itoa(docNum), strconv.Itoa(userNum)),
						})
					}

					return rwt.WriteRelationships(ctx, updates)
				})
				require.NoError(b, err)
			}

			// Sleep to give the datastore time to stabilize after all the writes
			time.Sleep(1 * time.Second)

			headRev, err := ds.HeadRevision(ctx)
			require.NoError(b, err)

			b.Run("TestTuple", func(b *testing.B) {
				b.Run("SnapshotRead", func(b *testing.B) {
					for n := 0; n < b.N; n++ {
						randDocNum := rand.Intn(numDocuments)
						iter, err := ds.SnapshotReader(headRev).QueryRelationships(ctx, datastore.RelationshipsFilter{
							ResourceType:             testfixtures.DocumentNS.Name,
							OptionalResourceIds:      []string{strconv.Itoa(randDocNum)},
							OptionalResourceRelation: "viewer",
						})
						require.NoError(b, err)
						var count int
						for rel := iter.Next(); rel != nil; rel = iter.Next() {
							count++
						}
						require.NoError(b, iter.Err())
						iter.Close()
						require.Equal(b, usersPerDoc, count)
					}
				})
				b.Run("SortedSnapshotReadOnlyNamespace", func(b *testing.B) {
					for orderName, order := range sortOrders {
						order := order
						b.Run(orderName, func(b *testing.B) {
							for n := 0; n < b.N; n++ {
								iter, err := ds.SnapshotReader(headRev).QueryRelationships(ctx, datastore.RelationshipsFilter{
									ResourceType: testfixtures.DocumentNS.Name,
								}, options.WithSort(order))
								require.NoError(b, err)
								var count int
								for rel := iter.Next(); rel != nil; rel = iter.Next() {
									count++
								}
								require.NoError(b, iter.Err())
								iter.Close()
							}
						})
					}
				})
				b.Run("SortedSnapshotReadWithRelation", func(b *testing.B) {
					for orderName, order := range sortOrders {
						order := order
						b.Run(orderName, func(b *testing.B) {
							for n := 0; n < b.N; n++ {
								iter, err := ds.SnapshotReader(headRev).QueryRelationships(ctx, datastore.RelationshipsFilter{
									ResourceType:             testfixtures.DocumentNS.Name,
									OptionalResourceRelation: "viewer",
								}, options.WithSort(order))
								require.NoError(b, err)
								var count int
								for rel := iter.Next(); rel != nil; rel = iter.Next() {
									count++
								}
								require.NoError(b, iter.Err())
								iter.Close()
							}
						})
					}
				})
				b.Run("SortedSnapshotReadAllResourceFields", func(b *testing.B) {
					for orderName, order := range sortOrders {
						order := order
						b.Run(orderName, func(b *testing.B) {
							for n := 0; n < b.N; n++ {
								randDocNum := rand.Intn(numDocuments)
								iter, err := ds.SnapshotReader(headRev).QueryRelationships(ctx, datastore.RelationshipsFilter{
									ResourceType:             testfixtures.DocumentNS.Name,
									OptionalResourceIds:      []string{strconv.Itoa(randDocNum)},
									OptionalResourceRelation: "viewer",
								}, options.WithSort(order))
								require.NoError(b, err)
								var count int
								for rel := iter.Next(); rel != nil; rel = iter.Next() {
									count++
								}
								require.NoError(b, iter.Err())
								iter.Close()
							}
						})
					}
				})
				b.Run("SnapshotReverseRead", func(b *testing.B) {
					for n := 0; n < b.N; n++ {
						iter, err := ds.SnapshotReader(headRev).ReverseQueryRelationships(ctx, datastore.SubjectsFilter{
							SubjectType: testfixtures.UserNS.Name,
						}, options.WithSortForReverse(options.ByResource))
						require.NoError(b, err)
						var count int
						for rel := iter.Next(); rel != nil; rel = iter.Next() {
							count++
						}
						require.NoError(b, iter.Err())
						iter.Close()
					}
				})
				b.Run("Touch", buildTupleTest(ctx, ds, core.RelationTupleUpdate_TOUCH))
				b.Run("Create", buildTupleTest(ctx, ds, core.RelationTupleUpdate_CREATE))
				b.Run("CreateAndTouch", func(b *testing.B) {
					const totalRelationships = 1000
					for _, portionCreate := range []float64{0, 0.10, 0.25, 0.50, 1} {
						portionCreate := portionCreate
						b.Run(fmt.Sprintf("%v_", portionCreate), func(b *testing.B) {
							for n := 0; n < b.N; n++ {
								portionCreateIndex := int(math.Floor(portionCreate * totalRelationships))
								mutations := make([]*core.RelationTupleUpdate, 0, totalRelationships)
								for index := 0; index < totalRelationships; index++ {
									if index >= portionCreateIndex {
										stableID := fmt.Sprintf("id-%d", index)
										tpl := docViewer(stableID, stableID)
										mutations = append(mutations, tuple.Touch(tpl))
									} else {
										randomID := testfixtures.RandomObjectID(32)
										tpl := docViewer(randomID, randomID)
										mutations = append(mutations, tuple.Create(tpl))
									}
								}

								_, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
									return rwt.WriteRelationships(ctx, mutations)
								})
								require.NoError(b, err)
							}
						})
					}
				})
			})
		})
	}
}

func TestAllDriversBenchmarkedOrSkipped(t *testing.T) {
	notBenchmarked := make(map[string]struct{}, len(datastore.Engines))
	for _, name := range datastore.Engines {
		notBenchmarked[name] = struct{}{}
	}

	for _, driver := range drivers {
		delete(notBenchmarked, driver.name)
	}
	for _, skippedEngine := range skipped {
		delete(notBenchmarked, skippedEngine)
	}

	require.Empty(t, notBenchmarked)
}

func buildTupleTest(ctx context.Context, ds datastore.Datastore, op core.RelationTupleUpdate_Operation) func(b *testing.B) {
	return func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
				randomID := testfixtures.RandomObjectID(32)
				return rwt.WriteRelationships(ctx, []*core.RelationTupleUpdate{
					{
						Operation: op,
						Tuple:     docViewer(randomID, randomID),
					},
				})
			})
			require.NoError(b, err)
		}
	}
}

func docViewer(documentID, userID string) *core.RelationTuple {
	return &core.RelationTuple{
		ResourceAndRelation: &core.ObjectAndRelation{
			Namespace: testfixtures.DocumentNS.Name,
			ObjectId:  documentID,
			Relation:  "viewer",
		},
		Subject: &core.ObjectAndRelation{
			Namespace: testfixtures.UserNS.Name,
			ObjectId:  userID,
			Relation:  datastore.Ellipsis,
		},
	}
}
