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
				_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
					var updates []tuple.RelationshipUpdate
					for userNum := 0; userNum < usersPerDoc; userNum++ {
						updates = append(updates, tuple.Create(docViewer(strconv.Itoa(docNum), strconv.Itoa(userNum))))
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
							OptionalResourceType:     testfixtures.DocumentNS.Name,
							OptionalResourceIds:      []string{strconv.Itoa(randDocNum)},
							OptionalResourceRelation: "viewer",
						})
						require.NoError(b, err)
						var count int
						for _, err := range iter {
							require.NoError(b, err)
							count++
						}
						require.Equal(b, usersPerDoc, count)
					}
				})
				b.Run("SnapshotReadOnlyNamespace", func(b *testing.B) {
					for n := 0; n < b.N; n++ {
						iter, err := ds.SnapshotReader(headRev).QueryRelationships(ctx, datastore.RelationshipsFilter{
							OptionalResourceType: testfixtures.DocumentNS.Name,
						})
						require.NoError(b, err)
						var count int
						for _, err := range iter {
							require.NoError(b, err)
							count++
						}
					}
				})
				b.Run("SortedSnapshotReadOnlyNamespace", func(b *testing.B) {
					for orderName, order := range sortOrders {
						order := order
						b.Run(orderName, func(b *testing.B) {
							for n := 0; n < b.N; n++ {
								iter, err := ds.SnapshotReader(headRev).QueryRelationships(ctx, datastore.RelationshipsFilter{
									OptionalResourceType: testfixtures.DocumentNS.Name,
								}, options.WithSort(order))
								require.NoError(b, err)
								var count int
								for _, err := range iter {
									require.NoError(b, err)
									count++
								}
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
									OptionalResourceType:     testfixtures.DocumentNS.Name,
									OptionalResourceRelation: "viewer",
								}, options.WithSort(order))
								require.NoError(b, err)
								var count int
								for _, err := range iter {
									require.NoError(b, err)
									count++
								}
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
									OptionalResourceType:     testfixtures.DocumentNS.Name,
									OptionalResourceIds:      []string{strconv.Itoa(randDocNum)},
									OptionalResourceRelation: "viewer",
								}, options.WithSort(order))
								require.NoError(b, err)
								var count int
								for _, err := range iter {
									require.NoError(b, err)
									count++
								}
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
						for _, err := range iter {
							require.NoError(b, err)
							count++
						}
					}
				})
				b.Run("Touch", buildRelTest(ctx, ds, tuple.UpdateOperationTouch))
				b.Run("Create", buildRelTest(ctx, ds, tuple.UpdateOperationCreate))
				b.Run("CreateAndTouch", func(b *testing.B) {
					const totalRelationships = 1000
					for _, portionCreate := range []float64{0, 0.10, 0.25, 0.50, 1} {
						portionCreate := portionCreate
						b.Run(fmt.Sprintf("%v_", portionCreate), func(b *testing.B) {
							for n := 0; n < b.N; n++ {
								portionCreateIndex := int(math.Floor(portionCreate * totalRelationships))
								mutations := make([]tuple.RelationshipUpdate, 0, totalRelationships)
								for index := 0; index < totalRelationships; index++ {
									if index >= portionCreateIndex {
										stableID := fmt.Sprintf("id-%d", index)
										rel := docViewer(stableID, stableID)
										mutations = append(mutations, tuple.Touch(rel))
									} else {
										randomID := testfixtures.RandomObjectID(32)
										rel := docViewer(randomID, randomID)
										mutations = append(mutations, tuple.Create(rel))
									}
								}

								_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
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

func buildRelTest(ctx context.Context, ds datastore.Datastore, op tuple.UpdateOperation) func(b *testing.B) {
	return func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				randomID := testfixtures.RandomObjectID(32)
				return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
					{
						Operation:    op,
						Relationship: docViewer(randomID, randomID),
					},
				})
			})
			require.NoError(b, err)
		}
	}
}

func docViewer(documentID, userID string) tuple.Relationship {
	return tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{
			Resource: tuple.ObjectAndRelation{
				ObjectType: testfixtures.DocumentNS.Name,
				ObjectID:   documentID,
				Relation:   "viewer",
			},
			Subject: tuple.ObjectAndRelation{
				ObjectType: testfixtures.UserNS.Name,
				ObjectID:   userID,
				Relation:   datastore.Ellipsis,
			},
		},
	}
}
