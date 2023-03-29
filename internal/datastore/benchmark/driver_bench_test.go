//go:build ci && docker
// +build ci,docker

package benchmark

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/crdb"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/mysql"
	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/testfixtures"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	numDocuments = 1000
	usersPerDoc  = 5

	revisionQuantization = 5 * time.Second
	gcWindow             = 2 * time.Hour
	gcInterval           = 1 * time.Hour
	watchBufferLength    = 1000
)

func BenchmarkDatastoreDriver(b *testing.B) {
	drivers := []struct {
		name     string
		initFunc func(*testing.B) datastore.Datastore
	}{
		// Spanner is excluded because performance testing a simulator doesn't make sense
		{"postgres", initPostgres},
		{"crdb-overlap-static", buildCRDBInit("static")},
		{"crdb-overlap-insecure", buildCRDBInit("insecure")},
		{"mysql", initMySQL},
		{"memdb", initMemdb},
	}

	for _, driver := range drivers {
		b.Run(driver.name, func(b *testing.B) {
			ctx := context.Background()
			ds := driver.initFunc(b)

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
						iter.Close()
						require.NoError(b, iter.Err())
						require.Equal(b, usersPerDoc, count)
					}
				})
				b.Run("Touch", buildTupleTest(ctx, ds, core.RelationTupleUpdate_TOUCH))
				b.Run("Create", buildTupleTest(ctx, ds, core.RelationTupleUpdate_CREATE))
			})
		})
	}
}

func initPostgres(b *testing.B) datastore.Datastore {
	builder := testdatastore.RunPostgresForTesting(b, "", "head")
	ds := builder.NewDatastore(b, func(engine, uri string) datastore.Datastore {
		ds, err := postgres.NewPostgresDatastore(uri,
			postgres.RevisionQuantization(revisionQuantization),
			postgres.GCWindow(gcWindow),
			postgres.GCInterval(gcInterval),
			postgres.WatchBufferLength(watchBufferLength),
		)
		require.NoError(b, err)
		return ds
	})
	return ds
}

func buildCRDBInit(overlapStrategy string) func(*testing.B) datastore.Datastore {
	return func(b *testing.B) datastore.Datastore {
		builder := testdatastore.RunCRDBForTesting(b, "")
		ds := builder.NewDatastore(b, func(engine, uri string) datastore.Datastore {
			ds, err := crdb.NewCRDBDatastore(
				uri,
				crdb.RevisionQuantization(revisionQuantization),
				crdb.GCWindow(gcWindow),
				crdb.WatchBufferLength(watchBufferLength),

				crdb.OverlapStrategy(overlapStrategy),
			)
			require.NoError(b, err)
			return ds
		})
		return ds
	}
}

func initMySQL(b *testing.B) datastore.Datastore {
	builder := testdatastore.RunMySQLForTesting(b, "")
	ds := builder.NewDatastore(b, func(engine, uri string) datastore.Datastore {
		ds, err := mysql.NewMySQLDatastore(uri,
			mysql.RevisionQuantization(revisionQuantization),
			mysql.GCWindow(gcWindow),
			mysql.GCInterval(gcInterval),
			mysql.WatchBufferLength(watchBufferLength),

			mysql.OverrideLockWaitTimeout(1),
		)
		require.NoError(b, err)
		return ds
	})
	return ds
}

func initMemdb(b *testing.B) datastore.Datastore {
	ds, err := memdb.NewMemdbDatastore(watchBufferLength, revisionQuantization, gcWindow)
	require.NoError(b, err)
	return ds
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
