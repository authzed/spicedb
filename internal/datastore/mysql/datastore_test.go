//go:build ci
// +build ci

package mysql

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	"github.com/authzed/spicedb/internal/datastore/test"
	"github.com/authzed/spicedb/internal/testfixtures"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/namespace"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	chunkRelationshipCount = 2000
)

type datastoreTester struct {
	b      testdatastore.TestDatastoreBuilder
	t      *testing.T
	prefix string
}

func (dst *datastoreTester) createDatastore(revisionFuzzingTimedelta, gcWindow time.Duration, _ uint16) (datastore.Datastore, error) {
	ds := dst.b.NewDatastore(dst.t, func(engine, uri string) datastore.Datastore {
		ds, err := NewMySQLDatastore(uri,
			RevisionFuzzingTimedelta(revisionFuzzingTimedelta),
			GCWindow(gcWindow),
			GCInterval(0*time.Second),
			TablePrefix(dst.prefix),
			DebugAnalyzeBeforeStatistics(),
		)
		require.NoError(dst.t, err)
		return ds
	})
	_, err := ds.IsReady(context.Background())
	require.NoError(dst.t, err)
	return ds, nil
}

func failOnError(t *testing.T, f func() error) {
	require.NoError(t, f())
}

var defaultOptions = []Option{
	RevisionFuzzingTimedelta(0 * time.Millisecond),
	GCWindow(1 * time.Millisecond),
	GCInterval(0 * time.Second),
	DebugAnalyzeBeforeStatistics(),
}

type datastoreTestFunc func(t *testing.T, ds datastore.Datastore)

func createDatastoreTest(b testdatastore.TestDatastoreBuilder, tf datastoreTestFunc, options ...Option) func(*testing.T) {
	return func(t *testing.T) {
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewMySQLDatastore(uri, options...)
			require.NoError(t, err)
			return ds
		})
		defer failOnError(t, ds.Close)

		tf(t, ds)
	}
}

func TestMySQLDatastore(t *testing.T) {
	b := testdatastore.NewMySQLBuilder(t)
	dst := datastoreTester{b: b, t: t}
	test.All(t, test.DatastoreTesterFunc(dst.createDatastore))

	t.Run("IsReady", createDatastoreTest(b, IsReadyTest))
	t.Run("IsReadyRace", createDatastoreTest(b, IsReadyRaceTest))
	t.Run("PrometheusCollector", createDatastoreTest(
		b,
		PrometheusCollectorTest,
		EnablePrometheusStats(),
	))
	t.Run("GarbageCollection", createDatastoreTest(b, GarbageCollectionTest, defaultOptions...))
	t.Run("GarbageCollectionByTime", createDatastoreTest(b, GarbageCollectionByTimeTest, defaultOptions...))
	t.Run("ChunkedGarbageCollection", createDatastoreTest(b, ChunkedGarbageCollectionTest, defaultOptions...))
	t.Run("TransactionTimestamps", createDatastoreTest(b, TransactionTimestampsTest, defaultOptions...))
}

func TestMySQLDatastoreWithTablePrefix(t *testing.T) {
	b := testdatastore.NewMySQLBuilderWithOptions(t, testdatastore.MySQLBuilderOptions{Migrate: true, Prefix: "spicedb_"})
	dst := datastoreTester{b: b, t: t, prefix: "spicedb_"}
	test.All(t, test.DatastoreTesterFunc(dst.createDatastore))
}

func IsReadyTest(t *testing.T, ds datastore.Datastore) {
	req := require.New(t)

	// ensure no revision is seeded by default
	ctx := context.Background()
	revision, err := ds.HeadRevision(ctx)
	req.Equal(datastore.NoRevision, revision)
	req.NoError(err)

	ready, err := ds.IsReady(ctx)
	req.NoError(err)
	req.True(ready)

	// verify IsReady seeds the revision is if not present
	revision, err = ds.HeadRevision(ctx)
	req.NoError(err)
	req.Equal(revisionFromTransaction(1), revision)
}

func IsReadyRaceTest(t *testing.T, ds datastore.Datastore) {
	req := require.New(t)

	ctx := context.Background()
	revision, err := ds.HeadRevision(ctx)
	req.Equal(datastore.NoRevision, revision)
	req.NoError(err)

	var wg sync.WaitGroup

	concurrency := 5
	for gn := 1; gn <= concurrency; gn++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			ready, err := ds.IsReady(ctx)
			req.NoError(err, "goroutine %d", i)
			req.True(ready, "goroutine %d", i)
		}(gn)
	}
	wg.Wait()

	// verify IsReady seeds the revision is if not present
	revision, err = ds.HeadRevision(ctx)
	req.NoError(err)
	req.Equal(revisionFromTransaction(1), revision)
}

func PrometheusCollectorTest(t *testing.T, ds datastore.Datastore) {
	req := require.New(t)

	// cause some use of the SQL connection pool to generate metrics
	_, err := ds.IsReady(context.Background())
	req.NoError(err)

	metrics, err := prometheus.DefaultGatherer.Gather()
	req.NoError(err, metrics)
	var collectorStatsFound, connectorStatsFound bool
	for _, metric := range metrics {
		if metric.GetName() == "go_sql_stats_connections_open" {
			collectorStatsFound = true
		}
		if metric.GetName() == "spicedb_datastore_mysql_connect_count_total" {
			connectorStatsFound = true
		}
	}
	req.True(collectorStatsFound, "mysql datastore did not issue prometheus metrics")
	req.True(connectorStatsFound, "mysql datastore connector did not issue prometheus metrics")
}

func GarbageCollectionTest(t *testing.T, ds datastore.Datastore) {
	req := require.New(t)

	ctx := context.Background()
	ok, err := ds.IsReady(ctx)
	req.NoError(err)
	req.True(ok)

	// Write basic namespaces.
	_, err = ds.WriteNamespace(ctx, namespace.Namespace(
		"resource",
		namespace.Relation("reader", nil),
	))
	req.NoError(err)

	writtenAt, err := ds.WriteNamespace(ctx, namespace.Namespace("user"))
	req.NoError(err)

	// Run GC at the transaction and ensure no relationships are removed.
	mds := ds.(*Datastore)

	relsDeleted, _, err := mds.collectGarbageForTransaction(ctx, uint64(writtenAt.IntPart()))
	req.Zero(relsDeleted)
	req.NoError(err)

	// Write a relationship.

	tpl := &corev1.RelationTuple{
		ObjectAndRelation: &corev1.ObjectAndRelation{
			Namespace: "resource",
			ObjectId:  "someresource",
			Relation:  "reader",
		},
		User: &corev1.User{UserOneof: &corev1.User_Userset{Userset: &corev1.ObjectAndRelation{
			Namespace: "user",
			ObjectId:  "someuser",
			Relation:  "...",
		}}},
	}
	relationship := tuple.ToRelationship(tpl)

	relWrittenAt, err := ds.WriteTuples(
		ctx,
		nil,
		[]*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
			Relationship: relationship,
		}},
	)
	req.NoError(err)

	// Run GC at the transaction and ensure no relationships are removed, but 1 transaction (the previous write namespace) is.
	relsDeleted, transactionsDeleted, err := mds.collectGarbageForTransaction(ctx, uint64(relWrittenAt.IntPart()))
	req.Zero(relsDeleted)
	req.Equal(int64(1), transactionsDeleted)
	req.NoError(err)

	// Run GC again and ensure there are no changes.
	relsDeleted, transactionsDeleted, err = mds.collectGarbageForTransaction(ctx, uint64(relWrittenAt.IntPart()))
	req.Zero(relsDeleted)
	req.Zero(transactionsDeleted)
	req.NoError(err)

	// Ensure the relationship is still present.
	tRequire := testfixtures.TupleChecker{Require: req, DS: ds}
	tRequire.TupleExists(ctx, tpl, relWrittenAt)

	// Overwrite the relationship.
	relOverwrittenAt, err := ds.WriteTuples(
		ctx,
		nil,
		[]*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: relationship,
		}},
	)
	req.NoError(err)

	// Run GC at the transaction and ensure the (older copy of the) relationship is removed, as well as 1 transaction (the write).
	relsDeleted, transactionsDeleted, err = mds.collectGarbageForTransaction(ctx, uint64(relOverwrittenAt.IntPart()))
	req.Equal(int64(1), relsDeleted)
	req.Equal(int64(1), transactionsDeleted)
	req.NoError(err)

	// Run GC again and ensure there are no changes.
	relsDeleted, transactionsDeleted, err = mds.collectGarbageForTransaction(ctx, uint64(relOverwrittenAt.IntPart()))
	req.Zero(relsDeleted)
	req.Zero(transactionsDeleted)
	req.NoError(err)

	// Ensure the relationship is still present.
	tRequire.TupleExists(ctx, tpl, relOverwrittenAt)

	// Delete the relationship.
	relDeletedAt, err := ds.WriteTuples(
		ctx,
		nil,
		[]*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_DELETE,
			Relationship: relationship,
		}},
	)
	req.NoError(err)

	// Ensure the relationship is gone.
	tRequire.NoTupleExists(ctx, tpl, relDeletedAt)

	// Run GC at the transaction and ensure the relationship is removed, as well as 1 transaction (the overwrite).
	relsDeleted, transactionsDeleted, err = mds.collectGarbageForTransaction(ctx, uint64(relDeletedAt.IntPart()))
	req.Equal(int64(1), relsDeleted)
	req.Equal(int64(1), transactionsDeleted)
	req.NoError(err)

	// Run GC again and ensure there are no changes.
	relsDeleted, transactionsDeleted, err = mds.collectGarbageForTransaction(ctx, uint64(relDeletedAt.IntPart()))
	req.Zero(relsDeleted)
	req.Zero(transactionsDeleted)
	req.NoError(err)

	// Write the relationship a few times.
	_, err = ds.WriteTuples(
		ctx,
		nil,
		[]*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: relationship,
		}},
	)
	req.NoError(err)

	_, err = ds.WriteTuples(
		ctx,
		nil,
		[]*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: relationship,
		}},
	)
	req.NoError(err)

	relLastWriteAt, err := ds.WriteTuples(
		ctx,
		nil,
		[]*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: relationship,
		}},
	)
	req.NoError(err)

	// Run GC at the transaction and ensure the older copies of the relationships are removed,
	// as well as the 2 older write transactions and the older delete transaction.
	relsDeleted, transactionsDeleted, err = mds.collectGarbageForTransaction(ctx, uint64(relLastWriteAt.IntPart()))
	req.Equal(int64(2), relsDeleted)
	req.Equal(int64(3), transactionsDeleted)
	req.NoError(err)

	// Ensure the relationship is still present.
	tRequire.TupleExists(ctx, tpl, relLastWriteAt)
}

func GarbageCollectionByTimeTest(t *testing.T, ds datastore.Datastore) {
	req := require.New(t)

	ctx := context.Background()
	ok, err := ds.IsReady(ctx)
	req.NoError(err)
	req.True(ok)

	// Write basic namespaces.
	_, err = ds.WriteNamespace(ctx, namespace.Namespace(
		"resource",
		namespace.Relation("reader", nil),
	))
	req.NoError(err)

	_, err = ds.WriteNamespace(ctx, namespace.Namespace("user"))
	req.NoError(err)

	mds := ds.(*Datastore)

	// Sleep 1ms to ensure GC will delete the previous transaction.
	time.Sleep(1 * time.Millisecond)

	// Write a relationship.
	tpl := &corev1.RelationTuple{
		ObjectAndRelation: &corev1.ObjectAndRelation{
			Namespace: "resource",
			ObjectId:  "someresource",
			Relation:  "reader",
		},
		User: &corev1.User{UserOneof: &corev1.User_Userset{Userset: &corev1.ObjectAndRelation{
			Namespace: "user",
			ObjectId:  "someuser",
			Relation:  "...",
		}}},
	}
	relationship := tuple.ToRelationship(tpl)

	relLastWriteAt, err := ds.WriteTuples(
		ctx,
		nil,
		[]*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
			Relationship: relationship,
		}},
	)
	req.NoError(err)

	// Run GC and ensure only transactions were removed.
	afterWrite, err := mds.getNow(ctx)
	req.NoError(err)

	relsDeleted, transactionsDeleted, err := mds.collectGarbageBefore(ctx, afterWrite)
	req.Zero(relsDeleted)
	req.NotZero(transactionsDeleted)
	req.NoError(err)

	// Ensure the relationship is still present.
	tRequire := testfixtures.TupleChecker{Require: req, DS: ds}
	tRequire.TupleExists(ctx, tpl, relLastWriteAt)

	// Sleep 1ms to ensure GC will delete the previous write.
	time.Sleep(1 * time.Millisecond)

	// Delete the relationship.
	relDeletedAt, err := ds.WriteTuples(
		ctx,
		nil,
		[]*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_DELETE,
			Relationship: relationship,
		}},
	)
	req.NoError(err)

	// Run GC and ensure the relationship is removed.
	afterDelete, err := mds.getNow(ctx)
	req.NoError(err)

	relsDeleted, transactionsDeleted, err = mds.collectGarbageBefore(ctx, afterDelete)
	req.Equal(int64(1), relsDeleted)
	req.Equal(int64(1), transactionsDeleted)
	req.NoError(err)

	// Ensure the relationship is still not present.
	tRequire.NoTupleExists(ctx, tpl, relDeletedAt)
}

func ChunkedGarbageCollectionTest(t *testing.T, ds datastore.Datastore) {
	req := require.New(t)

	ctx := context.Background()
	ok, err := ds.IsReady(ctx)
	req.NoError(err)
	req.True(ok)

	// Write basic namespaces.
	_, err = ds.WriteNamespace(ctx, namespace.Namespace(
		"resource",
		namespace.Relation("reader", nil),
	))
	req.NoError(err)

	_, err = ds.WriteNamespace(ctx, namespace.Namespace("user"))
	req.NoError(err)

	mds := ds.(*Datastore)

	// Prepare relationships to write.
	var tuples []*corev1.RelationTuple
	for i := 0; i < chunkRelationshipCount; i++ {
		tpl := &corev1.RelationTuple{
			ObjectAndRelation: &corev1.ObjectAndRelation{
				Namespace: "resource",
				ObjectId:  fmt.Sprintf("resource-%d", i),
				Relation:  "reader",
			},
			User: &corev1.User{UserOneof: &corev1.User_Userset{Userset: &corev1.ObjectAndRelation{
				Namespace: "user",
				ObjectId:  "someuser",
				Relation:  "...",
			}}},
		}
		tuples = append(tuples, tpl)
	}

	// Write a large number of relationships.
	updates := make([]*v1.RelationshipUpdate, 0, len(tuples))
	for _, tpl := range tuples {
		relationship := tuple.ToRelationship(tpl)
		updates = append(updates, &v1.RelationshipUpdate{
			Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
			Relationship: relationship,
		})
	}

	writtenAt, err := ds.WriteTuples(
		ctx,
		nil,
		updates,
	)
	req.NoError(err)

	// Ensure the relationships were written.
	tRequire := testfixtures.TupleChecker{Require: req, DS: ds}
	for _, tpl := range tuples {
		tRequire.TupleExists(ctx, tpl, writtenAt)
	}

	// Run GC and ensure only transactions were removed.
	afterWrite, err := mds.getNow(ctx)
	req.NoError(err)

	relsDeleted, transactionsDeleted, err := mds.collectGarbageBefore(ctx, afterWrite)
	req.Zero(relsDeleted)
	req.NotZero(transactionsDeleted)
	req.NoError(err)

	// Sleep to ensure the relationships will GC.
	time.Sleep(1 * time.Millisecond)

	// Delete all the relationships.
	deletes := make([]*v1.RelationshipUpdate, 0, len(tuples))
	for _, tpl := range tuples {
		relationship := tuple.ToRelationship(tpl)
		deletes = append(deletes, &v1.RelationshipUpdate{
			Operation:    v1.RelationshipUpdate_OPERATION_DELETE,
			Relationship: relationship,
		})
	}

	deletedAt, err := ds.WriteTuples(
		ctx,
		nil,
		deletes,
	)
	req.NoError(err)

	// Ensure the relationships were deleted.
	for _, tpl := range tuples {
		tRequire.NoTupleExists(ctx, tpl, deletedAt)
	}

	// Sleep to ensure GC.
	time.Sleep(1 * time.Millisecond)

	// Run GC and ensure all the stale relationships are removed.
	afterDelete, err := mds.getNow(ctx)
	req.NoError(err)

	relsDeleted, transactionsDeleted, err = mds.collectGarbageBefore(ctx, afterDelete)
	req.Equal(int64(chunkRelationshipCount), relsDeleted)
	req.Equal(int64(1), transactionsDeleted)
	req.NoError(err)
}

// From https://dev.mysql.com/doc/refman/8.0/en/datetime.html
// By default, the current time zone for each connection is the server's time.
// The time zone can be set on a per-connection basis.
func TransactionTimestampsTest(t *testing.T, ds datastore.Datastore) {
	req := require.New(t)

	// Setting db default time zone to before UTC
	ctx := context.Background()
	db := ds.(*Datastore).db
	_, err := db.ExecContext(ctx, "SET GLOBAL time_zone = 'America/New_York';")
	req.NoError(err)

	ok, err := ds.IsReady(ctx)
	req.NoError(err)
	req.True(ok)

	// Get timestamp in UTC as reference
	startTimeUTC, err := ds.(*Datastore).getNow(ctx)
	req.NoError(err)

	// Transaction timestamp should not be stored in system time zone
	tx, err := db.BeginTx(ctx, nil)
	req.NoError(err)
	txID, err := ds.(*Datastore).createNewTransaction(ctx, tx)
	req.NoError(err)
	err = tx.Commit()
	req.NoError(err)

	var ts time.Time
	query, args, err := sb.Select(colTimestamp).From(ds.(*Datastore).driver.RelationTupleTransaction()).Where(sq.Eq{colID: txID}).ToSql()
	req.NoError(err)
	err = db.QueryRowContext(ctx, query, args...).Scan(&ts)
	req.NoError(err)

	// Let's make sure both getNow() and transactionCreated() have timezones aligned
	req.True(ts.Sub(startTimeUTC) < 5*time.Minute)

	revision, err := ds.OptimizedRevision(ctx)
	req.NoError(err)
	req.Equal(revisionFromTransaction(txID), revision)
}

func TestMySQLMigrations(t *testing.T) {
	req := require.New(t)
	ds := testdatastore.NewMySQLBuilderWithOptions(t, testdatastore.MySQLBuilderOptions{Migrate: false}).NewDatastore(t, func(engine, uri string) datastore.Datastore {
		ds, err := NewMySQLDatastore(uri)
		req.NoError(err)
		return ds
	})
	migrationDriver := ds.(*Datastore).driver

	version, err := migrationDriver.Version()
	req.NoError(err)
	req.Equal("", version)

	err = migrations.Manager.Run(migrationDriver, migrate.Head, migrate.LiveRun)
	req.NoError(err)

	version, err = migrationDriver.Version()
	req.NoError(err)

	headVersion, err := migrations.Manager.HeadRevision()
	req.NoError(err)
	req.Equal(headVersion, version)
}

func TestMySQLMigrationsWithPrefix(t *testing.T) {
	req := require.New(t)
	prefix := "spicedb_"
	ds := testdatastore.NewMySQLBuilderWithOptions(t, testdatastore.MySQLBuilderOptions{Migrate: false, Prefix: prefix}).
		NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewMySQLDatastore(uri, TablePrefix(prefix))
			req.NoError(err)
			return ds
		})
	migrationDriver := ds.(*Datastore).driver

	version, err := migrationDriver.Version()
	req.NoError(err)
	req.Equal("", version)

	err = migrations.Manager.Run(migrationDriver, migrate.Head, migrate.LiveRun)
	req.NoError(err)

	version, err = migrationDriver.Version()
	req.NoError(err)

	headVersion, err := migrations.Manager.HeadRevision()
	req.NoError(err)
	req.Equal(headVersion, version)

	db := ds.(*Datastore).db
	rows, err := db.Query("SHOW TABLES;")
	req.NoError(err)

	for rows.Next() {
		var tbl string
		req.NoError(rows.Scan(&tbl))
		req.Contains(tbl, prefix)
	}
	req.NoError(rows.Err())
}
