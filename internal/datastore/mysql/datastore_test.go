//go:build ci
// +build ci

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	"github.com/authzed/spicedb/internal/testfixtures"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/test"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/namespace"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	chunkRelationshipCount = 2000
)

type datastoreTester struct {
	b      testdatastore.RunningEngineForTest
	t      *testing.T
	prefix string
}

func (dst *datastoreTester) createDatastore(revisionQuantization, gcWindow time.Duration, _ uint16) (datastore.Datastore, error) {
	ds := dst.b.NewDatastore(dst.t, func(engine, uri string) datastore.Datastore {
		ds, err := NewMySQLDatastore(uri,
			RevisionQuantization(revisionQuantization),
			GCWindow(gcWindow),
			GCInterval(0*time.Second),
			TablePrefix(dst.prefix),
			DebugAnalyzeBeforeStatistics(),
			OverrideLockWaitTimeout(1),
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
	RevisionQuantization(0 * time.Millisecond),
	GCWindow(1 * time.Millisecond),
	GCInterval(0 * time.Second),
	DebugAnalyzeBeforeStatistics(),
	OverrideLockWaitTimeout(1),
}

type datastoreTestFunc func(t *testing.T, ds datastore.Datastore)

func createDatastoreTest(b testdatastore.RunningEngineForTest, tf datastoreTestFunc, options ...Option) func(*testing.T) {
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
	b := testdatastore.RunMySQLForTesting(t, "")
	dst := datastoreTester{b: b, t: t}
	test.All(t, test.DatastoreTesterFunc(dst.createDatastore))

	t.Run("DatabaseSeeding", createDatastoreTest(b, DatabaseSeedingTest))
	t.Run("PrometheusCollector", createDatastoreTest(
		b,
		PrometheusCollectorTest,
		WithEnablePrometheusStats(true),
	))
	t.Run("GarbageCollection", createDatastoreTest(b, GarbageCollectionTest, defaultOptions...))
	t.Run("GarbageCollectionByTime", createDatastoreTest(b, GarbageCollectionByTimeTest, defaultOptions...))
	t.Run("ChunkedGarbageCollection", createDatastoreTest(b, ChunkedGarbageCollectionTest, defaultOptions...))
	t.Run("TransactionTimestamps", createDatastoreTest(b, TransactionTimestampsTest, defaultOptions...))
	t.Run("QuantizedRevisions", func(t *testing.T) {
		QuantizedRevisionTest(t, b)
	})
}

func TestMySQLDatastoreWithTablePrefix(t *testing.T) {
	b := testdatastore.RunMySQLForTestingWithOptions(t, testdatastore.MySQLTesterOptions{MigrateForNewDatastore: true, Prefix: "spicedb_"}, "")
	dst := datastoreTester{b: b, t: t, prefix: "spicedb_"}
	test.All(t, test.DatastoreTesterFunc(dst.createDatastore))
}

func DatabaseSeedingTest(t *testing.T, ds datastore.Datastore) {
	req := require.New(t)

	// ensure datastore is seeded right after initialization
	ctx := context.Background()
	isSeeded, err := ds.(*Datastore).isSeeded(ctx)
	req.NoError(err)
	req.True(isSeeded, "expected datastore to be seeded after initialization")

	ready, err := ds.IsReady(ctx)
	req.NoError(err)
	req.True(ready)
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
	writtenAt, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(
			namespace.Namespace(
				"resource",
				namespace.Relation("reader", nil),
			),
			namespace.Namespace("user"),
		)
	})
	req.NoError(err)

	// Run GC at the transaction and ensure no relationships are removed.
	mds := ds.(*Datastore)

	removed, err := mds.DeleteBeforeTx(ctx, uint64(writtenAt.IntPart()))
	req.NoError(err)
	req.Zero(removed.Relationships)
	req.Zero(removed.Namespaces)

	// Replace the namespace with a new one.
	writtenAt, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(
			namespace.Namespace(
				"resource",
				namespace.Relation("reader", nil),
				namespace.Relation("unused", nil),
			),
			namespace.Namespace("user"),
		)
	})
	req.NoError(err)

	// Run GC to remove the old namespace
	removed, err = mds.DeleteBeforeTx(ctx, uint64(writtenAt.IntPart()))
	req.NoError(err)
	req.Zero(removed.Relationships)
	req.Equal(int64(1), removed.Transactions)
	req.Equal(int64(2), removed.Namespaces)

	// Write a relationship.

	tpl := tuple.Parse("resource:someresource#reader@user:someuser#...")
	relWrittenAt, err := common.WriteTuples(ctx, ds, corev1.RelationTupleUpdate_CREATE, tpl)
	req.NoError(err)

	// Run GC at the transaction and ensure no relationships are removed, but 1 transaction (the previous write namespace) is.
	removed, err = mds.DeleteBeforeTx(ctx, uint64(relWrittenAt.IntPart()))
	req.NoError(err)
	req.Zero(removed.Relationships)
	req.Equal(int64(1), removed.Transactions)
	req.Zero(removed.Namespaces)

	// Run GC again and ensure there are no changes.
	removed, err = mds.DeleteBeforeTx(ctx, uint64(relWrittenAt.IntPart()))
	req.NoError(err)
	req.Zero(removed.Relationships)
	req.Zero(removed.Transactions)
	req.Zero(removed.Namespaces)

	// Ensure the relationship is still present.
	tRequire := testfixtures.TupleChecker{Require: req, DS: ds}
	tRequire.TupleExists(ctx, tpl, relWrittenAt)

	// Overwrite the relationship.
	relOverwrittenAt, err := common.WriteTuples(ctx, ds, corev1.RelationTupleUpdate_TOUCH, tpl)
	req.NoError(err)

	// Run GC at the transaction and ensure the (older copy of the) relationship is removed, as well as 1 transaction (the write).
	removed, err = mds.DeleteBeforeTx(ctx, uint64(relOverwrittenAt.IntPart()))
	req.NoError(err)
	req.Equal(int64(1), removed.Relationships)
	req.Equal(int64(1), removed.Transactions)
	req.Zero(removed.Namespaces)

	// Run GC again and ensure there are no changes.
	removed, err = mds.DeleteBeforeTx(ctx, uint64(relOverwrittenAt.IntPart()))
	req.NoError(err)
	req.Zero(removed.Relationships)
	req.Zero(removed.Transactions)
	req.Zero(removed.Namespaces)

	// Ensure the relationship is still present.
	tRequire.TupleExists(ctx, tpl, relOverwrittenAt)

	// Delete the relationship.
	relDeletedAt, err := common.WriteTuples(ctx, ds, corev1.RelationTupleUpdate_DELETE, tpl)
	req.NoError(err)

	// Ensure the relationship is gone.
	tRequire.NoTupleExists(ctx, tpl, relDeletedAt)

	// Run GC at the transaction and ensure the relationship is removed, as well as 1 transaction (the overwrite).
	removed, err = mds.DeleteBeforeTx(ctx, uint64(relDeletedAt.IntPart()))
	req.NoError(err)
	req.Equal(int64(1), removed.Relationships)
	req.Equal(int64(1), removed.Transactions)
	req.Zero(removed.Namespaces)

	// Run GC again and ensure there are no changes.
	removed, err = mds.DeleteBeforeTx(ctx, uint64(relDeletedAt.IntPart()))
	req.NoError(err)
	req.Zero(removed.Relationships)
	req.Zero(removed.Transactions)
	req.Zero(removed.Namespaces)

	// Write the relationship a few times.
	_, err = common.WriteTuples(ctx, ds, corev1.RelationTupleUpdate_TOUCH, tpl)
	req.NoError(err)

	_, err = common.WriteTuples(ctx, ds, corev1.RelationTupleUpdate_TOUCH, tpl)
	req.NoError(err)

	relLastWriteAt, err := common.WriteTuples(ctx, ds, corev1.RelationTupleUpdate_TOUCH, tpl)
	req.NoError(err)

	// Run GC at the transaction and ensure the older copies of the relationships are removed,
	// as well as the 2 older write transactions and the older delete transaction.
	removed, err = mds.DeleteBeforeTx(ctx, uint64(relLastWriteAt.IntPart()))
	req.NoError(err)
	req.Equal(int64(2), removed.Relationships)
	req.Equal(int64(3), removed.Transactions)
	req.Zero(removed.Namespaces)

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
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(
			namespace.Namespace(
				"resource",
				namespace.Relation("reader", nil),
			),
			namespace.Namespace("user"),
		)
	})
	req.NoError(err)

	mds := ds.(*Datastore)

	// Sleep 1ms to ensure GC will delete the previous transaction.
	time.Sleep(1 * time.Millisecond)

	// Write a relationship.
	tpl := tuple.Parse("resource:someresource#reader@user:someuser#...")

	relLastWriteAt, err := common.WriteTuples(ctx, ds, corev1.RelationTupleUpdate_CREATE, tpl)
	req.NoError(err)

	// Run GC and ensure only transactions were removed.
	afterWrite, err := mds.Now(ctx)
	req.NoError(err)

	afterWriteTx, err := mds.TxIDBefore(ctx, afterWrite)
	req.NoError(err)

	removed, err := mds.DeleteBeforeTx(ctx, afterWriteTx)
	req.NoError(err)
	req.Zero(removed.Relationships)
	req.NotZero(removed.Transactions)
	req.Zero(removed.Namespaces)

	// Ensure the relationship is still present.
	tRequire := testfixtures.TupleChecker{Require: req, DS: ds}
	tRequire.TupleExists(ctx, tpl, relLastWriteAt)

	// Sleep 1ms to ensure GC will delete the previous write.
	time.Sleep(1 * time.Millisecond)

	// Delete the relationship.
	relDeletedAt, err := common.WriteTuples(ctx, ds, corev1.RelationTupleUpdate_DELETE, tpl)
	req.NoError(err)

	// Run GC and ensure the relationship is removed.
	afterDelete, err := mds.Now(ctx)
	req.NoError(err)

	afterDeleteTx, err := mds.TxIDBefore(ctx, afterDelete)
	req.NoError(err)

	removed, err = mds.DeleteBeforeTx(ctx, afterDeleteTx)
	req.NoError(err)
	req.Equal(int64(1), removed.Relationships)
	req.Equal(int64(1), removed.Transactions)
	req.Zero(removed.Namespaces)

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
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(
			namespace.Namespace(
				"resource",
				namespace.Relation("reader", nil),
			),
			namespace.Namespace("user"),
		)
	})
	req.NoError(err)

	mds := ds.(*Datastore)

	// Prepare relationships to write.
	var tuples []*corev1.RelationTuple
	for i := 0; i < chunkRelationshipCount; i++ {
		tpl := tuple.Parse(fmt.Sprintf("resource:resource-%d#reader@user:someuser#...", i))
		tuples = append(tuples, tpl)
	}

	// Write a large number of relationships.

	writtenAt, err := common.WriteTuples(ctx, ds, corev1.RelationTupleUpdate_CREATE, tuples...)
	req.NoError(err)

	// Ensure the relationships were written.
	tRequire := testfixtures.TupleChecker{Require: req, DS: ds}
	for _, tpl := range tuples {
		tRequire.TupleExists(ctx, tpl, writtenAt)
	}

	// Run GC and ensure only transactions were removed.
	afterWrite, err := mds.Now(ctx)
	req.NoError(err)

	afterWriteTx, err := mds.TxIDBefore(ctx, afterWrite)
	req.NoError(err)

	removed, err := mds.DeleteBeforeTx(ctx, afterWriteTx)
	req.NoError(err)
	req.Zero(removed.Relationships)
	req.NotZero(removed.Transactions)
	req.Zero(removed.Namespaces)

	// Sleep to ensure the relationships will GC.
	time.Sleep(1 * time.Millisecond)

	// Delete all the relationships.
	deletedAt, err := common.WriteTuples(ctx, ds, corev1.RelationTupleUpdate_DELETE, tuples...)
	req.NoError(err)

	// Ensure the relationships were deleted.
	for _, tpl := range tuples {
		tRequire.NoTupleExists(ctx, tpl, deletedAt)
	}

	// Sleep to ensure GC.
	time.Sleep(1 * time.Millisecond)

	// Run GC and ensure all the stale relationships are removed.
	afterDelete, err := mds.Now(ctx)
	req.NoError(err)

	afterDeleteTx, err := mds.TxIDBefore(ctx, afterDelete)
	req.NoError(err)

	removed, err = mds.DeleteBeforeTx(ctx, afterDeleteTx)
	req.NoError(err)
	req.Equal(int64(chunkRelationshipCount), removed.Relationships)
	req.Equal(int64(1), removed.Transactions)
	req.Zero(removed.Namespaces)
}

func QuantizedRevisionTest(t *testing.T, b testdatastore.RunningEngineForTest) {
	testCases := []struct {
		testName         string
		quantization     time.Duration
		relativeTimes    []time.Duration
		expectedRevision uint64
	}{
		{
			"DefaultRevision",
			1 * time.Second,
			[]time.Duration{},
			1,
		},
		{
			"OnlyPastRevisions",
			1 * time.Second,
			[]time.Duration{-2 * time.Second},
			2,
		},
		{
			"OnlyFutureRevisions",
			1 * time.Second,
			[]time.Duration{2 * time.Second},
			2,
		},
		{
			"QuantizedLower",
			1 * time.Second,
			[]time.Duration{-2 * time.Second, -1 * time.Nanosecond, 0},
			3,
		},
		{
			"QuantizationDisabled",
			1 * time.Nanosecond,
			[]time.Duration{-2 * time.Second, -1 * time.Nanosecond, 0},
			4,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			require := require.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
				ds, err := NewMySQLDatastore(
					uri,
					RevisionQuantization(5*time.Second),
					GCWindow(24*time.Hour),
					WatchBufferLength(1),
				)
				require.NoError(err)
				return ds
			})
			mds := ds.(*Datastore)

			dbNow, err := mds.Now(ctx)
			require.NoError(err)

			tx, err := mds.db.BeginTx(ctx, nil)
			require.NoError(err)

			if len(tc.relativeTimes) > 0 {
				bulkWrite := sb.Insert(mds.driver.RelationTupleTransaction()).Columns(colTimestamp)

				for _, offset := range tc.relativeTimes {
					bulkWrite = bulkWrite.Values(dbNow.Add(offset))
				}

				sql, args, err := bulkWrite.ToSql()
				require.NoError(err)

				_, err = tx.ExecContext(ctx, sql, args...)
				require.NoError(err)
			}

			queryRevision := fmt.Sprintf(
				querySelectRevision,
				colID,
				mds.driver.RelationTupleTransaction(),
				colTimestamp,
				tc.quantization.Nanoseconds(),
			)

			var revision uint64
			var validFor time.Duration
			err = tx.QueryRowContext(ctx, queryRevision).Scan(&revision, &validFor)
			require.NoError(err)
			require.Greater(validFor, time.Duration(0))
			require.LessOrEqual(validFor, tc.quantization.Nanoseconds())
			require.Equal(tc.expectedRevision, revision)
		})
	}
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
	startTimeUTC, err := ds.(*Datastore).Now(ctx)
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

	// Let's make sure both Now() and transactionCreated() have timezones aligned
	req.True(ts.Sub(startTimeUTC) < 5*time.Minute)

	revision, err := ds.OptimizedRevision(ctx)
	req.NoError(err)
	req.Equal(revisionFromTransaction(txID), revision)
}

func TestMySQLMigrations(t *testing.T) {
	req := require.New(t)

	db := datastoreDB(t, false)
	migrationDriver := migrations.NewMySQLDriverFromDB(db, "")

	version, err := migrationDriver.Version(context.Background())
	req.NoError(err)
	req.Equal("", version)

	err = migrations.Manager.Run(context.Background(), migrationDriver, migrate.Head, migrate.LiveRun)
	req.NoError(err)

	version, err = migrationDriver.Version(context.Background())
	req.NoError(err)

	headVersion, err := migrations.Manager.HeadRevision()
	req.NoError(err)
	req.Equal(headVersion, version)
}

func TestMySQLMigrationsWithPrefix(t *testing.T) {
	req := require.New(t)

	prefix := "spicedb_"
	db := datastoreDB(t, false)
	migrationDriver := migrations.NewMySQLDriverFromDB(db, prefix)

	version, err := migrationDriver.Version(context.Background())
	req.NoError(err)
	req.Equal("", version)

	err = migrations.Manager.Run(context.Background(), migrationDriver, migrate.Head, migrate.LiveRun)
	req.NoError(err)

	version, err = migrationDriver.Version(context.Background())
	req.NoError(err)

	headVersion, err := migrations.Manager.HeadRevision()
	req.NoError(err)
	req.Equal(headVersion, version)

	rows, err := db.Query("SHOW TABLES;")
	req.NoError(err)

	for rows.Next() {
		var tbl string
		req.NoError(rows.Scan(&tbl))
		req.Contains(tbl, prefix)
	}
	req.NoError(rows.Err())
}

func datastoreDB(t *testing.T, migrate bool) *sql.DB {
	var databaseURI string
	testdatastore.RunMySQLForTestingWithOptions(t, testdatastore.MySQLTesterOptions{MigrateForNewDatastore: migrate}, "").NewDatastore(t, func(engine, uri string) datastore.Datastore {
		databaseURI = uri
		return nil
	})

	db, err := sql.Open("mysql", databaseURI)
	require.NoError(t, err)
	return db
}
