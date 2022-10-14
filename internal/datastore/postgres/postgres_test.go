//go:build ci && docker
// +build ci,docker

package postgres

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jackc/pgx/v4"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/internal/testfixtures"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/test"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestPostgresDatastore(t *testing.T) {
	// TODO remove this compatibility loop once the ID->XID migrations are all complete
	for _, config := range []struct {
		targetMigration string
		migrationPhase  string
	}{
		{"add-xid-columns", "write-both-read-old"},
		{"add-xid-constraints", "write-both-read-old"},
		{"add-xid-constraints", "write-both-read-new"},
		{"drop-id-constraints", "write-both-read-new"},
		{"drop-id-constraints", ""},
		{"drop-bigserial-ids", ""},
	} {
		t.Run(fmt.Sprintf("%s-%s", config.targetMigration, config.migrationPhase), func(t *testing.T) {
			t.Parallel()
			b := testdatastore.RunPostgresForTesting(t, "", config.targetMigration)

			test.All(t, test.DatastoreTesterFunc(func(revisionQuantization, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
				ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
					ds, err := NewPostgresDatastore(uri,
						RevisionQuantization(revisionQuantization),
						GCWindow(gcWindow),
						WatchBufferLength(watchBufferLength),
						DebugAnalyzeBeforeStatistics(),
						MigrationPhase(config.migrationPhase),
					)
					require.NoError(t, err)
					return ds
				})
				return ds, nil
			}))

			t.Run("WithSplit", func(t *testing.T) {
				// Set the split at a VERY small size, to ensure any WithUsersets queries are split.
				test.All(t, test.DatastoreTesterFunc(func(revisionQuantization, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
					ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
						ds, err := NewPostgresDatastore(uri,
							RevisionQuantization(revisionQuantization),
							GCWindow(gcWindow),
							WatchBufferLength(watchBufferLength),
							DebugAnalyzeBeforeStatistics(),
							SplitAtUsersetCount(1), // 1 userset
							MigrationPhase(config.migrationPhase),
						)
						require.NoError(t, err)
						return ds
					})

					return ds, nil
				}))
			})

			t.Run("GarbageCollection", createDatastoreTest(
				b,
				GarbageCollectionTest,
				RevisionQuantization(0),
				GCWindow(1*time.Millisecond),
				WatchBufferLength(1),
				MigrationPhase(config.migrationPhase),
			))

			t.Run("TransactionTimestamps", createDatastoreTest(
				b,
				TransactionTimestampsTest,
				RevisionQuantization(0),
				GCWindow(1*time.Millisecond),
				WatchBufferLength(1),
				MigrationPhase(config.migrationPhase),
			))

			t.Run("GarbageCollectionByTime", createDatastoreTest(
				b,
				GarbageCollectionByTimeTest,
				RevisionQuantization(0),
				GCWindow(1*time.Millisecond),
				WatchBufferLength(1),
				MigrationPhase(config.migrationPhase),
			))

			t.Run("ChunkedGarbageCollection", createDatastoreTest(
				b,
				ChunkedGarbageCollectionTest,
				RevisionQuantization(0),
				GCWindow(1*time.Millisecond),
				WatchBufferLength(1),
				MigrationPhase(config.migrationPhase),
			))

			t.Run("QuantizedRevisions", func(t *testing.T) {
				QuantizedRevisionTest(t, b)
			})
		})
	}

	// TODO remove this test once the ID->XID migrations are all complete
	t.Run("XIDMigrationAssumptionsTest", func(t *testing.T) {
		b := testdatastore.RunPostgresForTesting(t, "", "")
		XIDMigrationAssumptionsTest(t, b)
	})
}

type datastoreTestFunc func(t *testing.T, ds datastore.Datastore)

func createDatastoreTest(b testdatastore.RunningEngineForTest, tf datastoreTestFunc, options ...Option) func(*testing.T) {
	return func(t *testing.T) {
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewPostgresDatastore(uri, options...)
			require.NoError(t, err)
			return ds
		})
		defer ds.Close()

		tf(t, ds)
	}
}

func GarbageCollectionTest(t *testing.T, ds datastore.Datastore) {
	require := require.New(t)

	ctx := context.Background()
	ok, err := ds.IsReady(ctx)
	require.NoError(err)
	require.True(ok)

	writtenAt, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		// Write basic namespaces.
		return rwt.WriteNamespaces(namespace.Namespace(
			"resource",
			namespace.Relation("reader", nil),
		), namespace.Namespace("user"))
	})
	require.NoError(err)

	// Run GC at the transaction and ensure no relationships are removed.
	pds := ds.(*pgDatastore)

	removed, err := pds.DeleteBeforeTx(ctx, writtenAt)
	require.NoError(err)
	require.Zero(removed.Relationships)
	require.Zero(removed.Namespaces)

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
	require.NoError(err)

	// Run GC to remove the old namespace
	removed, err = pds.DeleteBeforeTx(ctx, writtenAt)
	require.NoError(err)
	require.Zero(removed.Relationships)
	require.Equal(int64(1), removed.Transactions)
	require.Equal(int64(2), removed.Namespaces)

	// Write a relationship.
	tpl := tuple.Parse("resource:someresource#reader@user:someuser#...")

	relWrittenAt, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, tpl)
	require.NoError(err)

	// Run GC at the transaction and ensure no relationships are removed, but 1 transaction (the previous write namespace) is.
	removed, err = pds.DeleteBeforeTx(ctx, relWrittenAt)
	require.NoError(err)
	require.Zero(removed.Relationships)
	require.Equal(int64(1), removed.Transactions)
	require.Zero(removed.Namespaces)

	// Run GC again and ensure there are no changes.
	removed, err = pds.DeleteBeforeTx(ctx, relWrittenAt)
	require.NoError(err)
	require.Zero(removed.Relationships)
	require.Zero(removed.Transactions)
	require.Zero(removed.Namespaces)

	// Ensure the relationship is still present.
	tRequire := testfixtures.TupleChecker{Require: require, DS: ds}
	tRequire.TupleExists(ctx, tpl, relWrittenAt)

	// Overwrite the relationship.
	relOverwrittenAt, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, tpl)
	require.NoError(err)

	// Run GC at the transaction and ensure the (older copy of the) relationship is removed, as well as 1 transaction (the write).
	removed, err = pds.DeleteBeforeTx(ctx, relOverwrittenAt)
	require.NoError(err)
	require.Equal(int64(1), removed.Relationships)
	require.Equal(int64(1), removed.Transactions)
	require.Zero(removed.Namespaces)

	// Run GC again and ensure there are no changes.
	removed, err = pds.DeleteBeforeTx(ctx, relOverwrittenAt)
	require.NoError(err)
	require.Zero(removed.Relationships)
	require.Zero(removed.Transactions)
	require.Zero(removed.Namespaces)

	// Ensure the relationship is still present.
	tRequire.TupleExists(ctx, tpl, relOverwrittenAt)

	// Delete the relationship.
	relDeletedAt, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_DELETE, tpl)
	require.NoError(err)

	// Ensure the relationship is gone.
	tRequire.NoTupleExists(ctx, tpl, relDeletedAt)

	// Run GC at the transaction and ensure the relationship is removed, as well as 1 transaction (the overwrite).
	removed, err = pds.DeleteBeforeTx(ctx, relDeletedAt)
	require.NoError(err)
	require.Equal(int64(1), removed.Relationships)
	require.Equal(int64(1), removed.Transactions)
	require.Zero(removed.Namespaces)

	// Run GC again and ensure there are no changes.
	removed, err = pds.DeleteBeforeTx(ctx, relDeletedAt)
	require.NoError(err)
	require.Zero(removed.Relationships)
	require.Zero(removed.Transactions)
	require.Zero(removed.Namespaces)

	// Write a the relationship a few times.
	var relLastWriteAt datastore.Revision
	for i := 0; i < 3; i++ {
		var err error
		relLastWriteAt, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, tpl)
		require.NoError(err)
	}

	// Run GC at the transaction and ensure the older copies of the relationships are removed,
	// as well as the 2 older write transactions and the older delete transaction.
	removed, err = pds.DeleteBeforeTx(ctx, relLastWriteAt)
	require.NoError(err)
	require.Equal(int64(2), removed.Relationships)
	require.Equal(int64(3), removed.Transactions)
	require.Zero(removed.Namespaces)

	// Ensure the relationship is still present.
	tRequire.TupleExists(ctx, tpl, relLastWriteAt)
}

func TransactionTimestampsTest(t *testing.T, ds datastore.Datastore) {
	require := require.New(t)

	ctx := context.Background()
	ok, err := ds.IsReady(ctx)
	require.NoError(err)
	require.True(ok)

	// Setting db default time zone to before UTC
	pgd := ds.(*pgDatastore)
	_, err = pgd.dbpool.Exec(ctx, "SET TIME ZONE 'America/New_York';")
	require.NoError(err)

	// Get timestamp in UTC as reference
	startTimeUTC, err := pgd.Now(ctx)
	require.NoError(err)

	// Transaction timestamp should not be stored in system time zone
	tx, err := pgd.dbpool.Begin(ctx)
	require.NoError(err)

	txXID, err := createNewTransaction(ctx, tx)
	require.NoError(err)

	err = tx.Commit(ctx)
	require.NoError(err)

	var ts time.Time
	sql, args, err := psql.Select("timestamp").From(tableTransaction).Where(sq.Eq{"xid": txXID}).ToSql()
	require.NoError(err)
	err = pgd.dbpool.QueryRow(
		datastore.SeparateContextWithTracing(ctx), sql, args...,
	).Scan(&ts)
	require.NoError(err)

	// Transaction timestamp will be before the reference time if it was stored
	// in the default time zone and reinterpreted
	require.True(startTimeUTC.Before(ts))
}

func GarbageCollectionByTimeTest(t *testing.T, ds datastore.Datastore) {
	require := require.New(t)

	ctx := context.Background()
	ok, err := ds.IsReady(ctx)
	require.NoError(err)
	require.True(ok)

	// Write basic namespaces.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(namespace.Namespace(
			"resource",
			namespace.Relation("reader", nil),
		), namespace.Namespace("user"))
	})
	require.NoError(err)

	pds := ds.(*pgDatastore)

	// Sleep 1ms to ensure GC will delete the previous transaction.
	time.Sleep(1 * time.Millisecond)

	// Write a relationship.
	tpl := tuple.Parse("resource:someresource#reader@user:someuser#...")
	relLastWriteAt, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, tpl)
	require.NoError(err)

	// Run GC and ensure only transactions were removed.
	afterWrite, err := pds.Now(ctx)
	require.NoError(err)

	afterWriteTx, err := pds.TxIDBefore(ctx, afterWrite)
	require.NoError(err)

	removed, err := pds.DeleteBeforeTx(ctx, afterWriteTx)
	require.NoError(err)
	require.Zero(removed.Relationships)
	require.True(removed.Transactions > 0)
	require.Zero(removed.Namespaces)

	// Ensure the relationship is still present.
	tRequire := testfixtures.TupleChecker{Require: require, DS: ds}
	tRequire.TupleExists(ctx, tpl, relLastWriteAt)

	// Sleep 1ms to ensure GC will delete the previous write.
	time.Sleep(1 * time.Millisecond)

	// Delete the relationship.
	relDeletedAt, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_DELETE, tpl)
	require.NoError(err)

	// Run GC and ensure the relationship is removed.
	afterDelete, err := pds.Now(ctx)
	require.NoError(err)

	afterDeleteTx, err := pds.TxIDBefore(ctx, afterDelete)
	require.NoError(err)

	removed, err = pds.DeleteBeforeTx(ctx, afterDeleteTx)
	require.NoError(err)
	require.Equal(int64(1), removed.Relationships)
	require.Equal(int64(1), removed.Transactions)
	require.Zero(removed.Namespaces)

	// Ensure the relationship is still not present.
	tRequire.NoTupleExists(ctx, tpl, relDeletedAt)
}

const chunkRelationshipCount = 2000

func ChunkedGarbageCollectionTest(t *testing.T, ds datastore.Datastore) {
	require := require.New(t)

	ctx := context.Background()
	ok, err := ds.IsReady(ctx)
	require.NoError(err)
	require.True(ok)

	// Write basic namespaces.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(namespace.Namespace(
			"resource",
			namespace.Relation("reader", nil),
		), namespace.Namespace("user"))
	})
	require.NoError(err)

	pds := ds.(*pgDatastore)

	// Prepare relationships to write.
	var tpls []*core.RelationTuple
	for i := 0; i < chunkRelationshipCount; i++ {
		tpl := tuple.Parse(fmt.Sprintf("resource:resource-%d#reader@user:someuser#...", i))
		tpls = append(tpls, tpl)
	}

	// Write a large number of relationships.
	writtenAt, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, tpls...)
	require.NoError(err)

	// Ensure the relationships were written.
	tRequire := testfixtures.TupleChecker{Require: require, DS: ds}
	for _, tpl := range tpls {
		tRequire.TupleExists(ctx, tpl, writtenAt)
	}

	// Run GC and ensure only transactions were removed.
	afterWrite, err := pds.Now(ctx)
	require.NoError(err)

	afterWriteTx, err := pds.TxIDBefore(ctx, afterWrite)
	require.NoError(err)

	removed, err := pds.DeleteBeforeTx(ctx, afterWriteTx)
	require.NoError(err)
	require.Zero(removed.Relationships)
	require.True(removed.Transactions > 0)
	require.Zero(removed.Namespaces)

	// Sleep to ensure the relationships will GC.
	time.Sleep(1 * time.Millisecond)

	// Delete all the relationships.
	deletedAt, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_DELETE, tpls...)
	require.NoError(err)

	// Ensure the relationships were deleted.
	for _, tpl := range tpls {
		tRequire.NoTupleExists(ctx, tpl, deletedAt)
	}

	// Sleep to ensure GC.
	time.Sleep(1 * time.Millisecond)

	// Run GC and ensure all the stale relationships are removed.
	afterDelete, err := pds.Now(ctx)
	require.NoError(err)

	afterDeleteTx, err := pds.TxIDBefore(ctx, afterDelete)
	require.NoError(err)

	removed, err = pds.DeleteBeforeTx(ctx, afterDeleteTx)
	require.NoError(err)
	require.Equal(int64(chunkRelationshipCount), removed.Relationships)
	require.Equal(int64(1), removed.Transactions)
	require.Zero(removed.Namespaces)
}

func QuantizedRevisionTest(t *testing.T, b testdatastore.RunningEngineForTest) {
	testCases := []struct {
		testName      string
		quantization  time.Duration
		relativeTimes []time.Duration
		numLower      uint64
		numHigher     uint64
	}{
		{
			"DefaultRevision",
			1 * time.Second,
			[]time.Duration{},
			0, 0,
		},
		{
			"OnlyPastRevisions",
			1 * time.Second,
			[]time.Duration{-2 * time.Second},
			1, 0,
		},
		{
			"OnlyFutureRevisions",
			1 * time.Second,
			[]time.Duration{2 * time.Second},
			1, 0,
		},
		{
			"QuantizedLower",
			1 * time.Second,
			[]time.Duration{-2 * time.Second, -1 * time.Nanosecond, 0},
			2, 1,
		},
		{
			"QuantizationDisabled",
			1 * time.Nanosecond,
			[]time.Duration{-2 * time.Second, -1 * time.Nanosecond, 0},
			3, 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			require := require.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var conn *pgx.Conn
			ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
				var err error
				conn, err = pgx.Connect(ctx, uri)
				require.NoError(err)

				ds, err := NewPostgresDatastore(
					uri,
					RevisionQuantization(5*time.Second),
					GCWindow(24*time.Hour),
					WatchBufferLength(1),
				)
				require.NoError(err)

				return ds
			})
			defer ds.Close()

			// set a random time zone to ensure the queries are unaffect by tz
			_, err := conn.Exec(ctx, fmt.Sprintf("SET TIME ZONE -%d", rand.Intn(8)+1))
			require.NoError(err)

			var dbNow time.Time
			err = conn.QueryRow(ctx, "SELECT (NOW() AT TIME ZONE 'utc')").Scan(&dbNow)
			require.NoError(err)

			if len(tc.relativeTimes) > 0 {
				psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
				insertTxn := psql.Insert(tableTransaction).Columns(colTimestamp)

				for _, offset := range tc.relativeTimes {
					sql, args, err := insertTxn.Values(dbNow.Add(offset)).ToSql()
					require.NoError(err)

					_, err = conn.Exec(ctx, sql, args...)
					require.NoError(err)
				}
			}

			queryRevision := fmt.Sprintf(
				querySelectRevision,
				colXID,
				tableTransaction,
				colTimestamp,
				tc.quantization.Nanoseconds(),
			)

			var revision xid8
			var validFor time.Duration
			err = conn.QueryRow(ctx, queryRevision).Scan(&revision, &validFor)
			require.NoError(err)

			queryFmt := "SELECT COUNT(%[1]s) FROM %[2]s WHERE %[1]s %[3]s $1;"
			numLowerQuery := fmt.Sprintf(queryFmt, colXID, tableTransaction, "<")
			numHigherQuery := fmt.Sprintf(queryFmt, colXID, tableTransaction, ">")

			var numLower, numHigher uint64
			require.NoError(conn.QueryRow(ctx, numLowerQuery, revision).Scan(&numLower))
			require.NoError(conn.QueryRow(ctx, numHigherQuery, revision).Scan(&numHigher))

			require.Equal(tc.numLower, numLower)
			require.Equal(tc.numHigher, numHigher)
		})
	}
}

func XIDMigrationAssumptionsTest(t *testing.T, b testdatastore.RunningEngineForTest) {
	ctx, cancel := context.WithTimeout(
		context.WithValue(context.Background(), migrate.BackfillBatchSize, uint64(1000)),
		30*time.Second,
	)
	defer cancel()

	require := require.New(t)

	uri := b.NewDatabase(t)

	migrationDriver, err := migrations.NewAlembicPostgresDriver(uri)
	require.NoError(err)
	require.NoError(migrations.DatabaseMigrations.Run(
		ctx,
		migrationDriver,
		"add-ns-config-id",
		migrate.LiveRun,
	))

	conn, err := pgx.Connect(ctx, uri)
	require.NoError(err)

	// Insert some rows using the old format
	var oldTxIDs []uint64
	for i := 0; i < 20; i++ {
		sql := fmt.Sprintf("INSERT INTO %s DEFAULT VALUES RETURNING id", tableTransaction)

		var newTx uint64
		require.NoError(conn.QueryRow(ctx, sql).Scan(&newTx))

		oldTxIDs = append(oldTxIDs, newTx)
	}

	insertNSSQL, insertNSArgs, err := psql.
		Insert(tableNamespace).
		Columns(colNamespace, colConfig, "created_transaction", "deleted_transaction").
		Values("one_namespace", "", oldTxIDs[0], oldTxIDs[1]).
		Values("one_namespace", "", oldTxIDs[1], liveDeletedTxnID).ToSql()

	require.NoError(err)

	_, err = conn.Exec(ctx, insertNSSQL, insertNSArgs...)
	require.NoError(err)

	insertRelsSQL, insertRelsArgs, err := psql.
		Insert(tableTuple).
		Columns(
			colNamespace,
			colObjectID,
			colRelation,
			colUsersetNamespace,
			colUsersetObjectID,
			colUsersetRelation,
			"created_transaction",
			"deleted_transaction",
		).
		Values("one_namespace", "1", "parent", "one_namespace", "2", datastore.Ellipsis, oldTxIDs[0], oldTxIDs[1]).
		Values("one_namespace", "1", "parent", "one_namespace", "2", datastore.Ellipsis, oldTxIDs[1], liveDeletedTxnID).
		ToSql()
	require.NoError(err)

	_, err = conn.Exec(ctx, insertRelsSQL, insertRelsArgs...)
	require.NoError(err)

	// Migrate to the version containing both old and new
	require.NoError(migrations.DatabaseMigrations.Run(
		ctx,
		migrationDriver,
		"add-xid-columns",
		migrate.LiveRun,
	))

	dsWriteBothReadOld, err := NewPostgresDatastore(
		uri,
		RevisionQuantization(0),
		MigrationPhase("write-both-read-old"),
	)
	require.NoError(err)

	writtenAtTwo, err := dsWriteBothReadOld.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		require.NoError(rwt.WriteNamespaces(namespace.Namespace(
			"two_namespace", namespace.Relation("parent", nil, nil))))

		require.NoError(rwt.WriteRelationships([]*core.RelationTupleUpdate{
			{
				Operation: core.RelationTupleUpdate_TOUCH,
				Tuple:     tuple.MustParse("one_namespace:1#parent@one_namespace:2#..."),
			},
			{
				Operation: core.RelationTupleUpdate_TOUCH,
				Tuple:     tuple.MustParse("two_namespace:1#parent@two_namespace:2#..."),
			},
		}))

		return nil
	})
	require.NoError(err)
	writtenAtOne := decimal.NewFromInt(int64(oldTxIDs[len(oldTxIDs)-1]))
	require.True(writtenAtTwo.GreaterThan(writtenAtOne))

	verifyProperAlive(ctx, require, dsWriteBothReadOld, writtenAtTwo, map[string]int{
		"one_namespace": 1,
		"two_namespace": 1,
	})

	// Backfill
	for _, migrationName := range []string{"backfill-xid-add-indices", "add-xid-constraints"} {
		require.NoError(migrations.DatabaseMigrations.Run(
			ctx,
			migrationDriver,
			migrationName,
			migrate.LiveRun,
		))

		lastWritten, err := dsWriteBothReadOld.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			require.NoError(rwt.WriteNamespaces(namespace.Namespace(
				"two_namespace", namespace.Relation("parent", nil, nil))))

			require.NoError(rwt.WriteRelationships([]*core.RelationTupleUpdate{
				{
					Operation: core.RelationTupleUpdate_TOUCH,
					Tuple:     tuple.MustParse("two_namespace:1#parent@two_namespace:2#..."),
				},
			}))

			return nil
		})
		require.NoError(err)

		// Use the existing datastore to check data again
		verifyProperAlive(ctx, require, dsWriteBothReadOld, lastWritten, map[string]int{
			"one_namespace": 1,
			"two_namespace": 1,
		})
	}

	dsWriteBothReadNew, err := NewPostgresDatastore(
		uri,
		RevisionQuantization(0),
		MigrationPhase("write-both-read-new"),
	)
	require.NoError(err)

	writtenAtThree, err := dsWriteBothReadNew.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		require.NoError(rwt.DeleteRelationships(&v1.RelationshipFilter{
			ResourceType: "one_namespace",
		}))

		require.NoError(rwt.DeleteNamespace("one_namespace"))

		require.NoError(rwt.WriteNamespaces(namespace.Namespace(
			"three_namespace", namespace.Relation("parent", nil, nil))))

		require.NoError(rwt.WriteRelationships([]*core.RelationTupleUpdate{
			{
				Operation: core.RelationTupleUpdate_TOUCH,
				Tuple:     tuple.MustParse("three_namespace:1#parent@three_namespace:2#..."),
			},
		}))

		return nil
	})
	require.NoError(err)
	require.True(writtenAtThree.GreaterThan(writtenAtTwo))

	verifyProperAlive(ctx, require, dsWriteBothReadNew, writtenAtThree, map[string]int{
		"one_namespace":   0,
		"two_namespace":   1,
		"three_namespace": 1,
	})

	// Drop the requirement to write old
	require.NoError(migrations.DatabaseMigrations.Run(
		ctx,
		migrationDriver,
		"drop-id-constraints",
		migrate.LiveRun,
	))

	// Use the old datastore to verify again
	verifyProperAlive(ctx, require, dsWriteBothReadNew, writtenAtThree, map[string]int{
		"one_namespace":   0,
		"two_namespace":   1,
		"three_namespace": 1,
	})

	dsWriteOnlyNew, err := NewPostgresDatastore(
		uri,
		RevisionQuantization(0),
	)
	require.NoError(err)

	writtenAtFour, err := dsWriteOnlyNew.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		require.NoError(rwt.DeleteRelationships(&v1.RelationshipFilter{
			ResourceType: "two_namespace",
		}))

		require.NoError(rwt.DeleteNamespace("two_namespace"))

		require.NoError(rwt.WriteNamespaces(namespace.Namespace(
			"four_namespace", namespace.Relation("parent", nil, nil))))

		require.NoError(rwt.WriteRelationships([]*core.RelationTupleUpdate{
			{
				Operation: core.RelationTupleUpdate_TOUCH,
				Tuple:     tuple.MustParse("four_namespace:1#parent@four_namespace:2#..."),
			},
		}))

		return nil
	})
	require.NoError(err)
	require.True(writtenAtFour.GreaterThan(writtenAtThree))

	verifyProperAlive(ctx, require, dsWriteOnlyNew, writtenAtFour, map[string]int{
		"one_namespace":   0,
		"two_namespace":   0,
		"three_namespace": 1,
		"four_namespace":  1,
	})

	// Drop the old columns entirely
	require.NoError(migrations.DatabaseMigrations.Run(
		ctx,
		migrationDriver,
		"drop-bigserial-ids",
		migrate.LiveRun,
	))

	// Use the existing datastore to verify
	verifyProperAlive(ctx, require, dsWriteOnlyNew, writtenAtFour, map[string]int{
		"one_namespace":   0,
		"two_namespace":   0,
		"three_namespace": 1,
		"four_namespace":  1,
	})

	// Attempt a write with the columns dropped
	writtenAtFive, err := dsWriteOnlyNew.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		require.NoError(rwt.DeleteRelationships(&v1.RelationshipFilter{
			ResourceType: "three_namespace",
		}))

		require.NoError(rwt.DeleteNamespace("three_namespace"))

		require.NoError(rwt.WriteNamespaces(namespace.Namespace(
			"five_namespace", namespace.Relation("parent", nil, nil))))

		require.NoError(rwt.WriteRelationships([]*core.RelationTupleUpdate{
			{
				Operation: core.RelationTupleUpdate_TOUCH,
				Tuple:     tuple.MustParse("five_namespace:1#parent@five_namespace:2#..."),
			},
		}))

		return nil
	})
	require.NoError(err)
	require.True(writtenAtFive.GreaterThan(writtenAtFour))

	// Check everything at the end state
	verifyProperAlive(ctx, require, dsWriteBothReadNew, writtenAtOne, map[string]int{
		"one_namespace": 1,
	})

	verifyProperAlive(ctx, require, dsWriteBothReadNew, writtenAtTwo, map[string]int{
		"one_namespace": 1,
		"two_namespace": 1,
	})

	verifyProperAlive(ctx, require, dsWriteBothReadNew, writtenAtThree, map[string]int{
		"one_namespace":   0,
		"two_namespace":   1,
		"three_namespace": 1,
	})

	verifyProperAlive(ctx, require, dsWriteOnlyNew, writtenAtFour, map[string]int{
		"one_namespace":   0,
		"two_namespace":   0,
		"three_namespace": 1,
		"four_namespace":  1,
	})

	verifyProperAlive(ctx, require, dsWriteOnlyNew, writtenAtFive, map[string]int{
		"one_namespace":   0,
		"two_namespace":   0,
		"three_namespace": 0,
		"four_namespace":  1,
		"five_namespace":  1,
	})
}

func countIterator(require *require.Assertions, iter datastore.RelationshipIterator) int {
	defer iter.Close()
	var count int
	for found := iter.Next(); found != nil; found = iter.Next() {
		count++
	}
	require.NoError(iter.Err())
	return count
}

func verifyProperAlive(
	ctx context.Context,
	require *require.Assertions,
	ds datastore.Datastore,
	revision datastore.Revision,
	expected map[string]int,
) {
	reader := ds.SnapshotReader(revision)

	for nsName, expectedCount := range expected {
		_, _, err := reader.ReadNamespace(ctx, nsName)
		if expectedCount == 0 {
			require.Error(err)
		} else {
			require.NoError(err)
		}

		found, err := reader.LookupNamespaces(ctx, []string{nsName})
		require.NoError(err)
		require.Equal(expectedCount, len(found))

		iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
			ResourceType: nsName,
		})
		require.NoError(err)
		require.Equal(expectedCount, countIterator(require, iter), "expected tuple count not equal for namespace: %s", nsName)
	}
}

func BenchmarkPostgresQuery(b *testing.B) {
	req := require.New(b)

	ds := testdatastore.RunPostgresForTesting(b, "", migrate.Head).NewDatastore(b, func(engine, uri string) datastore.Datastore {
		ds, err := NewPostgresDatastore(uri,
			RevisionQuantization(0),
			GCWindow(time.Millisecond*1),
			WatchBufferLength(1),
		)
		require.NoError(b, err)
		return ds
	})
	defer ds.Close()
	ds, revision := testfixtures.StandardDatastoreWithData(ds, req)

	b.Run("benchmark checks", func(b *testing.B) {
		require := require.New(b)

		for i := 0; i < b.N; i++ {
			iter, err := ds.SnapshotReader(revision).QueryRelationships(context.Background(), datastore.RelationshipsFilter{
				ResourceType: testfixtures.DocumentNS.Name,
			})
			require.NoError(err)

			defer iter.Close()

			for tpl := iter.Next(); tpl != nil; tpl = iter.Next() {
				require.Equal(testfixtures.DocumentNS.Name, tpl.ResourceAndRelation.Namespace)
			}
			require.NoError(iter.Err())
		}
	})
}
