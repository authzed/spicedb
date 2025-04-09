//go:build ci && docker
// +build ci,docker

package postgres

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/common"
	pgcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/postgres/schema"
	pgversion "github.com/authzed/spicedb/internal/datastore/postgres/version"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	"github.com/authzed/spicedb/internal/testfixtures"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/test"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/tuple"
)

const pgSerializationFailure = "40001"

const (
	veryLargeGCInterval = 90000 * time.Second
)

// Implement the interface for testing datastores
func (pgd *pgDatastore) ExampleRetryableError() error {
	return &pgconn.PgError{
		Code: pgSerializationFailure,
	}
}

type postgresTestConfig struct {
	targetMigration string
	migrationPhase  string
	pgVersion       string
	pgbouncer       bool
}

// the global OTel tracer is used everywhere, so we synchronize tests over a global test tracer
var (
	otelMutex         = sync.Mutex{}
	testTraceProvider *trace.TracerProvider
)

func init() {
	testTraceProvider = trace.NewTracerProvider(
		trace.WithSampler(trace.AlwaysSample()),
	)
	otel.SetTracerProvider(testTraceProvider)
}

func testPostgresDatastore(t *testing.T, config postgresTestConfig) {
	pgbouncerStr := ""
	if config.pgbouncer {
		pgbouncerStr = "pgbouncer-"
	}

	t.Run(fmt.Sprintf("%spostgres-%s-%s-%s-gc", pgbouncerStr, config.pgVersion, config.targetMigration, config.migrationPhase), func(t *testing.T) {
		b := testdatastore.RunPostgresForTesting(t, "", config.targetMigration, config.pgVersion, config.pgbouncer)
		ctx := context.Background()

		// NOTE: gc tests take exclusive locks, so they are run under non-parallel.
		test.OnlyGCTests(t, test.DatastoreTesterFunc(func(revisionQuantization, gcInterval, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
			ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
				ds, err := newPostgresDatastore(ctx, uri, primaryInstanceID,
					RevisionQuantization(revisionQuantization),
					GCWindow(gcWindow),
					GCInterval(gcInterval),
					WatchBufferLength(watchBufferLength),
					DebugAnalyzeBeforeStatistics(),
					MigrationPhase(config.migrationPhase),
					WithRevisionHeartbeat(false), // heartbeat revision messes with tests that assert over revisions
				)
				require.NoError(t, err)
				return ds
			})
			return ds, nil
		}), false)
	})

	t.Run(fmt.Sprintf("%spostgres-%s-%s-%s", pgbouncerStr, config.pgVersion, config.targetMigration, config.migrationPhase), func(t *testing.T) {
		t.Parallel()
		b := testdatastore.RunPostgresForTesting(t, "", config.targetMigration, config.pgVersion, config.pgbouncer)
		ctx := context.Background()

		test.AllWithExceptions(t, test.DatastoreTesterFunc(func(revisionQuantization, _, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
			ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
				ds, err := newPostgresDatastore(ctx, uri, primaryInstanceID,
					RevisionQuantization(revisionQuantization),
					GCWindow(gcWindow),
					GCInterval(veryLargeGCInterval),
					WatchBufferLength(watchBufferLength),
					DebugAnalyzeBeforeStatistics(),
					MigrationPhase(config.migrationPhase),
					WithRevisionHeartbeat(false), // heartbeat revision messes with tests that assert over revisions
				)
				require.NoError(t, err)
				return ds
			})
			return ds, nil
		}), test.WithCategories(test.GCCategory), false)

		t.Run("TransactionTimestamps", createDatastoreTest(
			b,
			TransactionTimestampsTest,
			RevisionQuantization(0),
			GCWindow(1*time.Millisecond),
			GCInterval(veryLargeGCInterval),
			WatchBufferLength(1),
			MigrationPhase(config.migrationPhase),
		))

		t.Run("ObjectData", createDatastoreTest(
			b,
			ObjectDataTest,
			RevisionQuantization(0),
			GCWindow(1*time.Millisecond),
			GCInterval(veryLargeGCInterval),
			WatchBufferLength(1),
			MigrationPhase(config.migrationPhase),
		))

		t.Run("QuantizedRevisions", func(t *testing.T) {
			QuantizedRevisionTest(t, b)
		})

		t.Run("OverlappingRevision", func(t *testing.T) {
			OverlappingRevisionTest(t, b)
		})

		t.Run("WatchNotEnabled", func(t *testing.T) {
			WatchNotEnabledTest(t, b, config.pgVersion)
		})

		t.Run("GCQueriesServedByExpectedIndexes", func(t *testing.T) {
			GCQueriesServedByExpectedIndexes(t, b, config.pgVersion)
		})

		if config.migrationPhase == "" {
			t.Run("RevisionInversion", createDatastoreTest(
				b,
				RevisionInversionTest,
				RevisionQuantization(0),
				GCWindow(1*time.Millisecond),
				GCInterval(veryLargeGCInterval),
				WatchBufferLength(1),
				MigrationPhase(config.migrationPhase),
			))

			t.Run("ConcurrentRevisionHead", createDatastoreTest(
				b,
				ConcurrentRevisionHeadTest,
				RevisionQuantization(0),
				GCWindow(1*time.Millisecond),
				GCInterval(veryLargeGCInterval),
				WatchBufferLength(1),
				MigrationPhase(config.migrationPhase),
			))

			t.Run("ConcurrentRevisionWatch", createDatastoreTest(
				b,
				ConcurrentRevisionWatchTest,
				RevisionQuantization(0),
				GCWindow(1*time.Millisecond),
				GCInterval(veryLargeGCInterval),
				WatchBufferLength(50),
				MigrationPhase(config.migrationPhase),
				WithRevisionHeartbeat(false),
			))

			t.Run("OverlappingRevisionWatch", createDatastoreTest(
				b,
				OverlappingRevisionWatchTest,
				RevisionQuantization(0),
				GCWindow(1*time.Millisecond),
				GCInterval(veryLargeGCInterval),
				WatchBufferLength(50),
				MigrationPhase(config.migrationPhase),
			))

			t.Run("RepairTransactionsTest", createDatastoreTest(
				b,
				RepairTransactionsTest,
				RevisionQuantization(0),
				GCWindow(1*time.Millisecond),
				GCInterval(veryLargeGCInterval),
				WatchBufferLength(1),
				MigrationPhase(config.migrationPhase),
			))

			t.Run("TestNullCaveatWatch", createDatastoreTest(
				b,
				NullCaveatWatchTest,
				RevisionQuantization(0),
				GCWindow(1*time.Millisecond),
				GCInterval(veryLargeGCInterval),
				WatchBufferLength(50),
				MigrationPhase(config.migrationPhase),
			))

			t.Run("TestRevisionTimestampAndTransactionID", createDatastoreTest(
				b,
				RevisionTimestampAndTransactionIDTest,
				RevisionQuantization(0),
				GCWindow(1*time.Millisecond),
				GCInterval(veryLargeGCInterval),
				WatchBufferLength(50),
				MigrationPhase(config.migrationPhase),
			))

			t.Run("TestContinuousCheckpointTest", createDatastoreTest(
				b,
				ContinuousCheckpointTest,
				RevisionQuantization(100*time.Millisecond),
				GCInterval(veryLargeGCInterval),
				WatchBufferLength(50),
				MigrationPhase(config.migrationPhase),
				WithRevisionHeartbeat(true),
			))

			t.Run("TestSerializationError", createDatastoreTest(
				b,
				SerializationErrorTest,
				RevisionQuantization(0),
				GCWindow(1*time.Millisecond),
				GCInterval(veryLargeGCInterval),
				WatchBufferLength(50),
				MigrationPhase(config.migrationPhase),
			))

			t.Run("TestStrictReadMode", createReplicaDatastoreTest(
				b,
				StrictReadModeTest,
				RevisionQuantization(0),
				GCWindow(1000*time.Second),
				GCInterval(veryLargeGCInterval),
				WatchBufferLength(50),
				MigrationPhase(config.migrationPhase),
			))

			t.Run("TestStrictReadModeFallback", createReplicaDatastoreTest(
				b,
				StrictReadModeFallbackTest,
				RevisionQuantization(0),
				GCWindow(1000*time.Second),
				GCInterval(veryLargeGCInterval),
				WatchBufferLength(50),
				MigrationPhase(config.migrationPhase),
			))

			t.Run("TestLocking", createMultiDatastoreTest(
				b,
				LockingTest,
				RevisionQuantization(0),
				GCWindow(1000*time.Second),
				GCInterval(veryLargeGCInterval),
				WatchBufferLength(50),
				MigrationPhase(config.migrationPhase),
			))
		}

		t.Run("OTelTracing", createDatastoreTest(
			b,
			OTelTracingTest,
			RevisionQuantization(0),
			GCWindow(1*time.Millisecond),
			GCInterval(veryLargeGCInterval),
			WatchBufferLength(1),
			MigrationPhase(config.migrationPhase),
		))
	})
}

func testPostgresDatastoreWithoutCommitTimestamps(t *testing.T, config postgresTestConfig) {
	pgVersion := config.pgVersion
	enablePgbouncer := config.pgbouncer
	t.Run(fmt.Sprintf("postgres-%s", pgVersion), func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		b := testdatastore.RunPostgresForTestingWithCommitTimestamps(t, "", "head", false, pgVersion, enablePgbouncer)

		// NOTE: watch API requires the commit timestamps, so we skip those tests here.
		// NOTE: gc tests take exclusive locks, so they are run under non-parallel.
		test.AllWithExceptions(t, test.DatastoreTesterFunc(func(revisionQuantization, _, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
			ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
				ds, err := newPostgresDatastore(ctx, uri, primaryInstanceID,
					RevisionQuantization(revisionQuantization),
					GCWindow(gcWindow),
					GCInterval(veryLargeGCInterval),
					WatchBufferLength(watchBufferLength),
					DebugAnalyzeBeforeStatistics(),
					WithRevisionHeartbeat(false),
				)
				require.NoError(t, err)
				return ds
			})
			return ds, nil
		}), test.WithCategories(test.WatchCategory, test.GCCategory), false)
	})

	t.Run(fmt.Sprintf("postgres-%s-gc", pgVersion), func(t *testing.T) {
		ctx := context.Background()
		b := testdatastore.RunPostgresForTestingWithCommitTimestamps(t, "", "head", false, pgVersion, enablePgbouncer)
		test.OnlyGCTests(t, test.DatastoreTesterFunc(func(revisionQuantization, gcInterval, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
			ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
				ds, err := newPostgresDatastore(ctx, uri, primaryInstanceID,
					RevisionQuantization(revisionQuantization),
					GCWindow(gcWindow),
					GCInterval(gcInterval),
					WatchBufferLength(watchBufferLength),
					DebugAnalyzeBeforeStatistics(),
					WithRevisionHeartbeat(false),
				)
				require.NoError(t, err)
				return ds
			})
			return ds, nil
		}), false)
	})
}

type datastoreTestFunc func(t *testing.T, ds datastore.Datastore)

func createDatastoreTest(b testdatastore.RunningEngineForTest, tf datastoreTestFunc, options ...Option) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()
		ctx := context.Background()
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := newPostgresDatastore(ctx, uri, primaryInstanceID, options...)
			require.NoError(t, err)
			return ds
		})
		defer ds.Close()

		tf(t, ds)
	}
}

func createReplicaDatastoreTest(b testdatastore.RunningEngineForTest, tf multiDatastoreTestFunc, options ...Option) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		var replicaDS datastore.Datastore

		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := newPostgresDatastore(ctx, uri, primaryInstanceID, options...)
			require.NoError(t, err)

			ds2, err := newPostgresDatastore(ctx, uri, 42, append(options, ReadStrictMode(true))...)
			require.NoError(t, err)
			replicaDS = ds2

			return ds
		})
		defer ds.Close()

		tf(t, ds, replicaDS)
	}
}

type multiDatastoreTestFunc func(t *testing.T, ds1 datastore.Datastore, ds2 datastore.Datastore)

func createMultiDatastoreTest(b testdatastore.RunningEngineForTest, tf multiDatastoreTestFunc, options ...Option) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		var secondDS datastore.Datastore
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := newPostgresDatastore(ctx, uri, primaryInstanceID, options...)
			require.NoError(t, err)

			ds2, err := newPostgresDatastore(ctx, uri, primaryInstanceID, options...)
			require.NoError(t, err)

			secondDS = ds2
			return ds
		})
		defer ds.Close()

		tf(t, ds, secondDS)
	}
}

func SerializationErrorTest(t *testing.T, ds datastore.Datastore) {
	require := require.New(t)

	ctx := context.Background()
	r, err := ds.ReadyState(ctx)
	require.NoError(err)
	require.True(r.IsReady)

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		updates := []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("resource:resource#reader@user:user#...")),
		}
		rwt.(*pgReadWriteTXN).tx = txWithSerializationError{rwt.(*pgReadWriteTXN).tx}
		return rwt.WriteRelationships(ctx, updates)
	}, options.WithDisableRetries(true) /* ensures the error is returned immediately */)

	require.Contains(err.Error(), "unable to write relationships due to a serialization error")
}

type txWithSerializationError struct {
	pgx.Tx
}

func (txwse txWithSerializationError) Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error) {
	return pgconn.CommandTag{}, &pgconn.PgError{
		Code:    pgSerializationFailure,
		Message: "fake serialization error",
	}
}

func GarbageCollectionTest(t *testing.T, ds datastore.Datastore) {
	require := require.New(t)

	ctx := context.Background()
	r, err := ds.ReadyState(ctx)
	require.NoError(err)
	require.True(r.IsReady)
	firstWrite, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		// Write basic namespaces.
		return rwt.WriteNamespaces(ctx, namespace.Namespace(
			"resource",
			namespace.MustRelation("reader", nil),
		), namespace.Namespace("user"))
	})
	require.NoError(err)

	// Run GC at the transaction and ensure no relationships are removed.
	pds := ds.(*pgDatastore)

	// Nothing to GC
	removed, err := pds.DeleteBeforeTx(ctx, firstWrite)
	require.NoError(err)
	require.Zero(removed.Relationships)
	require.Zero(removed.Namespaces)

	// Replace the namespace with a new one.
	updateTwoNamespaces, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(
			ctx,
			namespace.Namespace(
				"resource",
				namespace.MustRelation("reader", nil),
				namespace.MustRelation("unused", nil),
			),
			namespace.Namespace("user"),
		)
	})
	require.NoError(err)

	// Run GC to remove the old transaction
	removed, err = pds.DeleteBeforeTx(ctx, updateTwoNamespaces)
	require.NoError(err)
	require.Zero(removed.Relationships)
	require.Equal(int64(1), removed.Transactions) // firstWrite
	require.Equal(int64(2), removed.Namespaces)   // resource, user

	// Write a relationship.
	rel := tuple.MustParse("resource:someresource#reader@user:someuser#...")

	wroteOneRelationship, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, rel)
	require.NoError(err)

	// Run GC at the transaction and ensure no relationships are removed, but 1 transaction (the previous write namespace) is.
	removed, err = pds.DeleteBeforeTx(ctx, wroteOneRelationship)
	require.NoError(err)
	require.Zero(removed.Relationships)
	require.Equal(int64(1), removed.Transactions) // updateTwoNamespaces
	require.Zero(removed.Namespaces)

	// Run GC again and ensure there are no changes.
	removed, err = pds.DeleteBeforeTx(ctx, wroteOneRelationship)
	require.NoError(err)
	require.Zero(removed.Relationships)
	require.Zero(removed.Transactions)
	require.Zero(removed.Namespaces)

	// Ensure the relationship is still present.
	tRequire := testfixtures.RelationshipChecker{Require: require, DS: ds}
	tRequire.RelationshipExists(ctx, rel, wroteOneRelationship)

	// Overwrite the relationship by changing its caveat.
	rel = tuple.MustWithCaveat(rel, "somecaveat")
	relOverwrittenAt, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, rel)
	require.NoError(err)

	// Run GC, which won't clean anything because we're dropping the write transaction only
	removed, err = pds.DeleteBeforeTx(ctx, relOverwrittenAt)
	require.NoError(err)
	require.Equal(int64(1), removed.Relationships) // wroteOneRelationship
	require.Equal(int64(1), removed.Transactions)  // wroteOneRelationship
	require.Zero(removed.Namespaces)

	// Run GC again and ensure there are no changes.
	removed, err = pds.DeleteBeforeTx(ctx, relOverwrittenAt)
	require.NoError(err)
	require.Zero(removed.Relationships)
	require.Zero(removed.Transactions)
	require.Zero(removed.Namespaces)

	// Ensure the relationship is still present.
	tRequire.RelationshipExists(ctx, rel, relOverwrittenAt)

	// Delete the relationship.
	relDeletedAt, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationDelete, rel)
	require.NoError(err)

	// Ensure the relationship is gone.
	tRequire.NoRelationshipExists(ctx, rel, relDeletedAt)

	// Run GC, which will now drop the overwrite transaction only and the first rel revision
	removed, err = pds.DeleteBeforeTx(ctx, relDeletedAt)
	require.NoError(err)
	require.Equal(int64(1), removed.Relationships)
	require.Equal(int64(1), removed.Transactions) // relOverwrittenAt
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
		rel = tuple.MustWithCaveat(rel, fmt.Sprintf("somecaveat%d", i))

		var err error
		relLastWriteAt, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationTouch, rel)
		require.NoError(err)
	}

	// Run GC at the transaction and ensure the older copies of the relationships are removed,
	// as well as the 2 older write transactions and the older delete transaction.
	removed, err = pds.DeleteBeforeTx(ctx, relLastWriteAt)
	require.NoError(err)
	require.Equal(int64(2), removed.Relationships) // delete, old1
	require.Equal(int64(3), removed.Transactions)  // removed, write1, write2
	require.Zero(removed.Namespaces)

	// Ensure the relationship is still present.
	tRequire.RelationshipExists(ctx, rel, relLastWriteAt)

	// Inject a transaction to clean up the last write
	lastRev, err := pds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return nil
	})
	require.NoError(err)

	// Run GC to clean up the last write
	removed, err = pds.DeleteBeforeTx(ctx, lastRev)
	require.NoError(err)
	require.Zero(removed.Relationships)           // write3
	require.Equal(int64(1), removed.Transactions) // write3
	require.Zero(removed.Namespaces)
}

func TransactionTimestampsTest(t *testing.T, ds datastore.Datastore) {
	require := require.New(t)

	ctx := context.Background()
	r, err := ds.ReadyState(ctx)
	require.NoError(err)
	require.True(r.IsReady)

	// Setting db default time zone to before UTC
	pgd := ds.(*pgDatastore)
	_, err = pgd.writePool.Exec(ctx, "SET TIME ZONE 'America/New_York';")
	require.NoError(err)

	// Get timestamp in UTC as reference
	startTimeUTC, err := pgd.Now(ctx)
	require.NoError(err)

	// Transaction timestamp should not be stored in system time zone
	tx, err := pgd.writePool.Begin(ctx)
	require.NoError(err)

	txXID, _, err := createNewTransaction(ctx, tx, nil)
	require.NoError(err)

	err = tx.Commit(ctx)
	require.NoError(err)

	var ts time.Time
	sql, args, err := psql.Select("timestamp").From(schema.TableTransaction).Where(sq.Eq{"xid": txXID}).ToSql()
	require.NoError(err)
	err = pgd.readPool.QueryRow(ctx, sql, args...).Scan(&ts)
	require.NoError(err)

	// Transaction timestamp will be before the reference time if it was stored
	// in the default time zone and reinterpreted
	require.True(startTimeUTC.Before(ts))
}

func GarbageCollectionByTimeTest(t *testing.T, ds datastore.Datastore) {
	require := require.New(t)

	ctx := context.Background()
	r, err := ds.ReadyState(ctx)
	require.NoError(err)
	require.True(r.IsReady)
	// Write basic namespaces.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, namespace.Namespace(
			"resource",
			namespace.MustRelation("reader", nil),
		), namespace.Namespace("user"))
	})
	require.NoError(err)

	pds := ds.(*pgDatastore)

	// Sleep 1ms to ensure GC will delete the previous transaction.
	time.Sleep(1 * time.Millisecond)

	// Write a relationship.
	rel := tuple.MustParse("resource:someresource#reader@user:someuser#...")
	relLastWriteAt, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, rel)
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
	tRequire := testfixtures.RelationshipChecker{Require: require, DS: ds}
	tRequire.RelationshipExists(ctx, rel, relLastWriteAt)

	// Sleep 1ms to ensure GC will delete the previous write.
	time.Sleep(1 * time.Millisecond)

	// Delete the relationship.
	relDeletedAt, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationDelete, rel)
	require.NoError(err)

	// Inject a revision to sweep up the last revision
	_, err = pds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return nil
	})
	require.NoError(err)

	// Run GC and ensure the relationship is not removed.
	afterDelete, err := pds.Now(ctx)
	require.NoError(err)

	afterDeleteTx, err := pds.TxIDBefore(ctx, afterDelete)
	require.NoError(err)

	removed, err = pds.DeleteBeforeTx(ctx, afterDeleteTx)
	require.NoError(err)
	require.Equal(int64(1), removed.Relationships)
	require.Equal(int64(2), removed.Transactions) // relDeletedAt, injected
	require.Zero(removed.Namespaces)

	// Ensure the relationship is still not present.
	tRequire.NoRelationshipExists(ctx, rel, relDeletedAt)
}

const chunkRelationshipCount = 2000

func ChunkedGarbageCollectionTest(t *testing.T, ds datastore.Datastore) {
	require := require.New(t)

	ctx := context.Background()
	r, err := ds.ReadyState(ctx)
	require.NoError(err)
	require.True(r.IsReady)
	// Write basic namespaces.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, namespace.Namespace(
			"resource",
			namespace.MustRelation("reader", nil),
		), namespace.Namespace("user"))
	})
	require.NoError(err)

	pds := ds.(*pgDatastore)

	// Prepare relationships to write.
	var rels []tuple.Relationship
	for i := 0; i < chunkRelationshipCount; i++ {
		rel := tuple.MustParse(fmt.Sprintf("resource:resource-%d#reader@user:someuser#...", i))
		rels = append(rels, rel)
	}

	// Write a large number of relationships.
	writtenAt, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, rels...)
	require.NoError(err)

	// Ensure the relationships were written.
	tRequire := testfixtures.RelationshipChecker{Require: require, DS: ds}
	for _, rel := range rels {
		tRequire.RelationshipExists(ctx, rel, writtenAt)
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
	deletedAt, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationDelete, rels...)
	require.NoError(err)

	// Inject a revision to sweep up the last revision
	_, err = pds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return nil
	})
	require.NoError(err)

	// Ensure the relationships were deleted.
	for _, rel := range rels {
		tRequire.NoRelationshipExists(ctx, rel, deletedAt)
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
	require.Equal(int64(2), removed.Transactions)
	require.Zero(removed.Namespaces)
}

func QuantizedRevisionTest(t *testing.T, b testdatastore.RunningEngineForTest) {
	testCases := []struct {
		testName          string
		quantization      time.Duration
		followerReadDelay time.Duration
		relativeTimes     []time.Duration
		numLower          uint64
		numHigher         uint64
	}{
		{
			"DefaultRevision",
			1 * time.Second,
			0,
			[]time.Duration{},
			0, 0,
		},
		{
			"OnlyPastRevisions",
			1 * time.Second,
			0,
			[]time.Duration{-2 * time.Second},
			1, 0,
		},
		{
			"OldestInWindowIsSelected",
			1 * time.Second,
			0,
			[]time.Duration{1 * time.Millisecond, 2 * time.Millisecond},
			1, 1,
		},
		{
			"ShouldObservePreviousAndCurrent",
			1 * time.Second,
			0,
			[]time.Duration{-1 * time.Second, 0},
			2, 0,
		},
		{
			"OnlyFutureRevisions",
			1 * time.Second,
			0,
			[]time.Duration{2 * time.Second},
			1, 0,
		},
		{
			"QuantizedLower",
			2 * time.Second,
			0,
			[]time.Duration{-4 * time.Second, -1 * time.Nanosecond, 0},
			2, 1,
		},
		{
			"QuantizedRecentWithFollowerReadDelay",
			500 * time.Millisecond,
			2 * time.Second,
			[]time.Duration{-4 * time.Second, -2 * time.Second, 0},
			2, 1,
		},
		{
			"QuantizedRecentWithoutFollowerReadDelay",
			500 * time.Millisecond,
			0,
			[]time.Duration{-4 * time.Second, -2 * time.Second, 0},
			3, 0,
		},
		{
			"QuantizationDisabled",
			1 * time.Nanosecond,
			0,
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

				RegisterTypes(conn.TypeMap())

				ds, err := newPostgresDatastore(
					ctx,
					uri,
					primaryInstanceID,
					RevisionQuantization(tc.quantization),
					GCWindow(24*time.Hour),
					WatchBufferLength(1),
					FollowerReadDelay(tc.followerReadDelay),
				)
				require.NoError(err)

				return ds
			})
			defer ds.Close()

			// set a random time zone to ensure the queries are unaffected by tz
			_, err := conn.Exec(ctx, fmt.Sprintf("SET TIME ZONE -%d", rand.Intn(8)+1))
			require.NoError(err)

			var dbNow time.Time
			err = conn.QueryRow(ctx, "SELECT (NOW() AT TIME ZONE 'utc')").Scan(&dbNow)
			require.NoError(err)

			if len(tc.relativeTimes) > 0 {
				psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
				insertTxn := psql.Insert(schema.TableTransaction).Columns(schema.ColTimestamp)

				for _, offset := range tc.relativeTimes {
					sql, args, err := insertTxn.Values(dbNow.Add(offset)).ToSql()
					require.NoError(err)

					_, err = conn.Exec(ctx, sql, args...)
					require.NoError(err)
				}
			}

			assertRevisionLowerAndHigher(ctx, t, ds, conn, tc.numLower, tc.numHigher)
		})
	}
}

func OverlappingRevisionTest(t *testing.T, b testdatastore.RunningEngineForTest) {
	testCases := []struct {
		testName          string
		quantization      time.Duration
		followerReadDelay time.Duration
		revisions         []postgresRevision
		numLower          uint64
		numHigher         uint64
	}{
		{ // the two revisions are concurrent, and given they are past the quantization window (are way in the past, around unix epoch)
			// the function should return a revision snapshot that captures both of them
			"ConcurrentRevisions",
			5 * time.Second,
			0,
			[]postgresRevision{
				{optionalTxID: newXid8(3), snapshot: pgSnapshot{xmin: 1, xmax: 4, xipList: []uint64{2}}, optionalNanosTimestamp: uint64((time.Second * 1) * time.Nanosecond)},
				{optionalTxID: newXid8(2), snapshot: pgSnapshot{xmin: 1, xmax: 4, xipList: []uint64{3}}, optionalNanosTimestamp: uint64((time.Second * 2) * time.Nanosecond)},
			},
			2, 0,
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

				RegisterTypes(conn.TypeMap())

				ds, err := newPostgresDatastore(
					ctx,
					uri,
					primaryInstanceID,
					RevisionQuantization(tc.quantization),
					GCWindow(24*time.Hour),
					WatchBufferLength(1),
					FollowerReadDelay(tc.followerReadDelay),
				)
				require.NoError(err)

				return ds
			})
			defer ds.Close()

			// set a random time zone to ensure the queries are unaffected by tz
			_, err := conn.Exec(ctx, fmt.Sprintf("SET TIME ZONE -%d", rand.Intn(8)+1))
			require.NoError(err)

			for _, rev := range tc.revisions {
				stmt := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
				insertTxn := stmt.Insert(schema.TableTransaction).Columns(schema.ColXID, schema.ColSnapshot, schema.ColTimestamp)

				ts := time.Unix(0, int64(rev.optionalNanosTimestamp))
				sql, args, err := insertTxn.Values(rev.optionalTxID, rev.snapshot, ts).ToSql()
				require.NoError(err)

				_, err = conn.Exec(ctx, sql, args...)
				require.NoError(err)
			}

			assertRevisionLowerAndHigher(ctx, t, ds, conn, tc.numLower, tc.numHigher)
		})
	}
}

func assertRevisionLowerAndHigher(ctx context.Context, t *testing.T, ds datastore.Datastore, conn *pgx.Conn,
	expectedNumLower, expectedNumHigher uint64,
) {
	t.Helper()

	var revision xid8
	var snapshot pgSnapshot
	pgDS, ok := ds.(*pgDatastore)
	require.True(t, ok)
	rev, _, err := pgDS.optimizedRevisionFunc(ctx)
	require.NoError(t, err)

	pgRev, ok := rev.(postgresRevision)
	require.True(t, ok)
	revision = pgRev.optionalTxID
	require.NotNil(t, revision)
	snapshot = pgRev.snapshot

	queryFmt := "SELECT COUNT(%[1]s) FROM %[2]s WHERE pg_visible_in_snapshot(%[1]s, $1) = %[3]s;"
	numLowerQuery := fmt.Sprintf(queryFmt, schema.ColXID, schema.TableTransaction, "true")
	numHigherQuery := fmt.Sprintf(queryFmt, schema.ColXID, schema.TableTransaction, "false")

	var numLower, numHigher uint64
	require.NoError(t, conn.QueryRow(ctx, numLowerQuery, snapshot).Scan(&numLower), "%s - %s", revision, snapshot)
	require.NoError(t, conn.QueryRow(ctx, numHigherQuery, snapshot).Scan(&numHigher), "%s - %s", revision, snapshot)

	// Subtract one from numLower because of the artificially injected first transaction row
	require.Equal(t, expectedNumLower, numLower-1, "incorrect number of revisions visible to snapshot, expected %d, got %d", expectedNumLower, numLower-1)
	require.Equal(t, expectedNumHigher, numHigher, "incorrect number of revisions invisible to snapshot, expected %d, got %d", expectedNumHigher, numHigher)
}

// ConcurrentRevisionHeadTest uses goroutines and channels to intentionally set up a pair of
// revisions that are concurrently applied and then ensures a call to HeadRevision reflects
// the changes found in *both* revisions.
func ConcurrentRevisionHeadTest(t *testing.T, ds datastore.Datastore) {
	require := require.New(t)

	ctx := context.Background()
	r, err := ds.ReadyState(ctx)
	require.NoError(err)
	require.True(r.IsReady)
	// Write basic namespaces.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, namespace.Namespace(
			"resource",
			namespace.MustRelation("reader", nil),
		), namespace.Namespace("user"))
	})
	require.NoError(err)

	g := errgroup.Group{}

	waitToStart := make(chan struct{})
	waitToFinish := make(chan struct{})

	var commitLastRev, commitFirstRev datastore.Revision
	g.Go(func() error {
		var err error
		commitLastRev, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			rtu := tuple.Touch(tuple.MustParse("resource:123#reader@user:456"))
			err = rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{rtu})
			require.NoError(err)

			close(waitToStart)
			<-waitToFinish

			return err
		})
		require.NoError(err)
		return nil
	})

	<-waitToStart

	commitFirstRev, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		rtu := tuple.Touch(tuple.MustParse("resource:789#reader@user:456"))
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{rtu})
	})
	close(waitToFinish)

	require.NoError(err)
	require.NoError(g.Wait())

	// Ensure the revisions do not compare.
	require.False(commitFirstRev.GreaterThan(commitLastRev))
	require.False(commitFirstRev.Equal(commitLastRev))

	// Ensure a call to HeadRevision now reflects both sets of data applied.
	headRev, err := ds.HeadRevision(ctx)
	require.NoError(err)

	reader := ds.SnapshotReader(headRev)
	it, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: "resource",
	})
	require.NoError(err)

	found, err := datastore.IteratorToSlice(it)
	require.NoError(err)
	require.Equal(2, len(found), "missing relationships in %v", found)
}

// ConcurrentRevisionWatchTest uses goroutines and channels to intentionally set up a pair of
// revisions that are concurrently applied and then ensures that a Watch call does not end up
// in a loop.
func ConcurrentRevisionWatchTest(t *testing.T, ds datastore.Datastore) {
	require := require.New(t)

	ctx := context.Background()
	withCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	r, err := ds.ReadyState(ctx)
	require.NoError(err)
	require.True(r.IsReady)

	// Write basic namespaces.
	rev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, namespace.Namespace(
			"resource",
			namespace.MustRelation("reader", nil),
		), namespace.Namespace("user"))
	})
	require.NoError(err)

	// Start a watch loop.
	waitForWatch := make(chan struct{})
	seenWatchRevisions := make([]datastore.Revision, 0)
	seenWatchRevisionsLock := sync.Mutex{}

	go func() {
		changes, _ := ds.Watch(withCancel, rev, datastore.WatchJustRelationships())

		waitForWatch <- struct{}{}

		for {
			select {
			case change, ok := <-changes:
				if !ok {
					return
				}

				seenWatchRevisionsLock.Lock()
				seenWatchRevisions = append(seenWatchRevisions, change.Revision)
				seenWatchRevisionsLock.Unlock()

				time.Sleep(1 * time.Millisecond)
			case <-withCancel.Done():
				return
			}
		}
	}()

	<-waitForWatch

	// Write the two concurrent transactions, while watching for changes.
	g := errgroup.Group{}

	waitToStart := make(chan struct{})
	waitToFinish := make(chan struct{})

	var commitLastRev, commitFirstRev datastore.Revision
	g.Go(func() error {
		var err error
		commitLastRev, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			err = rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
				tuple.Touch(tuple.MustParse("something:001#viewer@user:123")),
				tuple.Touch(tuple.MustParse("something:002#viewer@user:123")),
				tuple.Touch(tuple.MustParse("something:003#viewer@user:123")),
			})
			require.NoError(err)

			close(waitToStart)
			<-waitToFinish

			return err
		}, options.WithDisableRetries(true))
		require.NoError(err)
		return nil
	})

	<-waitToStart

	commitFirstRev, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Touch(tuple.MustParse("resource:1001#reader@user:456")),
			tuple.Touch(tuple.MustParse("resource:1002#reader@user:456")),
			tuple.Touch(tuple.MustParse("resource:1003#reader@user:456")),
		})
	}, options.WithDisableRetries(true))
	require.NoError(err)

	close(waitToFinish)
	require.NoError(g.Wait())

	// Ensure the revisions do not compare.
	require.False(commitFirstRev.GreaterThan(commitLastRev), "found %v and %v", commitFirstRev, commitLastRev)
	require.False(commitLastRev.GreaterThan(commitFirstRev), "found %v and %v", commitLastRev, commitFirstRev)
	require.False(commitFirstRev.Equal(commitLastRev), "found %v and %v", commitFirstRev, commitLastRev)

	// Write another revision.
	afterRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		rtu := tuple.Touch(tuple.MustParse("resource:2345#reader@user:456"))
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{rtu})
	})
	require.NoError(err)
	require.True(afterRev.GreaterThan(commitFirstRev))
	require.True(afterRev.GreaterThan(commitLastRev))

	// Ensure that the last revision is eventually seen from the watch.
	require.Eventually(func() bool {
		seenWatchRevisionsLock.Lock()
		defer seenWatchRevisionsLock.Unlock()
		return len(seenWatchRevisions) == 3 && seenWatchRevisions[len(seenWatchRevisions)-1].String() == afterRev.String()
	}, 2*time.Second, 5*time.Millisecond)
}

func OverlappingRevisionWatchTest(t *testing.T, ds datastore.Datastore) {
	require := require.New(t)

	ctx := context.Background()
	r, err := ds.ReadyState(ctx)
	require.NoError(err)
	require.True(r.IsReady)

	rev, err := ds.HeadRevision(ctx)
	require.NoError(err)

	pds := ds.(*pgDatastore)
	require.True(pds.watchEnabled)

	prev := rev.(postgresRevision)
	nexttx := prev.snapshot.xmax + 1

	// Manually construct an equivalent of overlapping transactions in the database, from the repro
	// information (See: https://github.com/authzed/spicedb/issues/1272)
	err = pgx.BeginTxFunc(ctx, pds.writePool, pgx.TxOptions{IsoLevel: pgx.Serializable}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s ("%s", "%s") VALUES ('%d', '%d:%d:')`,
			schema.TableTransaction,
			schema.ColXID,
			schema.ColSnapshot,
			nexttx,
			nexttx,
			nexttx,
		))
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s") VALUES ('somenamespace', '123', 'viewer', 'user', '456', '...', '', null, '%d'::xid8)`,
			schema.TableTuple,
			schema.ColNamespace,
			schema.ColObjectID,
			schema.ColRelation,
			schema.ColUsersetNamespace,
			schema.ColUsersetObjectID,
			schema.ColUsersetRelation,
			schema.ColCaveatContextName,
			schema.ColCaveatContext,
			schema.ColCreatedXid,
			nexttx,
		))
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s ("xid", "snapshot") VALUES ('%d', '%d:%d:')`,
			schema.TableTransaction,
			nexttx+1,
			nexttx,
			nexttx,
		))
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s") VALUES ('somenamespace', '456', 'viewer', 'user', '456', '...', '', null, '%d'::xid8)`,
			schema.TableTuple,
			schema.ColNamespace,
			schema.ColObjectID,
			schema.ColRelation,
			schema.ColUsersetNamespace,
			schema.ColUsersetObjectID,
			schema.ColUsersetRelation,
			schema.ColCaveatContextName,
			schema.ColCaveatContext,
			schema.ColCreatedXid,
			nexttx+1,
		))

		return err
	})
	require.NoError(err)

	// Call watch and ensure it terminates with having only read the two expected sets of changes.
	changes, errChan := ds.Watch(ctx, rev, datastore.WatchJustRelationships())
	transactionCount := 0
loop:
	for {
		select {
		case _, ok := <-changes:
			if !ok {
				err := <-errChan
				require.NoError(err)
				return
			}

			transactionCount++
			time.Sleep(10 * time.Millisecond)
		case <-time.NewTimer(1 * time.Second).C:
			break loop
		}
	}

	require.Equal(2, transactionCount)
}

// RevisionInversionTest uses goroutines and channels to intentionally set up a pair of
// revisions that might compare incorrectly.
func RevisionInversionTest(t *testing.T, ds datastore.Datastore) {
	require := require.New(t)

	ctx := context.Background()
	r, err := ds.ReadyState(ctx)
	require.NoError(err)
	require.True(r.IsReady)
	// Write basic namespaces.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, namespace.Namespace(
			"resource",
			namespace.MustRelation("reader", nil),
		), namespace.Namespace("user"))
	})
	require.NoError(err)

	g := errgroup.Group{}

	waitToStart := make(chan struct{})
	waitToFinish := make(chan struct{})

	var commitLastRev, commitFirstRev datastore.Revision
	g.Go(func() error {
		var err error
		commitLastRev, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			rtu := tuple.Touch(tuple.MustParse("resource:123#reader@user:456"))
			err = rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{rtu})
			require.NoError(err)

			close(waitToStart)
			<-waitToFinish

			return err
		})
		require.NoError(err)
		return nil
	})

	<-waitToStart

	commitFirstRev, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		rtu := tuple.Touch(tuple.MustParse("resource:789#reader@user:ten"))
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{rtu})
	})
	close(waitToFinish)

	require.NoError(err)
	require.NoError(g.Wait())
	require.False(commitFirstRev.GreaterThan(commitLastRev))
	require.False(commitFirstRev.Equal(commitLastRev))
}

func OTelTracingTest(t *testing.T, ds datastore.Datastore) {
	otelMutex.Lock()
	defer otelMutex.Unlock()

	require := require.New(t)

	ctx := context.Background()
	r, err := ds.ReadyState(ctx)
	require.NoError(err)
	require.True(r.IsReady)

	spanrecorder := tracetest.NewSpanRecorder()
	testTraceProvider.RegisterSpanProcessor(spanrecorder)

	// Perform basic operation
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, namespace.Namespace("resource"))
	})
	require.NoError(err)

	ended := spanrecorder.Ended()
	var present bool
	for _, span := range ended {
		if span.Name() == "query INSERT" {
			present = true
		}
	}
	require.True(present, "missing trace for Streaming gRPC call")
}

func WatchNotEnabledTest(t *testing.T, _ testdatastore.RunningEngineForTest, pgVersion string) {
	require := require.New(t)

	ds := testdatastore.RunPostgresForTestingWithCommitTimestamps(t, "", migrate.Head, false, pgVersion, false).NewDatastore(t, func(engine, uri string) datastore.Datastore {
		ctx := context.Background()
		ds, err := newPostgresDatastore(ctx, uri,
			primaryInstanceID,
			RevisionQuantization(0),
			GCWindow(time.Millisecond*1),
			GCInterval(veryLargeGCInterval),
			WatchBufferLength(1),
		)
		require.NoError(err)
		return ds
	})
	defer ds.Close()

	ds, revision := testfixtures.StandardDatastoreWithData(ds, require)
	_, errChan := ds.Watch(
		context.Background(),
		revision,
		datastore.WatchJustRelationships(),
	)
	err := <-errChan
	require.NotNil(err)
	require.Contains(err.Error(), "track_commit_timestamp=on")
}

func BenchmarkPostgresQuery(b *testing.B) {
	req := require.New(b)

	ds := testdatastore.RunPostgresForTesting(b, "", migrate.Head, pgversion.MinimumSupportedPostgresVersion, false).NewDatastore(b, func(engine, uri string) datastore.Datastore {
		ctx := context.Background()
		ds, err := newPostgresDatastore(ctx, uri,
			primaryInstanceID,
			RevisionQuantization(0),
			GCWindow(time.Millisecond*1),
			GCInterval(veryLargeGCInterval),
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
				OptionalResourceType: testfixtures.DocumentNS.Name,
			})
			require.NoError(err)
			for rel, err := range iter {
				require.NoError(err)
				require.Equal(testfixtures.DocumentNS.Name, rel.Resource.ObjectType)
			}
		}
	})
}

func datastoreWithInterceptorAndTestData(t *testing.T, interceptor pgcommon.QueryInterceptor, pgVersion string) datastore.Datastore {
	require := require.New(t)

	ds := testdatastore.RunPostgresForTestingWithCommitTimestamps(t, "", migrate.Head, false, pgVersion, false).NewDatastore(t, func(engine, uri string) datastore.Datastore {
		ctx := context.Background()
		ds, err := newPostgresDatastore(ctx, uri,
			primaryInstanceID,
			RevisionQuantization(0),
			GCWindow(time.Millisecond*1),
			GCInterval(veryLargeGCInterval),
			WatchBufferLength(1),
			WithQueryInterceptor(interceptor),
		)
		require.NoError(err)
		return ds
	})
	t.Cleanup(func() {
		ds.Close()
	})

	ds, _ = testfixtures.StandardDatastoreWithData(ds, require)

	// Write namespaces and a few thousand relationships.
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			err := rwt.WriteNamespaces(ctx, namespace.Namespace(
				fmt.Sprintf("resource%d", i),
				namespace.MustRelation("reader", nil)))
			if err != nil {
				return err
			}

			// Write some relationships.
			rtu := tuple.Touch(tuple.Relationship{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ObjectAndRelation{
						ObjectType: testfixtures.DocumentNS.Name,
						ObjectID:   fmt.Sprintf("doc%d", i),
						Relation:   "reader",
					},
					Subject: tuple.ObjectAndRelation{
						ObjectType: "user",
						ObjectID:   "456",
						Relation:   "...",
					},
				},
			})

			rtu2 := tuple.Touch(tuple.Relationship{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ObjectAndRelation{
						ObjectType: fmt.Sprintf("resource%d", i),
						ObjectID:   "123",
						Relation:   "reader",
					},
					Subject: tuple.ObjectAndRelation{
						ObjectType: "user",
						ObjectID:   "456",
						Relation:   "...",
					},
				},
			})

			rtu3 := tuple.Touch(tuple.Relationship{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ObjectAndRelation{
						ObjectType: fmt.Sprintf("resource%d", i),
						ObjectID:   "123",
						Relation:   "writer",
					},
					Subject: tuple.ObjectAndRelation{
						ObjectType: "user",
						ObjectID:   "456",
						Relation:   "...",
					},
				},
			})

			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{rtu, rtu2, rtu3})
		})
		require.NoError(err)
	}

	// Delete some relationships.
	for i := 990; i < 1000; i++ {
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			rtu := tuple.Delete(tuple.Relationship{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ObjectAndRelation{
						ObjectType: testfixtures.DocumentNS.Name,
						ObjectID:   fmt.Sprintf("doc%d", i),
						Relation:   "reader",
					},
					Subject: tuple.ObjectAndRelation{
						ObjectType: "user",
						ObjectID:   "456",
						Relation:   "...",
					},
				},
			})

			rtu2 := tuple.Delete(tuple.Relationship{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ObjectAndRelation{
						ObjectType: fmt.Sprintf("resource%d", i),
						ObjectID:   "123",
						Relation:   "reader",
					},
					Subject: tuple.ObjectAndRelation{
						ObjectType: "user",
						ObjectID:   "456",
						Relation:   "...",
					},
				},
			})
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{rtu, rtu2})
		})
		require.NoError(err)
	}

	// Write some more relationships.
	for i := 1000; i < 1100; i++ {
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			// Write some relationships.
			rtu := tuple.Touch(tuple.Relationship{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ObjectAndRelation{
						ObjectType: testfixtures.DocumentNS.Name,
						ObjectID:   fmt.Sprintf("doc%d", i),
						Relation:   "reader",
					},
					Subject: tuple.ObjectAndRelation{
						ObjectType: "user",
						ObjectID:   "456",
						Relation:   "...",
					},
				},
			})

			rtu2 := tuple.Touch(tuple.Relationship{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ObjectAndRelation{
						ObjectType: fmt.Sprintf("resource%d", i),
						ObjectID:   "123",
						Relation:   "reader",
					},
					Subject: tuple.ObjectAndRelation{
						ObjectType: "user",
						ObjectID:   "456",
						Relation:   "...",
					},
				},
			})
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{rtu, rtu2})
		})
		require.NoError(err)
	}

	return ds
}

func GCQueriesServedByExpectedIndexes(t *testing.T, _ testdatastore.RunningEngineForTest, pgVersion string) {
	require := require.New(t)
	interceptor := &withQueryInterceptor{explanations: make(map[string]string, 0)}
	ds := datastoreWithInterceptorAndTestData(t, interceptor, pgVersion)

	// Get the head revision.
	ctx := context.Background()
	revision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	casted := datastore.UnwrapAs[common.GarbageCollector](ds)
	require.NotNil(casted)

	_, err = casted.DeleteBeforeTx(context.Background(), revision)
	require.NoError(err)

	require.NotEmpty(interceptor.explanations, "expected queries to be executed")

	// Ensure we have indexes representing each query in the GC workflow.
	for _, explanation := range interceptor.explanations {
		switch {
		case strings.HasPrefix(explanation, "Delete on relation_tuple_transaction"):
			fallthrough

		case strings.HasPrefix(explanation, "Delete on namespace_config"):
			fallthrough

		case strings.HasPrefix(explanation, "Delete on relation_tuple"):
			require.Contains(explanation, "Index Scan")

		default:
			require.Failf("unknown GC query: %s", explanation)
		}
	}
}

func RepairTransactionsTest(t *testing.T, ds datastore.Datastore) {
	// Break the datastore by adding a transaction entry with an XID greater the current one.
	pds := ds.(*pgDatastore)

	getVersionQuery := fmt.Sprintf("SELECT version()")
	var version string
	err := pds.writePool.QueryRow(context.Background(), getVersionQuery).Scan(&version)
	require.NoError(t, err)

	if strings.HasPrefix(version, "PostgreSQL 13.") || strings.HasPrefix(version, "PostgreSQL 14.") {
		t.Skip("Skipping test on PostgreSQL 13 and 14 as they do not support xid8 max")
		return
	}

	createLaterTxn := fmt.Sprintf(
		"INSERT INTO %s (\"xid\") VALUES (12345::text::xid8)",
		schema.TableTransaction,
	)

	_, err = pds.writePool.Exec(context.Background(), createLaterTxn)
	require.NoError(t, err)

	// Run the repair code.
	err = pds.repairTransactionIDs(context.Background(), false)
	require.NoError(t, err)

	// Ensure the current transaction ID is greater than the max specified in the transactions table.
	currentMaximumID := 0
	err = pds.writePool.QueryRow(context.Background(), queryCurrentTransactionID).Scan(&currentMaximumID)
	require.NoError(t, err)
	require.Greater(t, currentMaximumID, 12345)
}

func LockingTest(t *testing.T, ds datastore.Datastore, ds2 datastore.Datastore) {
	pds := ds.(*pgDatastore)
	pds2 := ds2.(*pgDatastore)

	// Acquire a lock.
	ctx := context.Background()
	acquired, err := pds.tryAcquireLock(ctx, 42)
	require.NoError(t, err)
	require.True(t, acquired)

	// Try to acquire again. Must be on a different session, as these locks are reentrant.
	acquired, err = pds2.tryAcquireLock(ctx, 42)
	require.NoError(t, err)
	require.False(t, acquired)

	// Acquire another lock.
	acquired, err = pds.tryAcquireLock(ctx, 43)
	require.NoError(t, err)
	require.True(t, acquired)

	// Release the first lock.
	err = pds.releaseLock(ctx, 42)
	require.NoError(t, err)

	// Try to acquire the first lock again.
	acquired, err = pds.tryAcquireLock(ctx, 42)
	require.NoError(t, err)
	require.True(t, acquired)

	// Release the second lock.
	err = pds.releaseLock(ctx, 43)
	require.NoError(t, err)

	// Release the first lock.
	err = pds.releaseLock(ctx, 42)
	require.NoError(t, err)
}

func StrictReadModeFallbackTest(t *testing.T, primaryDS datastore.Datastore, unwrappedReplicaDS datastore.Datastore) {
	require := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Write some relationships.
	_, err := primaryDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		rtu := tuple.Touch(tuple.MustParse("resource:123#reader@user:456"))
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{rtu})
	})
	require.NoError(err)

	// Get the HEAD revision.
	lowestRevision, err := primaryDS.HeadRevision(ctx)
	require.NoError(err)

	// Wrap the replica DS.
	replicaDS, err := proxy.NewStrictReplicatedDatastore(primaryDS, unwrappedReplicaDS.(datastore.StrictReadDatastore))
	require.NoError(err)

	// Perform a read at the head revision, which should succeed.
	reader := replicaDS.SnapshotReader(lowestRevision)
	it, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: "resource",
	})
	require.NoError(err)

	found, err := datastore.IteratorToSlice(it)
	require.NoError(err)
	require.NotEmpty(found)

	// Perform a read at a manually constructed revision beyond head, which should fallback to the primary.
	badRev := postgresRevision{
		snapshot: pgSnapshot{
			// NOTE: the struct defines this value as uint64, but the underlying
			// revision is defined as an int64, so we run into an overflow issue
			// if we try and use a big uint64.
			xmin: 123456789,
			xmax: 123456789,
		},
	}

	limit := uint64(50)
	it, err = replicaDS.SnapshotReader(badRev).QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: "resource",
	}, options.WithLimit(&limit))
	require.NoError(err)

	found2, err := datastore.IteratorToSlice(it)
	require.NoError(err)
	require.Equal(len(found), len(found2))
}

func StrictReadModeTest(t *testing.T, primaryDS datastore.Datastore, replicaDS datastore.Datastore) {
	require := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Write some relationships.
	_, err := primaryDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		rtu := tuple.Touch(tuple.MustParse("resource:123#reader@user:456"))
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{rtu})
	})
	require.NoError(err)

	// Get the HEAD revision.
	lowestRevision, err := primaryDS.HeadRevision(ctx)
	require.NoError(err)

	// Perform a read at the head revision, which should succeed.
	reader := replicaDS.SnapshotReader(lowestRevision)
	it, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: "resource",
	})
	require.NoError(err)

	found, err := datastore.IteratorToSlice(it)
	require.NoError(err)
	require.NotEmpty(found)

	// Perform a read at a manually constructed revision beyond head, which should fail.
	badRev := postgresRevision{
		snapshot: pgSnapshot{
			// NOTE: the struct defines this value as uint64, but the underlying
			// revision is defined as an int64, so we run into an overflow issue
			// if we try and use a big uint64.
			xmin: 123456789,
			xmax: 123456789,
		},
	}

	it, err = replicaDS.SnapshotReader(badRev).QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: "resource",
	})
	require.NoError(err)

	found2, err := datastore.IteratorToSlice(it)
	require.Error(err)
	require.ErrorContains(err, "is not available on the replica")
	require.ErrorAs(err, &common.RevisionUnavailableError{})
	require.Nil(found2)
}

func NullCaveatWatchTest(t *testing.T, ds datastore.Datastore) {
	require := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lowestRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	// Run the watch API.
	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchJustRelationships())
	require.Zero(len(errchan))

	// Manually insert a relationship with a NULL caveat. This is allowed, but can only happen due to
	// bulk import (normal write rels will make it empty instead)
	pds := ds.(*pgDatastore)
	_, err = pds.ReadWriteTx(ctx, func(ctx context.Context, drwt datastore.ReadWriteTransaction) error {
		rwt := drwt.(*pgReadWriteTXN)

		createInserts := writeTuple
		valuesToWrite := []interface{}{
			"resource",
			"someresourceid",
			"somerelation",
			"subject",
			"somesubject",
			"...",
			nil, // set explicitly to null
			nil, // set explicitly to null
			nil, // no expiration
		}

		query := createInserts.Values(valuesToWrite...)
		sql, args, err := query.ToSql()
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}

		_, err = rwt.tx.Exec(ctx, sql, args...)
		return err
	})
	require.NoError(err)

	// Verify the relationship create was tracked by the watch.
	test.VerifyUpdates(require, [][]tuple.RelationshipUpdate{
		{
			tuple.Touch(tuple.MustParse("resource:someresourceid#somerelation@subject:somesubject")),
		},
	},
		changes,
		errchan,
		false,
	)

	// Delete the relationship and ensure it does not raise an error in watch.
	deleteUpdate := tuple.Delete(tuple.MustParse("resource:someresourceid#somerelation@subject:somesubject"))
	_, err = common.UpdateRelationshipsInDatastore(ctx, ds, deleteUpdate)
	require.NoError(err)

	// Verify the delete.
	test.VerifyUpdates(require, [][]tuple.RelationshipUpdate{
		{
			tuple.Delete(tuple.MustParse("resource:someresourceid#somerelation@subject:somesubject")),
		},
	},
		changes,
		errchan,
		false,
	)
}

func RevisionTimestampAndTransactionIDTest(t *testing.T, ds datastore.Datastore) {
	require := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lowestRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	// Run the watch API.
	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchOptions{
		Content: datastore.WatchRelationships | datastore.WatchSchema | datastore.WatchCheckpoints,
	})
	require.Zero(len(errchan))

	pds := ds.(*pgDatastore)
	_, err = pds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Touch(tuple.MustParse("something:001#viewer@user:123")),
		})
	})
	require.NoError(err)

	anHourAgo := time.Now().UTC().Add(-1 * time.Hour)
	var checkedUpdate, checkedCheckpoint bool
	for {
		if checkedCheckpoint && checkedUpdate {
			break
		}

		changeWait := time.NewTimer(waitForChangesTimeout)
		select {
		case change, ok := <-changes:
			if !ok {
				errWait := time.NewTimer(waitForChangesTimeout)
				select {
				case err := <-errchan:
					require.True(errors.As(err, &datastore.WatchDisconnectedError{}))
					return
				case <-errWait.C:
					require.Fail("Timed out waiting for WatchDisconnectedError")
				}
				return
			}

			if !change.IsCheckpoint {
				rev := change.Revision.(postgresRevision)
				timestamp, timestampPresent := rev.OptionalNanosTimestamp()
				require.True(timestampPresent, "expected timestamp to be present in revision")
				isCorrectAndUsesNanos := time.Unix(0, int64(timestamp)).After(anHourAgo)
				require.True(isCorrectAndUsesNanos, "timestamp is not correct")

				_, transactionIDPresent := rev.OptionalTransactionID()
				require.True(transactionIDPresent, "expected transactionID to be present in revision")

				checkedUpdate = true
			} else {
				// we wait for a checkpoint right after the update. Checkpoints could happen at any time off band.
				if checkedUpdate {
					checkedCheckpoint = true
				}
			}

			time.Sleep(1 * time.Millisecond)
		case <-changeWait.C:
			require.Fail("Timed out")
		}
	}
}

func ContinuousCheckpointTest(t *testing.T, ds datastore.Datastore) {
	require := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lowestRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	// Run the watch API.
	changes, errchan := ds.Watch(ctx, lowestRevision, datastore.WatchOptions{
		Content:            datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Zero(len(errchan))

	var checkpointCount int
	for {
		changeWait := time.NewTimer(waitForChangesTimeout)
		select {
		case change, ok := <-changes:
			if !ok {
				require.GreaterOrEqual(checkpointCount, 10, "expected at least 5 checkpoints")

				return
			}

			if change.IsCheckpoint {
				if change.Revision.GreaterThan(lowestRevision) {
					checkpointCount++
					lowestRevision = change.Revision
				}

				if checkpointCount >= 10 {
					return
				}
			}

			time.Sleep(10 * time.Millisecond)
		case <-changeWait.C:
			require.Fail("timed out waiting for checkpoint for out of band change")
		}
	}
}

func ObjectDataTest(t *testing.T, ds datastore.Datastore) {
	ctx := context.Background()
	_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(
			ctx,
			namespace.Namespace(
				"user",
				namespace.MustRelation("", nil),
			),
			namespace.Namespace(
				"document",
				namespace.MustRelation("viewer", nil),
			),
		)
	})
	require.NoError(t, err)

	// Test 1: Basic write and read
	t.Run("BasicWriteRead", func(t *testing.T) {
		resourceObjectDataStruct, err := structpb.NewStruct(map[string]interface{}{
			"title": "Document 1",
			"tags":  []interface{}{"tag1", "tag2"},
			"metadata": map[string]interface{}{
				"created_at": time.Now().UTC().Format(time.RFC3339),
				"updated_at": time.Now().UTC().Format(time.RFC3339),
				"version":    1,
				"size":       1024,
			},
		})
		require.NoError(t, err)

		subjectObjectDataStruct, err := structpb.NewStruct(map[string]interface{}{
			"name":  "Bob",
			"email": "bob@test.com",
		})
		require.NoError(t, err)

		// Write relationship with object data
		rel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ObjectAndRelation{
					ObjectType: "document",
					ObjectID:   "doc1",
					Relation:   "viewer",
					ObjectData: resourceObjectDataStruct,
				},
				Subject: tuple.ObjectAndRelation{
					ObjectType: "user",
					ObjectID:   "bob",
					ObjectData: subjectObjectDataStruct,
				},
			},
		}

		mutations := []tuple.RelationshipUpdate{{
			Operation:    tuple.UpdateOperationCreate,
			Relationship: rel,
		}}

		revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, mutations)
		})

		require.NoError(t, err)

		// Read and verify
		filter := datastore.RelationshipsFilter{
			OptionalResourceType:     "document",
			OptionalResourceIds:      []string{"doc1"},
			OptionalResourceRelation: "viewer",
			OptionalSubjectsSelectors: []datastore.SubjectsSelector{
				{
					OptionalSubjectType: "user",
					OptionalSubjectIds:  []string{"bob"},
				},
			},
		}

		iter, err := ds.SnapshotReader(revision).QueryRelationships(ctx, filter, options.WithIncludeObjectData(true))
		require.NoError(t, err)

		found := false
		for rel := range iter {
			found = true
			require.Equal(t, resourceObjectDataStruct, rel.Resource.ObjectData)
			require.Equal(t, subjectObjectDataStruct, rel.Subject.ObjectData)
		}
		require.True(t, found)
	})

	// Test 2: Deduplication
	t.Run("Deduplication", func(t *testing.T) {
		userObjectDataStruct, err := structpb.NewStruct(map[string]any{
			"name":  "Bob",
			"email": "bob@test.com",
		})
		require.NoError(t, err)

		// Create two relationships with same subject
		rel1 := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ObjectAndRelation{
					ObjectType: "document",
					ObjectID:   "doc2",
					Relation:   "viewer",
				},
				Subject: tuple.ObjectAndRelation{
					ObjectType: "user",
					ObjectID:   "bob",
					ObjectData: userObjectDataStruct,
				},
			},
		}

		rel2 := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ObjectAndRelation{
					ObjectType: "document",
					ObjectID:   "doc3",
					Relation:   "viewer",
				},
				Subject: tuple.ObjectAndRelation{
					ObjectType: "user",
					ObjectID:   "bob",
					ObjectData: userObjectDataStruct,
				},
			},
		}

		mutations := []tuple.RelationshipUpdate{
			{
				Operation:    tuple.UpdateOperationCreate,
				Relationship: rel1,
			},
			{
				Operation:    tuple.UpdateOperationCreate,
				Relationship: rel2,
			},
		}

		_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, mutations)
		})
		require.NoError(t, err)

		// Verify object data was stored only once by checking database directly
		var count int
		err = ds.(*pgDatastore).readPool.QueryRow(ctx, `
			SELECT COUNT(*) FROM object_data 
			WHERE od_type = 'user' AND od_id = 'bob' AND od_deleted_xid = '9223372036854775807'::xid8
		`).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	// Test 3: Edge cases
	t.Run("EdgeCases", func(t *testing.T) {
		cases := []struct {
			name       string
			data       map[string]interface{}
			dataStruct *structpb.Struct
		}{
			{
				name:       "NilData",
				data:       nil,
				dataStruct: nil,
			},
			{
				name:       "EmptyData",
				data:       map[string]interface{}{},
				dataStruct: &structpb.Struct{Fields: map[string]*structpb.Value{}},
			},
			{
				name: "NestedData",
				data: map[string]interface{}{
					"metadata": map[string]interface{}{
						"preferences": map[string]interface{}{
							"theme": "dark",
						},
					},
				},
			},
			{
				name: "ArrayData",
				data: map[string]interface{}{
					"tags": []interface{}{"tag1", "tag2"},
				},
			},
		}

		for _, tc := range cases {
			var err error
			if tc.data != nil {
				tc.dataStruct, err = structpb.NewStruct(tc.data)
				require.NoError(t, err)
			}
			t.Run(tc.name, func(t *testing.T) {
				rel := tuple.Relationship{
					RelationshipReference: tuple.RelationshipReference{
						Resource: tuple.ObjectAndRelation{
							ObjectType: "document",
							ObjectID:   fmt.Sprintf("doc-%s", tc.name),
							Relation:   "viewer",
							ObjectData: tc.dataStruct,
						},
						Subject: tuple.ObjectAndRelation{
							ObjectType: "user",
							ObjectID:   "bob",
						},
					},
				}

				mutations := []tuple.RelationshipUpdate{{
					Operation:    tuple.UpdateOperationCreate,
					Relationship: rel,
				}}

				_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
					return rwt.WriteRelationships(ctx, mutations)
				})
				require.NoError(t, err)
			})
		}
	})
}

const waitForChangesTimeout = 10 * time.Second
