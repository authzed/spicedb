//go:build ci
// +build ci

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/ory/dockertest/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	"github.com/authzed/spicedb/internal/datastore/test"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestMysqlDatastore(t *testing.T) {
	tester := newTester()
	test.All(t, tester)
}

func TestMysqlDatastoreWithTablePrefix(t *testing.T) {
	tester := newPrefixTester("spicedb_")
	test.All(t, tester)
}

func TestMySQLMigrations(t *testing.T) {
	req := require.New(t)

	connectStr := setupDatabase()
	migrationDriver, err := createMigrationDriver(connectStr)
	req.NoError(err)

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

	connectStr := setupDatabase()

	migrationDriver, err := createMigrationDriverWithPrefix(connectStr, "spicedb_")
	req.NoError(err)

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

	db, err := sql.Open("mysql", connectStr)
	rows, err := db.Query("SHOW TABLES;")

	for rows.Next() {
		var tbl string
		req.NoError(rows.Scan(&tbl))
		req.Contains(tbl, "spicedb_")
	}
	req.NoError(rows.Err())
}

func TestIsReady(t *testing.T) {
	req := require.New(t)

	connectStr := setupDatabase()
	store, err := NewMysqlDatastore(connectStr)
	req.NoError(err)

	migrateDatabase(connectStr)

	// ensure no revision is seeded by default
	ctx := context.Background()
	revision, err := store.HeadRevision(ctx)
	req.Equal(datastore.NoRevision, revision)
	req.NoError(err)

	ready, err := store.IsReady(ctx)
	req.NoError(err)
	req.True(ready)

	// verify IsReady seeds the revision is if not present
	revision, err = store.HeadRevision(ctx)
	req.NoError(err)
	req.Equal(revisionFromTransaction(1), revision)
}

func TestIsReadyRace(t *testing.T) {
	req := require.New(t)

	connectStr := setupDatabase()
	store, err := NewMysqlDatastore(connectStr)
	req.NoError(err)

	migrateDatabase(connectStr)

	ctx := context.Background()
	revision, err := store.HeadRevision(ctx)
	req.Equal(datastore.NoRevision, revision)
	req.NoError(err)

	var wg sync.WaitGroup

	concurrency := 5
	for gn := 1; gn <= concurrency; gn++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			ready, err := store.IsReady(ctx)
			req.NoError(err, "goroutine %d", i)
			req.True(ready, "goroutine %d", i)
		}(gn)
	}
	wg.Wait()

	// verify IsReady seeds the revision is if not present
	revision, err = store.HeadRevision(ctx)
	req.NoError(err)
	req.Equal(revisionFromTransaction(1), revision)
}

func TestPrometheusCollector(t *testing.T) {
	req := require.New(t)

	connectStr := setupDatabase()
	store, err := NewMysqlDatastore(connectStr, EnablePrometheusStats())
	req.NoError(err)
	_, err = store.IsReady(context.Background())
	req.NoError(err)

	metrics, err := prometheus.DefaultGatherer.Gather()
	req.NoError(err, metrics)
	var collectorStatsFound, connectorStatsFound bool
	for _, metric := range metrics {
		if "go_sql_stats_connections_open" == metric.GetName() {
			collectorStatsFound = true
		}
		if "spicedb_datastore_mysql_connect_count_total" == metric.GetName() {
			connectorStatsFound = true
		}
	}
	req.True(collectorStatsFound, "mysql datastore did not issue prometheus metrics")
	req.True(connectorStatsFound, "mysql datastore connector did not issue prometheus metrics")
}

func TestGarbageCollection(t *testing.T) {
	req := require.New(t)

	tester := newTester()

	ds, err := tester.New(0, time.Millisecond*1, 1)
	req.NoError(err)
	defer FailOnError(t, ds.Close)

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
	mds := ds.(*mysqlDatastore)

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

func TestGarbageCollectionByTime(t *testing.T) {
	req := require.New(t)

	tester := newTester()

	ds, err := tester.New(0, time.Millisecond*1, 1)
	req.NoError(err)
	defer FailOnError(t, ds.Close)

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

	mds := ds.(*mysqlDatastore)

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

func TestChunkedGarbageCollection(t *testing.T) {
	req := require.New(t)

	tester := newTester()

	ds, err := tester.New(0, time.Millisecond*1, 1)
	req.NoError(err)
	defer FailOnError(t, ds.Close)

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

	mds := ds.(*mysqlDatastore)

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
	var updates []*v1.RelationshipUpdate
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
	var deletes []*v1.RelationshipUpdate
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
func TestTransactionTimestamps(t *testing.T) {
	req := require.New(t)
	connectStr := setupDatabase()
	migrateDatabase(connectStr)

	// Setting db default time zone to before UTC
	ctx := context.Background()
	db, err := sql.Open("mysql", connectStr)
	_, err = db.ExecContext(ctx, "SET GLOBAL time_zone = 'America/New_York';")
	req.NoError(err)

	ds, err := NewMysqlDatastore(connectStr)
	req.NoError(err)

	ok, err := ds.IsReady(ctx)
	req.NoError(err)
	req.True(ok)

	// Get timestamp in UTC as reference
	startTimeUTC, err := ds.getNow(ctx)
	req.NoError(err)

	// Transaction timestamp should not be stored in system time zone
	tx, err := db.BeginTx(ctx, nil)
	txId, err := ds.createNewTransaction(ctx, tx)
	err = tx.Commit()
	req.NoError(err)

	var ts time.Time
	query, args, err := sb.Select(colTimestamp).From(ds.driver.RelationTupleTransaction()).Where(sq.Eq{colID: txId}).ToSql()
	req.NoError(err)
	err = db.QueryRowContext(ctx, query, args...).Scan(&ts)
	req.NoError(err)

	// Let's make sure both getNow() and transactionCreated() have timezones aligned
	req.True(ts.Sub(startTimeUTC) < 5*time.Minute)

	revision, err := ds.OptimizedRevision(ctx)
	req.NoError(err)
	req.Equal(revisionFromTransaction(txId), revision)
}

func TestMain(m *testing.M) {
	mysqlContainerRunOpts := &dockertest.RunOptions{
		Repository: "mysql",
		Tag:        "5",
		Env:        []string{"MYSQL_ROOT_PASSWORD=secret"},
		// increase max connections (default 151) to accommodate tests using the same docker container
		Cmd: []string{"--max-connections=500"},
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		fmt.Printf("could not connect to docker: %s\n", err)
		os.Exit(1)
	}

	// only bring up the container once
	containerResource, err := pool.RunWithOptions(mysqlContainerRunOpts)
	if err != nil {
		fmt.Printf("could not start resource: %s\n", err)
		os.Exit(1)
	}

	containerCleanup := func() {
		// When you're done, kill and remove the container
		if err := pool.Purge(containerResource); err != nil {
			fmt.Printf("could not purge resource: %s\n", err)
			os.Exit(1)
		}
	}

	containerPort = containerResource.GetPort(fmt.Sprintf("%d/tcp", mysqlPort))
	connectStr := fmt.Sprintf("%s@(localhost:%s)/mysql?parseTime=true", creds, containerPort)

	db, err := sql.Open("mysql", connectStr)
	if err != nil {
		fmt.Printf("failed to open db: %s\n", err)
		containerCleanup()
		os.Exit(1)
	}

	defer func() {
		err := db.Close() // we do not want this connection to stay open
		if err != nil {
			fmt.Printf("failed to close db: %s\n", err)
			containerCleanup()
			os.Exit(1)
		}
	}()

	err = pool.Retry(func() error {
		var err error
		err = db.Ping()
		if err != nil {
			return fmt.Errorf("couldn't validate docker/mysql readiness: %w", err)
		}
		return nil
	})

	if err != nil {
		fmt.Printf("mysql database error: %s\n", err)
		containerCleanup()
		os.Exit(1)
	}

	exitStatus := m.Run()
	containerCleanup()
	os.Exit(exitStatus)
}
