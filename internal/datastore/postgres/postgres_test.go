//go:build ci
// +build ci

package postgres

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/alecthomas/units"
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/internal/datastore/test"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/secrets"
	"github.com/authzed/spicedb/pkg/tuple"
)

type sqlTest struct {
	dbpool                    *pgxpool.Pool
	port                      string
	creds                     string
	splitAtEstimatedQuerySize units.Base2Bytes
	cleanup                   func()
}

var postgresContainer = &dockertest.RunOptions{
	Repository: "postgres",
	Tag:        "9.6",
	Env:        []string{"POSTGRES_PASSWORD=secret", "POSTGRES_DB=defaultdb"},
}

func (st sqlTest) New(revisionFuzzingTimedelta, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
	uniquePortion, err := secrets.TokenHex(4)
	if err != nil {
		return nil, err
	}

	newDBName := "db" + uniquePortion

	_, err = st.dbpool.Exec(context.Background(), "CREATE DATABASE "+newDBName)
	if err != nil {
		return nil, fmt.Errorf("unable to create database: %w", err)
	}

	connectStr := fmt.Sprintf(
		"postgres://%s@localhost:%s/%s?sslmode=disable",
		st.creds,
		st.port,
		newDBName,
	)

	migrationDriver, err := migrations.NewAlembicPostgresDriver(connectStr)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize migration engine: %w", err)
	}

	err = migrations.DatabaseMigrations.Run(migrationDriver, migrate.Head, migrate.LiveRun)
	if err != nil {
		return nil, fmt.Errorf("unable to migrate database: %w", err)
	}

	return NewPostgresDatastore(
		connectStr,
		RevisionFuzzingTimedelta(revisionFuzzingTimedelta),
		GCWindow(gcWindow),
		GCInterval(0*time.Second), // Disable auto GC
		WatchBufferLength(watchBufferLength),
		SplitAtEstimatedQuerySize(st.splitAtEstimatedQuerySize),
	)
}

func TestPostgresDatastore(t *testing.T) {
	tester := newTester(postgresContainer, "postgres:secret", 5432)
	defer tester.cleanup()

	test.All(t, tester)
}

func TestPostgresDatastoreWithSplit(t *testing.T) {
	// Set the split at a VERY small size, to ensure any WithUsersets queries are split.
	tester := newTester(postgresContainer, "postgres:secret", 5432)
	tester.splitAtEstimatedQuerySize = 1 // bytes
	defer tester.cleanup()

	test.All(t, tester)
}

func TestPostgresGarbageCollection(t *testing.T) {
	require := require.New(t)

	tester := newTester(postgresContainer, "postgres:secret", 5432)
	defer tester.cleanup()

	ds, err := tester.New(0, time.Millisecond*1, 1)
	require.NoError(err)
	defer ds.Close()

	ctx := context.Background()
	ok, err := ds.IsReady(ctx)
	require.NoError(err)
	require.True(ok)

	// Write basic namespaces.
	_, err = ds.WriteNamespace(ctx, namespace.Namespace(
		"resource",
		namespace.Relation("reader", nil),
	))
	require.NoError(err)

	writtenAt, err := ds.WriteNamespace(ctx, namespace.Namespace("user"))
	require.NoError(err)

	// Run GC at the transaction and ensure no relationships are removed.
	pds := ds.(*pgDatastore)

	relsDeleted, _, err := pds.collectGarbageForTransaction(ctx, uint64(writtenAt.IntPart()))
	require.Equal(int64(0), relsDeleted)
	require.NoError(err)

	// Write a relationship.
	tpl := &v0.RelationTuple{
		ObjectAndRelation: &v0.ObjectAndRelation{
			Namespace: "resource",
			ObjectId:  "someresource",
			Relation:  "reader",
		},
		User: &v0.User{UserOneof: &v0.User_Userset{Userset: &v0.ObjectAndRelation{
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
	require.NoError(err)

	// Run GC at the transaction and ensure no relationships are removed, but 1 transaction (the previous write namespace) is.
	relsDeleted, transactionsDeleted, err := pds.collectGarbageForTransaction(ctx, uint64(relWrittenAt.IntPart()))
	require.Equal(int64(0), relsDeleted)
	require.Equal(int64(1), transactionsDeleted)
	require.NoError(err)

	// Run GC again and ensure there are no changes.
	relsDeleted, transactionsDeleted, err = pds.collectGarbageForTransaction(ctx, uint64(relWrittenAt.IntPart()))
	require.Equal(int64(0), relsDeleted)
	require.Equal(int64(0), transactionsDeleted)
	require.NoError(err)

	// Ensure the relationship is still present.
	tRequire := testfixtures.TupleChecker{Require: require, DS: ds}
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
	require.NoError(err)

	// Run GC at the transaction and ensure the (older copy of the) relationship is removed, as well as 1 transaction (the write).
	relsDeleted, transactionsDeleted, err = pds.collectGarbageForTransaction(ctx, uint64(relOverwrittenAt.IntPart()))
	require.Equal(int64(1), relsDeleted)
	require.Equal(int64(1), transactionsDeleted)
	require.NoError(err)

	// Run GC again and ensure there are no changes.
	relsDeleted, transactionsDeleted, err = pds.collectGarbageForTransaction(ctx, uint64(relOverwrittenAt.IntPart()))
	require.Equal(int64(0), relsDeleted)
	require.Equal(int64(0), transactionsDeleted)
	require.NoError(err)

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
	require.NoError(err)

	// Ensure the relationship is gone.
	tRequire.NoTupleExists(ctx, tpl, relDeletedAt)

	// Run GC at the transaction and ensure the relationship is removed, as well as 1 transaction (the overwrite).
	relsDeleted, transactionsDeleted, err = pds.collectGarbageForTransaction(ctx, uint64(relDeletedAt.IntPart()))
	require.Equal(int64(1), relsDeleted)
	require.Equal(int64(1), transactionsDeleted)
	require.NoError(err)

	// Run GC again and ensure there are no changes.
	relsDeleted, transactionsDeleted, err = pds.collectGarbageForTransaction(ctx, uint64(relDeletedAt.IntPart()))
	require.Equal(int64(0), relsDeleted)
	require.Equal(int64(0), transactionsDeleted)
	require.NoError(err)

	// Write a the relationship a few times.
	_, err = ds.WriteTuples(
		ctx,
		nil,
		[]*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: relationship,
		}},
	)
	require.NoError(err)

	_, err = ds.WriteTuples(
		ctx,
		nil,
		[]*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: relationship,
		}},
	)
	require.NoError(err)

	relLastWriteAt, err := ds.WriteTuples(
		ctx,
		nil,
		[]*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: relationship,
		}},
	)
	require.NoError(err)

	// Run GC at the transaction and ensure the older copies of the relationships are removed,
	// as well as the 2 older write transactions and the older delete transaction.
	relsDeleted, transactionsDeleted, err = pds.collectGarbageForTransaction(ctx, uint64(relLastWriteAt.IntPart()))
	require.Equal(int64(2), relsDeleted)
	require.Equal(int64(3), transactionsDeleted)
	require.NoError(err)

	// Ensure the relationship is still present.
	tRequire.TupleExists(ctx, tpl, relLastWriteAt)
}

func TestPostgresGarbageCollectionByTime(t *testing.T) {
	require := require.New(t)

	tester := newTester(postgresContainer, "postgres:secret", 5432)
	defer tester.cleanup()

	ds, err := tester.New(0, time.Millisecond*1, 1)
	require.NoError(err)
	defer ds.Close()

	ctx := context.Background()
	ok, err := ds.IsReady(ctx)
	require.NoError(err)
	require.True(ok)

	// Write basic namespaces.
	_, err = ds.WriteNamespace(ctx, namespace.Namespace(
		"resource",
		namespace.Relation("reader", nil),
	))
	require.NoError(err)

	_, err = ds.WriteNamespace(ctx, namespace.Namespace("user"))
	require.NoError(err)

	pds := ds.(*pgDatastore)

	// Sleep 1ms to ensure GC will delete the previous transaction.
	time.Sleep(1 * time.Millisecond)

	// Write a relationship.
	tpl := &v0.RelationTuple{
		ObjectAndRelation: &v0.ObjectAndRelation{
			Namespace: "resource",
			ObjectId:  "someresource",
			Relation:  "reader",
		},
		User: &v0.User{UserOneof: &v0.User_Userset{Userset: &v0.ObjectAndRelation{
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
	require.NoError(err)

	// Run GC and ensure only transactions were removed.
	afterWrite, err := pds.getNow(ctx)
	require.NoError(err)

	relsDeleted, transactionsDeleted, err := pds.collectGarbageBefore(ctx, afterWrite)
	require.Equal(int64(0), relsDeleted)
	require.True(transactionsDeleted > 0)
	require.NoError(err)

	// Ensure the relationship is still present.
	tRequire := testfixtures.TupleChecker{Require: require, DS: ds}
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
	require.NoError(err)

	// Run GC and ensure the relationship is removed.
	afterDelete, err := pds.getNow(ctx)
	require.NoError(err)

	relsDeleted, transactionsDeleted, err = pds.collectGarbageBefore(ctx, afterDelete)
	require.Equal(int64(1), relsDeleted)
	require.Equal(int64(1), transactionsDeleted)
	require.NoError(err)

	// Ensure the relationship is still not present.
	tRequire.NoTupleExists(ctx, tpl, relDeletedAt)
}

const chunkRelationshipCount = 2000

func TestPostgresChunkedGarbageCollection(t *testing.T) {
	require := require.New(t)

	tester := newTester(postgresContainer, "postgres:secret", 5432)
	defer tester.cleanup()

	ds, err := tester.New(0, time.Millisecond*1, 1)
	require.NoError(err)
	defer ds.Close()

	ctx := context.Background()
	ok, err := ds.IsReady(ctx)
	require.NoError(err)
	require.True(ok)

	// Write basic namespaces.
	_, err = ds.WriteNamespace(ctx, namespace.Namespace(
		"resource",
		namespace.Relation("reader", nil),
	))
	require.NoError(err)

	_, err = ds.WriteNamespace(ctx, namespace.Namespace("user"))
	require.NoError(err)

	pds := ds.(*pgDatastore)

	// Prepare relationships to write.
	var tpls []*v0.RelationTuple
	for i := 0; i < chunkRelationshipCount; i++ {
		tpl := &v0.RelationTuple{
			ObjectAndRelation: &v0.ObjectAndRelation{
				Namespace: "resource",
				ObjectId:  fmt.Sprintf("resource-%d", i),
				Relation:  "reader",
			},
			User: &v0.User{UserOneof: &v0.User_Userset{Userset: &v0.ObjectAndRelation{
				Namespace: "user",
				ObjectId:  "someuser",
				Relation:  "...",
			}}},
		}
		tpls = append(tpls, tpl)
	}

	// Write a large number of relationships.
	var updates []*v1.RelationshipUpdate
	for _, tpl := range tpls {
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
	require.NoError(err)

	// Ensure the relationships were written.
	tRequire := testfixtures.TupleChecker{Require: require, DS: ds}
	for _, tpl := range tpls {
		tRequire.TupleExists(ctx, tpl, writtenAt)
	}

	// Run GC and ensure only transactions were removed.
	afterWrite, err := pds.getNow(ctx)
	require.NoError(err)

	relsDeleted, transactionsDeleted, err := pds.collectGarbageBefore(ctx, afterWrite)
	require.Equal(int64(0), relsDeleted)
	require.True(transactionsDeleted > 0)
	require.NoError(err)

	// Sleep to ensure the relationships will GC.
	time.Sleep(1 * time.Millisecond)

	// Delete all the relationships.
	var deletes []*v1.RelationshipUpdate
	for _, tpl := range tpls {
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
	require.NoError(err)

	// Ensure the relationships were deleted.
	for _, tpl := range tpls {
		tRequire.NoTupleExists(ctx, tpl, deletedAt)
	}

	// Sleep to ensure GC.
	time.Sleep(1 * time.Millisecond)

	// Run GC and ensure all the stale relationships are removed.
	afterDelete, err := pds.getNow(ctx)
	require.NoError(err)

	relsDeleted, transactionsDeleted, err = pds.collectGarbageBefore(ctx, afterDelete)
	require.Equal(int64(chunkRelationshipCount), relsDeleted)
	require.Equal(int64(1), transactionsDeleted)
	require.NoError(err)
}

func BenchmarkPostgresQuery(b *testing.B) {
	req := require.New(b)

	tester := newTester(postgresContainer, "postgres:secret", 5432)
	defer tester.cleanup()

	ds, err := tester.New(0, 24*time.Hour, 1)
	req.NoError(err)

	_, revision := testfixtures.StandardDatastoreWithData(ds, req)

	b.Run("benchmark checks", func(b *testing.B) {
		require := require.New(b)

		for i := 0; i < b.N; i++ {
			iter, err := ds.QueryTuples(datastore.TupleQueryResourceFilter{
				ResourceType: testfixtures.DocumentNS.Name,
			}, revision).Execute(context.Background())
			require.NoError(err)

			defer iter.Close()

			for tpl := iter.Next(); tpl != nil; tpl = iter.Next() {
				require.Equal(testfixtures.DocumentNS.Name, tpl.ObjectAndRelation.Namespace)
			}
			require.NoError(iter.Err())
		}
	})
}

func newTester(containerOpts *dockertest.RunOptions, creds string, portNum uint16) *sqlTest {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resource, err := pool.RunWithOptions(containerOpts)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	var dbpool *pgxpool.Pool
	port := resource.GetPort(fmt.Sprintf("%d/tcp", portNum))
	if err = pool.Retry(func() error {
		var err error
		dbpool, err = pgxpool.Connect(context.Background(), fmt.Sprintf("postgres://%s@localhost:%s/defaultdb?sslmode=disable", creds, port))
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	cleanup := func() {
		// When you're done, kill and remove the container
		if err = pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}

	return &sqlTest{dbpool: dbpool, port: port, cleanup: cleanup, creds: creds, splitAtEstimatedQuerySize: common.DefaultSplitAtEstimatedQuerySize}
}
