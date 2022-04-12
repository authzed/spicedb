//go:build ci
// +build ci

package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/test"
	"github.com/authzed/spicedb/internal/testfixtures"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestPostgresDatastore(t *testing.T) {
	b := testdatastore.NewPostgresBuilder(t)

	test.All(t, test.DatastoreTesterFunc(func(revisionFuzzingTimedelta, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewPostgresDatastore(uri,
				RevisionFuzzingTimedelta(revisionFuzzingTimedelta),
				GCWindow(gcWindow),
				WatchBufferLength(watchBufferLength),
				DebugAnalyzeBeforeStatistics(),
			)
			require.NoError(t, err)
			return ds
		})
		return ds, nil
	}))
}

func TestPostgresDatastoreWithSplit(t *testing.T) {
	b := testdatastore.NewPostgresBuilder(t)
	// Set the split at a VERY small size, to ensure any WithUsersets queries are split.
	test.All(t, test.DatastoreTesterFunc(func(revisionFuzzingTimedelta, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewPostgresDatastore(uri,
				RevisionFuzzingTimedelta(revisionFuzzingTimedelta),
				GCWindow(gcWindow),
				WatchBufferLength(watchBufferLength),
				DebugAnalyzeBeforeStatistics(),
				SplitAtUsersetCount(1), // 1 userset
			)
			require.NoError(t, err)
			return ds
		})

		return ds, nil
	}))
}

func TestPostgresGarbageCollection(t *testing.T) {
	require := require.New(t)

	ds := testdatastore.NewPostgresBuilder(t).NewDatastore(t, func(engine, uri string) datastore.Datastore {
		ds, err := NewPostgresDatastore(uri,
			RevisionFuzzingTimedelta(0),
			GCWindow(time.Millisecond*1),
			WatchBufferLength(1),
		)
		require.NoError(err)
		return ds
	})
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
	tpl := &core.RelationTuple{
		ObjectAndRelation: &core.ObjectAndRelation{
			Namespace: "resource",
			ObjectId:  "someresource",
			Relation:  "reader",
		},
		User: &core.User{UserOneof: &core.User_Userset{Userset: &core.ObjectAndRelation{
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

func TestPostgresTransactionTimestamps(t *testing.T) {
	require := require.New(t)

	ds := testdatastore.NewPostgresBuilder(t).NewDatastore(t, func(engine, uri string) datastore.Datastore {
		ds, err := NewPostgresDatastore(uri,
			RevisionFuzzingTimedelta(0),
			GCWindow(time.Millisecond*1),
			WatchBufferLength(1),
		)
		require.NoError(err)
		return ds
	})
	defer ds.Close()

	ctx := context.Background()
	ok, err := ds.IsReady(ctx)
	require.NoError(err)
	require.True(ok)

	// Setting db default time zone to before UTC
	pgd := ds.(*pgDatastore)
	_, err = pgd.dbpool.Exec(ctx, "SET TIME ZONE 'America/New_York';")
	require.NoError(err)

	// Get timestamp in UTC as reference
	startTimeUTC, err := pgd.getNow(ctx)
	require.NoError(err)

	// Transaction timestamp should not be stored in system time zone
	tx, err := pgd.dbpool.Begin(ctx)
	txId, err := createNewTransaction(ctx, tx)
	err = tx.Commit(ctx)
	require.NoError(err)

	var ts time.Time
	sql, args, err := psql.Select("timestamp").From(tableTransaction).Where(sq.Eq{"id": txId}).ToSql()
	require.NoError(err)
	err = pgd.dbpool.QueryRow(
		datastore.SeparateContextWithTracing(ctx), sql, args...,
	).Scan(&ts)
	require.NoError(err)

	// Transaction timestamp will be before the reference time if it was stored
	// in the default time zone and reinterpreted
	require.True(startTimeUTC.Before(ts))
}

func TestPostgresGarbageCollectionByTime(t *testing.T) {
	require := require.New(t)

	ds := testdatastore.NewPostgresBuilder(t).NewDatastore(t, func(engine, uri string) datastore.Datastore {
		ds, err := NewPostgresDatastore(uri,
			RevisionFuzzingTimedelta(0),
			GCWindow(time.Millisecond*1),
			WatchBufferLength(1),
		)
		require.NoError(err)
		return ds
	})
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
	tpl := &core.RelationTuple{
		ObjectAndRelation: &core.ObjectAndRelation{
			Namespace: "resource",
			ObjectId:  "someresource",
			Relation:  "reader",
		},
		User: &core.User{UserOneof: &core.User_Userset{Userset: &core.ObjectAndRelation{
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

	ds := testdatastore.NewPostgresBuilder(t).NewDatastore(t, func(engine, uri string) datastore.Datastore {
		ds, err := NewPostgresDatastore(uri,
			RevisionFuzzingTimedelta(0),
			GCWindow(time.Millisecond*1),
			WatchBufferLength(1),
		)
		require.NoError(err)
		return ds
	})
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
	var tpls []*core.RelationTuple
	for i := 0; i < chunkRelationshipCount; i++ {
		tpl := &core.RelationTuple{
			ObjectAndRelation: &core.ObjectAndRelation{
				Namespace: "resource",
				ObjectId:  fmt.Sprintf("resource-%d", i),
				Relation:  "reader",
			},
			User: &core.User{UserOneof: &core.User_Userset{Userset: &core.ObjectAndRelation{
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

	ds := testdatastore.NewPostgresBuilder(b).NewDatastore(b, func(engine, uri string) datastore.Datastore {
		ds, err := NewPostgresDatastore(uri,
			RevisionFuzzingTimedelta(0),
			GCWindow(time.Millisecond*1),
			WatchBufferLength(1),
		)
		require.NoError(b, err)
		return ds
	})
	defer ds.Close()
	_, revision := testfixtures.StandardDatastoreWithData(ds, req)

	b.Run("benchmark checks", func(b *testing.B) {
		require := require.New(b)

		for i := 0; i < b.N; i++ {
			iter, err := ds.QueryTuples(context.Background(), &v1.RelationshipFilter{
				ResourceType: testfixtures.DocumentNS.Name,
			}, revision)
			require.NoError(err)

			defer iter.Close()

			for tpl := iter.Next(); tpl != nil; tpl = iter.Next() {
				require.Equal(testfixtures.DocumentNS.Name, tpl.ObjectAndRelation.Namespace)
			}
			require.NoError(iter.Err())
		}
	})
}
