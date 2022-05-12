package test

import (
	"context"
	"errors"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	ns "github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	testNamespace = ns.Namespace("foo/bar",
		ns.Relation("editor", nil, ns.AllowedRelation(testUserNS.Name, "...")),
	)

	updatedNamespace = ns.Namespace(testNamespace.Name,
		ns.Relation("reader", nil, ns.AllowedRelation(testUserNS.Name, "...")),
		ns.Relation("editor", nil, ns.AllowedRelation(testUserNS.Name, "...")),
	)
)

// NamespaceWriteTest tests whether or not the requirements for writing
// namespaces hold for a particular datastore.
func NamespaceWriteTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := context.Background()

	startRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)
	require.True(startRevision.GreaterThanOrEqual(datastore.NoRevision))

	nsDefs, err := ds.SnapshotReader(startRevision).ListNamespaces(ctx)
	require.NoError(err)
	require.Equal(0, len(nsDefs))

	writtenRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(testUserNS)
	})
	require.NoError(err)
	require.True(writtenRev.GreaterThan(startRevision))

	nsDefs, err = ds.SnapshotReader(writtenRev).ListNamespaces(ctx)
	require.NoError(err)
	require.Equal(1, len(nsDefs))
	require.Equal(testUserNS.Name, nsDefs[0].Name)

	secondWritten, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(testNamespace)
	})
	require.NoError(err)
	require.True(secondWritten.GreaterThan(writtenRev))

	nsDefs, err = ds.SnapshotReader(secondWritten).ListNamespaces(ctx)
	require.NoError(err)
	require.Equal(2, len(nsDefs))

	_, _, err = ds.SnapshotReader(writtenRev).ReadNamespace(ctx, testNamespace.Name)
	require.Error(err)

	nsDefs, err = ds.SnapshotReader(writtenRev).ListNamespaces(ctx)
	require.NoError(err)
	require.Equal(1, len(nsDefs))

	found, createdRev, err := ds.SnapshotReader(secondWritten).ReadNamespace(ctx, testNamespace.Name)
	require.NoError(err)
	require.True(createdRev.LessThanOrEqual(secondWritten))
	require.True(createdRev.GreaterThan(startRevision))
	foundDiff := cmp.Diff(testNamespace, found, protocmp.Transform())
	require.Empty(foundDiff)

	updatedRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(updatedNamespace)
	})
	require.NoError(err)

	checkUpdated, createdRev, err := ds.SnapshotReader(updatedRevision).ReadNamespace(ctx, testNamespace.Name)
	require.NoError(err)
	require.True(createdRev.LessThanOrEqual(updatedRevision))
	require.True(createdRev.GreaterThan(startRevision))
	foundUpdated := cmp.Diff(updatedNamespace, checkUpdated, protocmp.Transform())
	require.Empty(foundUpdated)

	checkOld, createdRev, err := ds.SnapshotReader(writtenRev).ReadNamespace(ctx, testUserNamespace)
	require.NoError(err)
	require.True(createdRev.LessThanOrEqual(writtenRev))
	require.True(createdRev.GreaterThan(startRevision))
	require.Empty(cmp.Diff(testUserNS, checkOld, protocmp.Transform()))

	checkOldList, err := ds.SnapshotReader(writtenRev).ListNamespaces(ctx)
	require.NoError(err)
	require.Equal(1, len(checkOldList))
	require.Equal(testUserNS.Name, checkOldList[0].Name)
	require.Empty(cmp.Diff(testUserNS, checkOldList[0], protocmp.Transform()))
}

// NamespaceDeleteTest tests whether or not the requirements for deleting
// namespaces hold for a particular datastore.
func NamespaceDeleteTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	tRequire := testfixtures.TupleChecker{Require: require, DS: ds}
	docTpl := tuple.Parse(testfixtures.StandardTuples[0])
	require.NotNil(docTpl)
	tRequire.TupleExists(ctx, docTpl, revision)

	folderTpl := tuple.Parse(testfixtures.StandardTuples[2])
	require.NotNil(folderTpl)
	tRequire.TupleExists(ctx, folderTpl, revision)

	deletedRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.DeleteNamespace(testfixtures.DocumentNS.Name)
	})
	require.NoError(err)
	require.True(deletedRev.GreaterThan(revision))

	_, _, err = ds.SnapshotReader(deletedRev).ReadNamespace(ctx, testfixtures.DocumentNS.Name)
	require.True(errors.As(err, &datastore.ErrNamespaceNotFound{}))

	found, nsCreatedRev, err := ds.SnapshotReader(deletedRev).ReadNamespace(ctx, testfixtures.FolderNS.Name)
	require.NotNil(found)
	require.True(nsCreatedRev.LessThan(deletedRev))
	require.NoError(err)

	allNamespaces, err := ds.SnapshotReader(deletedRev).ListNamespaces(ctx)
	require.NoError(err)
	for _, ns := range allNamespaces {
		require.NotEqual(testfixtures.DocumentNS.Name, ns.Name, "deleted namespace '%s' should not be in namespace list", ns.Name)
	}

	deletedRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	iter, err := ds.SnapshotReader(deletedRevision).QueryRelationships(ctx, &v1.RelationshipFilter{
		ResourceType: testfixtures.DocumentNS.Name,
	})
	require.NoError(err)
	tRequire.VerifyIteratorResults(iter)

	tRequire.TupleExists(ctx, folderTpl, deletedRevision)
}

// EmptyNamespaceDeleteTest tests deleting an empty namespace in the datastore.
func EmptyNamespaceDeleteTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	deletedRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.DeleteNamespace(testfixtures.UserNS.Name)
	})
	require.NoError(err)
	require.True(deletedRev.GreaterThan(revision))

	_, _, err = ds.SnapshotReader(deletedRev).ReadNamespace(ctx, testfixtures.UserNS.Name)
	require.True(errors.As(err, &datastore.ErrNamespaceNotFound{}))
}

// NamespaceCacheKeyTest tests that the datastore returns namespace cache keys that make sense.
func NamespaceCacheKeyTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	key, err := ds.SnapshotReader(revision).NamespaceCacheKey(testfixtures.DocumentNS.Name)
	require.NoError(err)
	require.NotEmpty(key)

	sameKey, err := ds.SnapshotReader(revision).NamespaceCacheKey(testfixtures.DocumentNS.Name)
	require.NoError(err)
	require.Equal(key, sameKey)

	diffNamespace, err := ds.SnapshotReader(revision).NamespaceCacheKey(testfixtures.FolderNS.Name)
	require.NoError(err)
	require.NotEmpty(diffNamespace)
	require.NotEqual(key, diffNamespace)

	newRevision, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		_, err = rwt.NamespaceCacheKey(testfixtures.DocumentNS.Name)
		require.Error(err, "should not be able to get the cache key inside of a read-write transaction")

		err := rwt.WriteNamespace(testfixtures.DocumentNS)
		require.NoError(err)
		return err
	})
	require.NoError(err)

	diffRevision, err := ds.SnapshotReader(newRevision).NamespaceCacheKey(testfixtures.DocumentNS.Name)
	require.NoError(err)
	require.NotEmpty(diffRevision)
	require.NotEqual(key, diffRevision)
}
