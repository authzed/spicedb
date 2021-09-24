package test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	ns "github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	testNamespace = ns.Namespace("test/test",
		ns.Relation("editor", nil, ns.RelationReference(testUserNS.Name, "...")),
	)

	updatedNamespace = ns.Namespace(testNamespace.Name,
		ns.Relation("reader", nil, ns.RelationReference(testUserNS.Name, "...")),
		ns.Relation("editor", nil, ns.RelationReference(testUserNS.Name, "...")),
	)
)

// NamespaceWriteTest tests whether or not the requirements for writing
// namespaces hold for a particular datastore.
func NamespaceWriteTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := context.Background()

	nsDefs, err := ds.ListNamespaces(ctx)
	require.NoError(err)
	require.Equal(0, len(nsDefs))

	_, err = ds.WriteNamespace(ctx, testUserNS)
	require.NoError(err)

	nsDefs, err = ds.ListNamespaces(ctx)
	require.NoError(err)
	require.Equal(1, len(nsDefs))
	require.Equal(testUserNS.Name, nsDefs[0].Name)

	_, err = ds.WriteNamespace(ctx, testNamespace)
	require.NoError(err)

	nsDefs, err = ds.ListNamespaces(ctx)
	require.NoError(err)
	require.Equal(2, len(nsDefs))

	found, _, err := ds.ReadNamespace(ctx, testNamespace.Name)
	require.NoError(err)
	foundDiff := cmp.Diff(testNamespace, found, protocmp.Transform())
	require.Empty(foundDiff)

	_, err = ds.WriteNamespace(ctx, updatedNamespace)
	require.NoError(err)

	checkUpdated, _, err := ds.ReadNamespace(ctx, testNamespace.Name)
	require.NoError(err)
	foundUpdated := cmp.Diff(updatedNamespace, checkUpdated, protocmp.Transform())
	require.Empty(foundUpdated)
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

	deletedRev, err := ds.DeleteNamespace(ctx, testfixtures.DocumentNS.Name)
	require.NoError(err)
	require.True(deletedRev.GreaterThan(datastore.NoRevision))

	_, _, err = ds.ReadNamespace(ctx, testfixtures.DocumentNS.Name)
	require.True(errors.As(err, &datastore.ErrNamespaceNotFound{}))

	found, ver, err := ds.ReadNamespace(ctx, testfixtures.FolderNS.Name)
	require.NotNil(found)
	require.True(ver.GreaterThan(datastore.NoRevision))
	require.NoError(err)

	deletedRevision, err := ds.SyncRevision(ctx)
	require.NoError(err)

	iter, err := ds.QueryTuples(datastore.TupleQueryResourceFilter{
		ResourceType: testfixtures.DocumentNS.Name,
	}, deletedRevision).Execute(ctx)
	require.NoError(err)
	tRequire.VerifyIteratorResults(iter)

	tRequire.TupleExists(ctx, folderTpl, deletedRevision)
}
