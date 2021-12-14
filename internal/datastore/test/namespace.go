package test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
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

	startRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)
	require.True(startRevision.GreaterThanOrEqual(datastore.NoRevision))

	nsDefs, err := ds.ListNamespaces(ctx, startRevision)
	require.NoError(err)
	require.Equal(0, len(nsDefs))

	writtenRev, err := ds.WriteNamespace(ctx, testUserNS)
	require.NoError(err)
	require.True(writtenRev.GreaterThan(startRevision))

	nsDefs, err = ds.ListNamespaces(ctx, writtenRev)
	require.NoError(err)
	require.Equal(1, len(nsDefs))
	require.Equal(testUserNS.Name, nsDefs[0].Name)

	secondWritten, err := ds.WriteNamespace(ctx, testNamespace)
	require.NoError(err)
	require.True(secondWritten.GreaterThan(writtenRev))

	nsDefs, err = ds.ListNamespaces(ctx, secondWritten)
	require.NoError(err)
	require.Equal(2, len(nsDefs))

	_, _, err = ds.ReadNamespace(ctx, testNamespace.Name, writtenRev)
	require.Error(err)

	nsDefs, err = ds.ListNamespaces(ctx, writtenRev)
	require.NoError(err)
	require.Equal(1, len(nsDefs))

	found, createdRev, err := ds.ReadNamespace(ctx, testNamespace.Name, secondWritten)
	require.NoError(err)
	require.True(createdRev.LessThanOrEqual(secondWritten))
	require.True(createdRev.GreaterThan(startRevision))
	foundDiff := cmp.Diff(testNamespace, found, protocmp.Transform())
	require.Empty(foundDiff)

	updatedRevision, err := ds.WriteNamespace(ctx, updatedNamespace)
	require.NoError(err)

	checkUpdated, createdRev, err := ds.ReadNamespace(ctx, testNamespace.Name, updatedRevision)
	require.NoError(err)
	require.True(createdRev.LessThanOrEqual(updatedRevision))
	require.True(createdRev.GreaterThan(startRevision))
	foundUpdated := cmp.Diff(updatedNamespace, checkUpdated, protocmp.Transform())
	require.Empty(foundUpdated)

	checkOld, createdRev, err := ds.ReadNamespace(ctx, testUserNamespace, writtenRev)
	require.NoError(err)
	require.True(createdRev.LessThanOrEqual(writtenRev))
	require.True(createdRev.GreaterThan(startRevision))
	require.Empty(cmp.Diff(testUserNS, checkOld, protocmp.Transform()))

	checkOldList, err := ds.ListNamespaces(ctx, writtenRev)
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

	deletedRev, err := ds.DeleteNamespace(ctx, testfixtures.DocumentNS.Name)
	require.NoError(err)
	require.True(deletedRev.GreaterThan(revision))

	_, _, err = ds.ReadNamespace(ctx, testfixtures.DocumentNS.Name, deletedRev)
	require.True(errors.As(err, &datastore.ErrNamespaceNotFound{}))

	found, nsCreatedRev, err := ds.ReadNamespace(ctx, testfixtures.FolderNS.Name, deletedRev)
	require.NotNil(found)
	require.True(nsCreatedRev.LessThan(deletedRev))
	require.NoError(err)

	allNamespaces, err := ds.ListNamespaces(ctx, deletedRev)
	require.NoError(err)
	for _, ns := range allNamespaces {
		require.NotEqual(testfixtures.DocumentNS.Name, ns.Name, "deleted namespace '%s' should not be in namespace list", ns.Name)
	}

	deletedRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	iter, err := ds.QueryTuples(ctx, &v1.RelationshipFilter{
		ResourceType: testfixtures.DocumentNS.Name,
	}, deletedRevision)
	require.NoError(err)
	tRequire.VerifyIteratorResults(iter)

	tRequire.TupleExists(ctx, folderTpl, deletedRevision)
}
