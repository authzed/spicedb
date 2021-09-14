package test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/authzed/spicedb/internal/datastore"
	v1 "github.com/authzed/spicedb/internal/proto/authzed/api/v1"
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

func TestNamespaceWrite(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(err)

	isEmpty, err := ds.IsEmpty(context.Background())
	require.NoError(err)
	require.True(isEmpty)

	ctx := context.Background()
	_, err = ds.WriteNamespace(ctx, testUserNS)
	require.NoError(err)

	_, err = ds.WriteNamespace(ctx, testNamespace)
	require.NoError(err)

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

	isEmpty, err = ds.IsEmpty(context.Background())
	require.NoError(err)
	require.False(isEmpty)
}

func TestNamespaceDelete(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	tRequire := testfixtures.TupleChecker{Require: require, DS: ds}
	docTpl := tuple.Scan(testfixtures.StandardTuples[0])
	require.NotNil(docTpl)
	tRequire.TupleExists(ctx, docTpl, revision)

	folderTpl := tuple.Scan(testfixtures.StandardTuples[2])
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

	iter, err := ds.QueryTuples(&v1.ObjectFilter{
		ObjectType: testfixtures.DocumentNS.Name,
	}, deletedRevision).Execute(ctx)
	require.NoError(err)
	tRequire.VerifyIteratorResults(iter)

	tRequire.TupleExists(ctx, folderTpl, deletedRevision)
}
