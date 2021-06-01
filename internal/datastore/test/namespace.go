package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestNamespaceDelete(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, disableGC, 1)
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
	require.Equal(datastore.ErrNamespaceNotFound, err)

	found, ver, err := ds.ReadNamespace(ctx, testfixtures.FolderNS.Name)
	require.NotNil(found)
	require.True(ver.GreaterThan(datastore.NoRevision))
	require.NoError(err)

	deletedRevision, err := ds.SyncRevision(ctx)
	require.NoError(err)

	iter, err := ds.QueryTuples(testfixtures.DocumentNS.Name, deletedRevision).Execute(ctx)
	require.NoError(err)
	tRequire.VerifyIteratorResults(iter)

	tRequire.TupleExists(ctx, folderTpl, deletedRevision)
}
