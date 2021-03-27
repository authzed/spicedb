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

	rawDS, err := tester.New(0)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	tRequire := testfixtures.TupleChecker{Require: require, DS: ds}
	docTpl := tuple.Scan(testfixtures.StandardTuples[0])
	require.NotNil(docTpl)
	tRequire.TupleExists(docTpl, revision)

	folderTpl := tuple.Scan(testfixtures.StandardTuples[2])
	require.NotNil(folderTpl)
	tRequire.TupleExists(folderTpl, revision)

	deletedRev, err := ds.DeleteNamespace(testfixtures.DocumentNS.Name)
	require.NoError(err)
	require.Greater(deletedRev, uint64(0))

	_, _, err = ds.ReadNamespace(testfixtures.DocumentNS.Name)
	require.Equal(datastore.ErrNamespaceNotFound, err)

	found, ver, err := ds.ReadNamespace(testfixtures.FolderNS.Name)
	require.NotNil(found)
	require.Greater(ver, uint64(0))
	require.NoError(err)

	deletedRevision, err := ds.SyncRevision(context.Background())
	require.NoError(err)

	iter, err := ds.QueryTuples(testfixtures.DocumentNS.Name, revision).Execute()
	require.Nil(iter)
	require.Equal(datastore.ErrNamespaceNotFound, err)

	tRequire.TupleExists(folderTpl, deletedRevision)
}
