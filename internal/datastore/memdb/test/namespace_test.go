package memdbtest

import (
	"testing"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/stretchr/testify/require"
)

var documentNamespace = &pb.NamespaceDefinition{
	Name: "document",
	Relation: []*pb.Relation{
		{
			Name: "owner",
		},
	},
}

func TestDelete(t *testing.T) {
	require := require.New(t)

	ds, revision := testfixtures.StandardDatastoreWithData(require)

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

	tRequire.NoTupleExists(docTpl, revision)
	tRequire.TupleExists(folderTpl, revision)
}
