package memdbtest

import (
	"testing"

	"github.com/authzed/spicedb/internal/datastore"
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

	ds, revision := standardDatastoreWithData(require)

	tRequire := tupleChecker{require, ds}
	docTpl := tuple.Scan(standardTuples[0])
	require.NotNil(docTpl)
	tRequire.tupleExists(docTpl, revision)

	folderTpl := tuple.Scan(standardTuples[2])
	require.NotNil(folderTpl)
	tRequire.tupleExists(folderTpl, revision)

	deletedRev, err := ds.DeleteNamespace(documentNS.Name)
	require.NoError(err)
	require.Greater(deletedRev, uint64(0))

	_, _, err = ds.ReadNamespace(documentNS.Name)
	require.Equal(datastore.ErrNamespaceNotFound, err)

	found, ver, err := ds.ReadNamespace(folderNS.Name)
	require.NotNil(found)
	require.Greater(ver, uint64(0))
	require.NoError(err)

	tRequire.noTupleExists(docTpl, revision)
	tRequire.tupleExists(folderTpl, revision)
}
