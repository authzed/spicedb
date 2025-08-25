package query_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/schema/v2"
)

func TestBuildTree(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	// This stands in for the step of fetching and caching the schema locally.
	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	it, err := query.BuildIteratorFromSchema(dsSchema, "document", "edit")
	require.NoError(err)

	t.Logf("\n%s", it.Explain().String())

	ctx := &query.Context{
		Context:   t.Context(),
		Datastore: ds,
		Revision:  revision,
	}

	relSeq, err := it.Check(ctx, []string{"specialplan"}, "multiroleguy")
	require.NoError(err)

	rels, err := query.CollectAll(relSeq)
	require.NoError(err)
	t.Log(rels)
}
