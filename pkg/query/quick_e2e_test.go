package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema/v2"
)

func TestCheck(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	// This stands in for the step of fetching and caching the schema locally.
	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// This stands in for the step that pre-builds query trees from the schema,
	// by iterating through the relationships in the schema and then walking them.
	//
	// In this case, it's a little contrived.
	vande := NewRelationIterator(dsSchema.Definitions()["document"].Relations()["viewer_and_editor"].BaseRelations()[0])
	edit := NewRelationIterator(dsSchema.Definitions()["document"].Relations()["editor"].BaseRelations()[0])
	it := NewIntersection()
	it.addSubIterator(vande)
	it.addSubIterator(edit)

	ctx := &Context{
		Context:   t.Context(),
		Executor:  LocalExecutor{},
		Datastore: ds,
		Revision:  revision,
	}

	relSeq, err := ctx.Check(it, NewObjects("document", "specialplan"), NewObject("user", "multiroleguy").WithEllipses())
	require.NoError(err)

	_, err = CollectAll(relSeq)
	require.NoError(err)
}

func TestBaseIterSubjects(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	// This stands in for the step of fetching and caching the schema locally.
	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	vande := NewRelationIterator(dsSchema.Definitions()["document"].Relations()["viewer_and_editor"].BaseRelations()[0])

	ctx := &Context{
		Context:   t.Context(),
		Executor:  LocalExecutor{},
		Datastore: ds,
		Revision:  revision,
	}

	relSeq, err := ctx.IterSubjects(vande, NewObject("document", "specialplan"))
	require.NoError(err)

	_, err = CollectAll(relSeq)
	require.NoError(err)
}

func TestCheckArrow(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	// This stands in for the step of fetching and caching the schema locally.
	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// This is effectively `permission foo = parent_folder->viewer`
	folders := NewRelationIterator(dsSchema.Definitions()["document"].Relations()["parent"].BaseRelations()[0])
	view := NewRelationIterator(dsSchema.Definitions()["folder"].Relations()["viewer"].BaseRelations()[0])
	it := NewArrow(folders, view)

	ctx := &Context{
		Context:   t.Context(),
		Executor:  LocalExecutor{},
		Datastore: ds,
		Revision:  revision,
	}

	relSeq, err := ctx.Check(it, NewObjects("document", "companyplan"), NewObject("user", "legal").WithEllipses())
	require.NoError(err)

	_, err = CollectAll(relSeq)
	require.NoError(err)
}
