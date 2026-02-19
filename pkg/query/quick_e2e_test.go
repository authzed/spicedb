package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datalayer"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema/v2"
)

func TestCheck(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
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
	docDef, _ := dsSchema.GetTypeDefinition("document")
	vandeRel, _ := docDef.GetRelation("viewer_and_editor")
	vande := NewDatastoreIterator(vandeRel.BaseRelations()[0])
	editRel, _ := docDef.GetRelation("editor")
	edit := NewDatastoreIterator(editRel.BaseRelations()[0])
	it := NewIntersectionIterator(vande, edit)

	ctx := NewLocalContext(t.Context(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(revision)))

	relSeq, err := ctx.Check(it, NewObjects("document", "specialplan"), NewObject("user", "multiroleguy").WithEllipses())
	require.NoError(err)

	_, err = CollectAll(relSeq)
	require.NoError(err)
}

func TestBaseIterSubjects(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	// This stands in for the step of fetching and caching the schema locally.
	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	docDef, _ := dsSchema.GetTypeDefinition("document")
	vandeRel, _ := docDef.GetRelation("viewer_and_editor")
	vande := NewDatastoreIterator(vandeRel.BaseRelations()[0])

	ctx := NewLocalContext(t.Context(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(revision)))

	relSeq, err := ctx.IterSubjects(vande, NewObject("document", "specialplan"), NoObjectFilter())
	require.NoError(err)

	_, err = CollectAll(relSeq)
	require.NoError(err)
}

func TestCheckArrow(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	// This stands in for the step of fetching and caching the schema locally.
	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// This is effectively `permission foo = parent_folder->viewer`
	docDef, _ := dsSchema.GetTypeDefinition("document")
	parentRel, _ := docDef.GetRelation("parent")
	folders := NewDatastoreIterator(parentRel.BaseRelations()[0])
	folderDef, _ := dsSchema.GetTypeDefinition("folder")
	viewRel, _ := folderDef.GetRelation("viewer")
	view := NewDatastoreIterator(viewRel.BaseRelations()[0])
	it := NewArrowIterator(folders, view)

	ctx := NewLocalContext(t.Context(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(revision)))

	relSeq, err := ctx.Check(it, NewObjects("document", "companyplan"), NewObject("user", "legal").WithEllipses())
	require.NoError(err)

	_, err = CollectAll(relSeq)
	require.NoError(err)
}
