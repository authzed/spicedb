package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestCheck(t *testing.T) {
	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(t, rawDS)

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
		WithRevisionedReader(datalayer.NewDataLayer(ds).SnapshotReader(revision, datalayer.NoSchemaHashForTesting)))

	_, err = ctx.Check(it, NewObject("document", "specialplan"), NewObject("user", "multiroleguy").WithEllipses())
	require.NoError(err)
}

func TestBaseIterSubjects(t *testing.T) {
	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(t, rawDS)

	// This stands in for the step of fetching and caching the schema locally.
	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	docDef, _ := dsSchema.GetTypeDefinition("document")
	vandeRel, _ := docDef.GetRelation("viewer_and_editor")
	vande := NewDatastoreIterator(vandeRel.BaseRelations()[0])

	ctx := NewLocalContext(t.Context(),
		WithRevisionedReader(datalayer.NewDataLayer(ds).SnapshotReader(revision, datalayer.NoSchemaHashForTesting)))

	relSeq, err := ctx.IterSubjects(vande, NewObject("document", "specialplan"), NoObjectFilter())
	require.NoError(err)

	_, err = CollectAll(relSeq)
	require.NoError(err)
}

func TestCheckArrow(t *testing.T) {
	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(t, rawDS)

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
		WithRevisionedReader(datalayer.NewDataLayer(ds).SnapshotReader(revision, datalayer.NoSchemaHashForTesting)))

	_, err = ctx.Check(it, NewObject("document", "companyplan"), NewObject("user", "legal").WithEllipses())
	require.NoError(err)
}

// cyclicSchema is a simple schema with a recursive structure
// that is relatively easy to make cyclic.
var cyclicSchema = `
definition user {}

definition folder {
	relation viewer: user
	relation parent: folder
	permission view = parent->view + viewer
}

definition resource {
	relation folder: folder
	permission view = folder->view
}`

// TestCyclicLookupResources verifies that the Query Planner
// can handle an LR that would have caused the old logic to
// return a MaxDepthExceeded error.
func TestCyclicLookupResources(t *testing.T) {
	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ctx := t.Context()

	// The folders are connected to each other and to a user,
	// but there are no resources connected to either.
	relationships := []tuple.Relationship{
		tuple.MustParse("folder:a#viewer@user:tom"),
		tuple.MustParse("folder:a#parent@folder:b"),
		tuple.MustParse("folder:b#parent@folder:a"),
	}
	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(t, rawDS, cyclicSchema, relationships)

	dsSchema, err := ReadSchema(ctx, ds, revision)
	require.NoError(err)

	canonicalOutline, err := BuildOutlineFromSchema(dsSchema, "resource", "view")
	require.NoError(err)
	it, err := canonicalOutline.Compile()
	require.NoError(err)

	subject := NewObject("user", "tom").WithEllipses()
	filterResourceType := NoObjectFilter()

	reader := NewQueryDatastoreReader(datalayer.NewDataLayer(ds).SnapshotReader(revision, datalayer.NoSchemaHashForTesting))

	opts := []ContextOption{
		WithReader(reader),
		WithMaxRecursionDepth(defaultMaxRecursionDepth),
	}

	queryCtx := NewLocalContext(ctx, opts...)
	paths, err := queryCtx.IterResources(it, subject, filterResourceType)
	require.NoError(err, "should not reach max depth")
	results, err := CollectAll(paths)
	require.NoError(err)
	require.Empty(results, "no results expected for query")
}

// TestCyclicLookupResources verifies that the Query Planner
// can handle an LS that would have caused the old logic to
// return a MaxDepthExceeded error.
func TestCyclicLookupSubjects(t *testing.T) {
	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ctx := t.Context()

	// The folders are connected to each other, and a resource
	// is connected to a folder, but there are no connections to
	// a user.
	relationships := []tuple.Relationship{
		tuple.MustParse("folder:a#parent@folder:b"),
		tuple.MustParse("folder:b#parent@folder:a"),
		tuple.MustParse("resource:foo#parent@folder:a"),
	}
	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(t, rawDS, cyclicSchema, relationships)

	dsSchema, err := ReadSchema(ctx, ds, revision)
	require.NoError(err)

	canonicalOutline, err := BuildOutlineFromSchema(dsSchema, "resource", "view")
	require.NoError(err)
	it, err := canonicalOutline.Compile()
	require.NoError(err)

	object := NewObject("resource", "foo")
	filterSubjectType := NewType("user")

	reader := NewQueryDatastoreReader(datalayer.NewDataLayer(ds).SnapshotReader(revision, datalayer.NoSchemaHashForTesting))

	opts := []ContextOption{
		WithReader(reader),
		WithMaxRecursionDepth(defaultMaxRecursionDepth),
	}
	queryCtx := NewLocalContext(ctx, opts...)

	paths, err := queryCtx.IterSubjects(it, object, filterSubjectType)
	require.NoError(err, "should not reach max depth")
	results, err := CollectAll(paths)
	require.NoError(err)
	require.Empty(results, "no results expected for query")
}

// TestCyclicCheck verifies that the Query Planner
// can handle an Check that would have caused the old logic to
// return a MaxDepthExceeded error.
// NOTE: This is basically the same setup as the LookupSubjects
// test, because the old LS implementation was using the check
// path.
func TestCyclicCheck(t *testing.T) {
	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ctx := t.Context()

	// The folders are connected to each other, and a resource
	// is connected to a folder, but there are no connections to
	// a user.
	relationships := []tuple.Relationship{
		tuple.MustParse("folder:a#parent@folder:b"),
		tuple.MustParse("folder:b#parent@folder:a"),
		tuple.MustParse("resource:foo#parent@folder:a"),
	}
	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(t, rawDS, cyclicSchema, relationships)

	dsSchema, err := ReadSchema(ctx, ds, revision)
	require.NoError(err)

	canonicalOutline, err := BuildOutlineFromSchema(dsSchema, "resource", "view")
	require.NoError(err)
	it, err := canonicalOutline.Compile()
	require.NoError(err)

	object := NewObject("resource", "foo")
	subject := NewObject("user", "tom").WithEllipses()

	reader := NewQueryDatastoreReader(datalayer.NewDataLayer(ds).SnapshotReader(revision, datalayer.NoSchemaHashForTesting))

	opts := []ContextOption{
		WithReader(reader),
		WithMaxRecursionDepth(defaultMaxRecursionDepth),
	}
	queryCtx := NewLocalContext(ctx, opts...)

	path, err := queryCtx.Check(it, object, subject)
	require.NoError(err, "should not reach max depth")
	require.Nil(path, "no results expected for query")
}

// ReadSchema reads all namespace and caveat definitions from the datastore at
// the given revision and returns the compiled schema.
// NOTE: This is a duplicate of the logic in the benchmark package. Find a common
// place to put them and dedupe
// TODO: It seems like there ought to be a better way to construct this schema object
// after reading from the datastore.
func ReadSchema(ctx context.Context, ds datastore.Datastore, rev datastore.Revision) (*schema.Schema, error) {
	dl := datalayer.NewDataLayer(ds)

	revision, hash, err := dl.HeadRevision(ctx)
	if err != nil {
		return nil, err
	}

	reader := dl.SnapshotReader(revision, hash)
	schemaReader, err := reader.ReadSchema(ctx)
	if err != nil {
		return nil, err
	}

	caveats, err := schemaReader.ListAllCaveatDefinitions(ctx)
	if err != nil {
		return nil, err
	}

	defs, err := schemaReader.ListAllTypeDefinitions(ctx)
	if err != nil {
		return nil, err
	}

	return schema.BuildSchemaFromDefinitions(
		slicez.Map(defs, func(def datastore.RevisionedTypeDefinition) *corev1.NamespaceDefinition {
			return def.Definition
		}),
		slicez.Map(caveats, func(caveat datastore.RevisionedCaveat) *corev1.CaveatDefinition {
			return caveat.Definition
		}),
	)
}
