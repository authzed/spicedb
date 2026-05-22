package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datalayer"
)

// arrowWildcardSchema is the schema used by the arrow wildcard tests:
//
//	document.parent may be a concrete folder OR the folder:* wildcard.
//	folder.viewer has concrete users.
//	view = parent->viewer
//
// The wildcard on document.parent exercises the new wildcard-inversion branch
// in ArrowIterator.CheckImpl and the skip branch in ArrowIterator.IterSubjectsImpl.
const arrowWildcardSchema = `
	definition user {}
	definition folder {
		relation viewer: user
	}
	definition document {
		relation parent: folder | folder:*
		permission view = parent->viewer
	}
`

// TestArrowIterator_WildcardInversion_Check verifies that when the left side
// of an arrow yields a wildcard subject (folder:*), CheckImpl inverts and
// calls IterResources on the right side to find any intermediate of the
// matching type that satisfies the target subject.
func TestArrowIterator_WildcardInversion_Check(t *testing.T) {
	ds, rev, dsSchema := setupTestDB(t, arrowWildcardSchema,
		// doc1 grants parent via wildcard — any folder matches.
		"document:doc1#parent@folder:*",
		// A concrete folder where alice is a viewer.
		"folder:shared#viewer@user:alice",
	)

	docDef, _ := dsSchema.GetTypeDefinition("document")
	parentRel, _ := docDef.GetRelation("parent")
	folderDef, _ := dsSchema.GetTypeDefinition("folder")
	viewerRel, _ := folderDef.GetRelation("viewer")

	// parent has two base relations: concrete folder and folder:*. Union them.
	left := NewUnionIterator(
		NewDatastoreIterator(parentRel.BaseRelations()[0]),
		NewDatastoreIterator(parentRel.BaseRelations()[1]),
	)
	right := NewDatastoreIterator(viewerRel.BaseRelations()[0])
	arrow := NewArrowIterator(left, right)

	ctx := NewLocalContext(t.Context(),
		WithRevisionedReader(datalayer.NewDataLayer(ds).SnapshotReader(rev, datalayer.NoSchemaHashForTesting)),
		WithTraceLogger(NewTraceLogger()),
	)

	path, err := ctx.Check(arrow, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
	require.NoError(t, err)
	if t.Failed() && ctx.TraceLogger != nil {
		t.Logf("Trace:\n%s", ctx.TraceLogger.DumpTrace())
	}
	require.NotNil(t, path, "wildcard left should be satisfied by inversion finding folder:shared#viewer@user:alice")
	require.Equal(t, "alice", path.Subject.ObjectID)
	require.Equal(t, "doc1", path.Resource.ObjectID)
}

// TestArrowIterator_WildcardInversion_Check_NoConcreteMatch verifies the negative
// case: wildcard on the left, but no concrete intermediate has the target subject.
func TestArrowIterator_WildcardInversion_Check_NoConcreteMatch(t *testing.T) {
	ds, rev, dsSchema := setupTestDB(t, arrowWildcardSchema,
		"document:doc1#parent@folder:*",
		"folder:shared#viewer@user:alice",
	)

	docDef, _ := dsSchema.GetTypeDefinition("document")
	parentRel, _ := docDef.GetRelation("parent")
	folderDef, _ := dsSchema.GetTypeDefinition("folder")
	viewerRel, _ := folderDef.GetRelation("viewer")

	left := NewUnionIterator(
		NewDatastoreIterator(parentRel.BaseRelations()[0]),
		NewDatastoreIterator(parentRel.BaseRelations()[1]),
	)
	right := NewDatastoreIterator(viewerRel.BaseRelations()[0])
	arrow := NewArrowIterator(left, right)

	ctx := NewLocalContext(t.Context(),
		WithRevisionedReader(datalayer.NewDataLayer(ds).SnapshotReader(rev, datalayer.NoSchemaHashForTesting)),
	)

	// bob has no viewer relationship on any folder.
	path, err := ctx.Check(arrow, NewObject("document", "doc1"), NewObject("user", "bob").WithEllipses())
	require.NoError(t, err)
	require.Nil(t, path)
}

// TestArrowIterator_WildcardSkip_IterSubjects verifies that when the left side
// yields a wildcard, ArrowIterator.IterSubjectsImpl skips it rather than trying
// to expand. This mirrors the traditional dispatch path's behavior — wildcard
// tupleset entries are not followed through arrows during subject enumeration.
func TestArrowIterator_WildcardSkip_IterSubjects(t *testing.T) {
	ds, rev, dsSchema := setupTestDB(t, arrowWildcardSchema,
		"document:doc1#parent@folder:*",
		"document:doc1#parent@folder:concrete",
		"folder:concrete#viewer@user:alice",
		"folder:shared#viewer@user:bob", // unreachable via concrete parent, only via wildcard
	)

	docDef, _ := dsSchema.GetTypeDefinition("document")
	parentRel, _ := docDef.GetRelation("parent")
	folderDef, _ := dsSchema.GetTypeDefinition("folder")
	viewerRel, _ := folderDef.GetRelation("viewer")

	left := NewUnionIterator(
		NewDatastoreIterator(parentRel.BaseRelations()[0]),
		NewDatastoreIterator(parentRel.BaseRelations()[1]),
	)
	right := NewDatastoreIterator(viewerRel.BaseRelations()[0])
	arrow := NewArrowIterator(left, right)

	ids := collectSubjectIDs(t, t.Context(), ds, rev, arrow, NewObject("document", "doc1"))
	// Only alice (via folder:concrete) — bob requires wildcard expansion which is skipped.
	require.ElementsMatch(t, []string{"alice"}, ids)
}

// TestIntersectionArrowIterator_WildcardInversion_Check mirrors the Arrow test
// for intersection arrows. The `all` arrow requires ALL left subjects to satisfy
// the right side; a wildcard on the left uses the same IterResources inversion
// strategy as the regular arrow.
func TestIntersectionArrowIterator_WildcardInversion_Check(t *testing.T) {
	ds, rev, dsSchema := setupTestDB(t, arrowWildcardSchema,
		// Only the wildcard parent exists — no concrete parents.
		"document:doc1#parent@folder:*",
		"folder:shared#viewer@user:alice",
	)

	docDef, _ := dsSchema.GetTypeDefinition("document")
	parentRel, _ := docDef.GetRelation("parent")
	folderDef, _ := dsSchema.GetTypeDefinition("folder")
	viewerRel, _ := folderDef.GetRelation("viewer")

	left := NewUnionIterator(
		NewDatastoreIterator(parentRel.BaseRelations()[0]),
		NewDatastoreIterator(parentRel.BaseRelations()[1]),
	)
	right := NewDatastoreIterator(viewerRel.BaseRelations()[0])
	iarrow := NewIntersectionArrowIterator(left, right)

	ctx := NewLocalContext(t.Context(),
		WithRevisionedReader(datalayer.NewDataLayer(ds).SnapshotReader(rev, datalayer.NoSchemaHashForTesting)),
	)

	path, err := ctx.Check(iarrow, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
	require.NoError(t, err)
	require.NotNil(t, path, "wildcard left should be satisfied via IterResources inversion")
}

// TestIntersectionArrowIterator_WildcardSkip_IterSubjects verifies the
// parallel skip branch in IntersectionArrowIterator.IterSubjectsImpl.
func TestIntersectionArrowIterator_WildcardSkip_IterSubjects(t *testing.T) {
	ds, rev, dsSchema := setupTestDB(t, arrowWildcardSchema,
		"document:doc1#parent@folder:*",
		"document:doc1#parent@folder:concrete",
		"folder:concrete#viewer@user:alice",
	)

	docDef, _ := dsSchema.GetTypeDefinition("document")
	parentRel, _ := docDef.GetRelation("parent")
	folderDef, _ := dsSchema.GetTypeDefinition("folder")
	viewerRel, _ := folderDef.GetRelation("viewer")

	left := NewUnionIterator(
		NewDatastoreIterator(parentRel.BaseRelations()[0]),
		NewDatastoreIterator(parentRel.BaseRelations()[1]),
	)
	right := NewDatastoreIterator(viewerRel.BaseRelations()[0])
	iarrow := NewIntersectionArrowIterator(left, right)

	ids := collectSubjectIDs(t, t.Context(), ds, rev, iarrow, NewObject("document", "doc1"))
	// Wildcard parent is skipped; only folder:concrete is considered, so alice surfaces.
	require.ElementsMatch(t, []string{"alice"}, ids)
}
