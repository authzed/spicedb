package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

// setupTestDB compiles a schema, writes it and relationships to a memdb datastore,
// and returns the datastore, revision, and compiled schema.
func setupTestDB(t *testing.T, schemaText string, rels ...string) (datastore.Datastore, datastore.Revision, *schema.Schema) {
	t.Helper()
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(t, err)

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	ctx := t.Context()
	// Write the schema
	_, err = datalayer.WriteStoredSchemaForTest(ctx, rawDS, schemaText)
	require.NoError(t, err)

	parsedRels := make([]tuple.Relationship, 0, len(rels))
	for _, r := range rels {
		parsedRels = append(parsedRels, tuple.MustParse(r))
	}

	revision, err := common.WriteRelationships(ctx, rawDS, tuple.UpdateOperationCreate, parsedRels...)
	require.NoError(t, err)

	dsSchema, err := schema.BuildSchemaFromDefinitions(compiled.ObjectDefinitions, nil)
	require.NoError(t, err)

	return rawDS, revision, dsSchema
}

// collectSubjectIDs runs IterSubjects on the given iterator and returns the subject IDs.
func collectSubjectIDs(t *testing.T, ctx context.Context, ds datastore.Datastore, rev datastore.Revision, it Iterator, resource Object) []string {
	t.Helper()
	qctx := NewLocalContext(ctx,
		WithRevisionedReader(datalayer.NewDataLayer(ds).SnapshotReader(rev, datalayer.NoSchemaHashForTesting)),
		WithTraceLogger(NewTraceLogger()),
	)
	seq, err := qctx.IterSubjects(it, resource, NoObjectFilter())
	require.NoError(t, err)

	paths, err := CollectAll(seq)
	require.NoError(t, err)

	if t.Failed() && qctx.TraceLogger != nil {
		t.Logf("Trace:\n%s", qctx.TraceLogger.DumpTrace())
	}

	return slicez.Map(paths, func(p *Path) string { return p.Subject.ObjectID })
}

// TestIntersectionWithWildcard tests that a wildcard from one side of an
// intersection correctly matches concrete subjects from the other side.
// Schema: permission view = viewer & reader
// Where viewer has user:* and reader has concrete users.
func TestIntersectionWithWildcard(t *testing.T) {
	ds, rev, dsSchema := setupTestDB(t, `
		definition user {}
		definition resource {
			relation viewer: user | user:*
			relation reader: user | user:*
			permission view = viewer & reader
		}
	`,
		"resource:doc1#viewer@user:*",
		"resource:doc1#reader@user:alice",
		"resource:doc1#reader@user:bob",
	)

	resourceDef, _ := dsSchema.GetTypeDefinition("resource")
	viewerRel, _ := resourceDef.GetRelation("viewer")
	readerRel, _ := resourceDef.GetRelation("reader")

	t.Run("WildcardIntersectConcrete", func(t *testing.T) {
		// viewer=user:* ∩ reader={alice, bob} → {alice, bob}
		it := NewIntersectionIterator(
			NewUnionIterator(
				NewDatastoreIterator(viewerRel.BaseRelations()[0]),
				NewDatastoreIterator(viewerRel.BaseRelations()[1]),
			),
			NewUnionIterator(
				NewDatastoreIterator(readerRel.BaseRelations()[0]),
				NewDatastoreIterator(readerRel.BaseRelations()[1]),
			),
		)
		ids := collectSubjectIDs(t, t.Context(), ds, rev, it, NewObject("resource", "doc1"))
		require.ElementsMatch(t, []string{"alice", "bob"}, ids)
	})
}

// TestIntersectionBothWildcards tests intersection when both sides have wildcards.
func TestIntersectionBothWildcards(t *testing.T) {
	ds, rev, dsSchema := setupTestDB(t, `
		definition user {}
		definition resource {
			relation viewer: user | user:*
			relation reader: user | user:*
			permission view = viewer & reader
		}
	`,
		"resource:doc1#viewer@user:*",
		"resource:doc1#viewer@user:alice",
		"resource:doc1#reader@user:*",
		"resource:doc1#reader@user:bob",
	)

	resourceDef, _ := dsSchema.GetTypeDefinition("resource")
	viewerRel, _ := resourceDef.GetRelation("viewer")
	readerRel, _ := resourceDef.GetRelation("reader")

	t.Run("BothWildcardsPlusConcrete", func(t *testing.T) {
		// viewer={*, alice} ∩ reader={*, bob}
		// Concrete ∩ concrete: (none in common)
		// Concrete ∩ wildcard: alice matched by reader:*, bob matched by viewer:*
		// Wildcard ∩ wildcard: * ∩ * = *
		// After top-level filtering of *, we get {alice, bob}
		it := NewIntersectionIterator(
			NewUnionIterator(
				NewDatastoreIterator(viewerRel.BaseRelations()[0]),
				NewDatastoreIterator(viewerRel.BaseRelations()[1]),
			),
			NewUnionIterator(
				NewDatastoreIterator(readerRel.BaseRelations()[0]),
				NewDatastoreIterator(readerRel.BaseRelations()[1]),
			),
		)
		ids := collectSubjectIDs(t, t.Context(), ds, rev, it, NewObject("resource", "doc1"))
		require.ElementsMatch(t, []string{"alice", "bob"}, ids)
	})
}

// TestExclusionWithWildcardExcluded tests that a wildcard in the excluded set
// correctly excludes all concrete subjects from the main set.
func TestExclusionWithWildcardExcluded(t *testing.T) {
	ds, rev, dsSchema := setupTestDB(t, `
		definition user {}
		definition resource {
			relation viewer: user
			relation banned: user | user:*
			permission view = viewer - banned
		}
	`,
		"resource:doc1#viewer@user:alice",
		"resource:doc1#viewer@user:bob",
		"resource:doc1#banned@user:*",
	)

	resourceDef, _ := dsSchema.GetTypeDefinition("resource")
	viewerRel, _ := resourceDef.GetRelation("viewer")
	bannedRel, _ := resourceDef.GetRelation("banned")

	t.Run("WildcardExcludesAllConcrete", func(t *testing.T) {
		// viewer={alice, bob} - banned={*} → empty (everyone is banned)
		it := NewExclusionIterator(
			NewUnionIterator(
				NewDatastoreIterator(viewerRel.BaseRelations()[0]),
			),
			NewUnionIterator(
				NewDatastoreIterator(bannedRel.BaseRelations()[0]),
				NewDatastoreIterator(bannedRel.BaseRelations()[1]),
			),
		)
		ids := collectSubjectIDs(t, t.Context(), ds, rev, it, NewObject("resource", "doc1"))
		require.Empty(t, ids, "wildcard ban should exclude all concrete viewers")
	})
}

// TestExclusionWildcardMinusConcrete tests that a wildcard in the main set
// passes through when the excluded set only has concrete subjects.
// The wildcard is stripped at the top level.
func TestExclusionWildcardMinusConcrete(t *testing.T) {
	ds, rev, dsSchema := setupTestDB(t, `
		definition user {}
		definition resource {
			relation viewer: user | user:*
			relation banned: user
			permission view = viewer - banned
		}
	`,
		"resource:doc1#viewer@user:*",
		"resource:doc1#viewer@user:alice",
		"resource:doc1#banned@user:alice",
	)

	resourceDef, _ := dsSchema.GetTypeDefinition("resource")
	viewerRel, _ := resourceDef.GetRelation("viewer")
	bannedRel, _ := resourceDef.GetRelation("banned")

	t.Run("WildcardMainMinusConcreteBan", func(t *testing.T) {
		// viewer={*, alice} - banned={alice}
		// alice is directly excluded.
		// * passes through (top-level filter strips it).
		// Result after filtering: empty (only concrete subjects survive, and alice was excluded)
		it := NewExclusionIterator(
			NewUnionIterator(
				NewDatastoreIterator(viewerRel.BaseRelations()[0]),
				NewDatastoreIterator(viewerRel.BaseRelations()[1]),
			),
			NewUnionIterator(
				NewDatastoreIterator(bannedRel.BaseRelations()[0]),
			),
		)
		ids := collectSubjectIDs(t, t.Context(), ds, rev, it, NewObject("resource", "doc1"))
		// alice is banned, * is filtered at top level → empty
		require.Empty(t, ids)
	})
}

// TestNestedExclusionWithWildcards tests the nested exclusion scenario:
// view = viewer - (maybebanned - notreallybanned)
// where viewer=*, maybebanned=*, notreallybanned={sarah}
// Expected: sarah escapes the ban → sarah can view.
func TestNestedExclusionWithWildcards(t *testing.T) {
	ds, rev, dsSchema := setupTestDB(t, `
		definition user {}
		definition resource {
			relation viewer: user | user:*
			relation maybebanned: user | user:*
			relation notreallybanned: user | user:*
			permission possiblybanned = maybebanned - notreallybanned
			permission view = viewer - possiblybanned
		}
	`,
		"resource:doc1#viewer@user:*",
		"resource:doc1#maybebanned@user:*",
		"resource:doc1#notreallybanned@user:sarah",
	)

	resourceDef, _ := dsSchema.GetTypeDefinition("resource")
	viewerRel, _ := resourceDef.GetRelation("viewer")
	maybebannedRel, _ := resourceDef.GetRelation("maybebanned")
	notreallybannedRel, _ := resourceDef.GetRelation("notreallybanned")

	t.Run("SarahEscapesBan", func(t *testing.T) {
		// possiblybanned = maybebanned:{*} - notreallybanned:{sarah}
		//   = {* with ExcludedSubjects=[sarah]}
		// view = viewer:{*} - possiblybanned:{* except sarah}
		//   = sarah (she's excluded from the exclusion, so she escapes)
		possiblybanned := NewExclusionIterator(
			NewUnionIterator(
				NewDatastoreIterator(maybebannedRel.BaseRelations()[0]),
				NewDatastoreIterator(maybebannedRel.BaseRelations()[1]),
			),
			NewUnionIterator(
				NewDatastoreIterator(notreallybannedRel.BaseRelations()[0]),
				NewDatastoreIterator(notreallybannedRel.BaseRelations()[1]),
			),
		)

		view := NewExclusionIterator(
			NewUnionIterator(
				NewDatastoreIterator(viewerRel.BaseRelations()[0]),
				NewDatastoreIterator(viewerRel.BaseRelations()[1]),
			),
			possiblybanned,
		)

		ids := collectSubjectIDs(t, t.Context(), ds, rev, view, NewObject("resource", "doc1"))
		require.ElementsMatch(t, []string{"sarah"}, ids, "sarah should escape the nested ban")
	})
}

// TestTopLevelFilterStripsWildcards ensures that wildcards propagate internally
// but are stripped before reaching the caller.
func TestTopLevelFilterStripsWildcards(t *testing.T) {
	ds, rev, dsSchema := setupTestDB(t, `
		definition user {}
		definition resource {
			relation viewer: user | user:*
		}
	`,
		"resource:doc1#viewer@user:*",
	)

	resourceDef, _ := dsSchema.GetTypeDefinition("resource")
	viewerRel, _ := resourceDef.GetRelation("viewer")

	t.Run("PureWildcardFiltered", func(t *testing.T) {
		// Only a wildcard relationship, no concrete subjects.
		// The wildcard propagates internally but is stripped at the top level.
		it := NewUnionIterator(
			NewDatastoreIterator(viewerRel.BaseRelations()[0]),
			NewDatastoreIterator(viewerRel.BaseRelations()[1]),
		)
		ids := collectSubjectIDs(t, t.Context(), ds, rev, it, NewObject("resource", "doc1"))
		require.Empty(t, ids, "pure wildcard should be stripped at top level")
	})
}

// TestWildcardInUnionWithConcrete tests that wildcards in a union coexist
// with concrete subjects — concrete subjects survive the top-level filter.
func TestWildcardInUnionWithConcrete(t *testing.T) {
	ds, rev, dsSchema := setupTestDB(t, `
		definition user {}
		definition resource {
			relation viewer: user | user:*
		}
	`,
		"resource:doc1#viewer@user:*",
		"resource:doc1#viewer@user:alice",
		"resource:doc1#viewer@user:bob",
	)

	resourceDef, _ := dsSchema.GetTypeDefinition("resource")
	viewerRel, _ := resourceDef.GetRelation("viewer")

	t.Run("ConcreteSubjectsSurviveFilter", func(t *testing.T) {
		it := NewUnionIterator(
			NewDatastoreIterator(viewerRel.BaseRelations()[0]),
			NewDatastoreIterator(viewerRel.BaseRelations()[1]),
		)
		ids := collectSubjectIDs(t, t.Context(), ds, rev, it, NewObject("resource", "doc1"))
		require.ElementsMatch(t, []string{"alice", "bob"}, ids)
	})
}

// TestIntersectionWildcardWithExclusion tests the combined scenario:
// permission view = (viewer & reader) where viewer=* and reader has concrete + exclusion.
func TestIntersectionWildcardWithExclusion(t *testing.T) {
	ds, rev, dsSchema := setupTestDB(t, `
		definition user {}
		definition resource {
			relation viewer: user | user:*
			relation reader: user
			relation banned: user
			permission allowed_readers = reader - banned
			permission view = viewer & allowed_readers
		}
	`,
		"resource:doc1#viewer@user:*",
		"resource:doc1#reader@user:alice",
		"resource:doc1#reader@user:bob",
		"resource:doc1#banned@user:bob",
	)

	resourceDef, _ := dsSchema.GetTypeDefinition("resource")
	viewerRel, _ := resourceDef.GetRelation("viewer")
	readerRel, _ := resourceDef.GetRelation("reader")
	bannedRel, _ := resourceDef.GetRelation("banned")

	t.Run("WildcardIntersectExclusion", func(t *testing.T) {
		// allowed_readers = reader:{alice, bob} - banned:{bob} = {alice}
		// view = viewer:{*} ∩ allowed_readers:{alice} = {alice}
		allowedReaders := NewExclusionIterator(
			NewDatastoreIterator(readerRel.BaseRelations()[0]),
			NewDatastoreIterator(bannedRel.BaseRelations()[0]),
		)

		view := NewIntersectionIterator(
			NewUnionIterator(
				NewDatastoreIterator(viewerRel.BaseRelations()[0]),
				NewDatastoreIterator(viewerRel.BaseRelations()[1]),
			),
			allowedReaders,
		)

		ids := collectSubjectIDs(t, t.Context(), ds, rev, view, NewObject("resource", "doc1"))
		require.ElementsMatch(t, []string{"alice"}, ids)
	})
}
