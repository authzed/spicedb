package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/tuple"
)

// TestDatastoreIterator_CheckWildcard exercises checkWildcardImpl
// (datastore.go:85) — the branch taken when the base relation is a wildcard
// (e.g. user:*). The caller asks whether a concrete subject is allowed via the
// wildcard row, and checkWildcardImpl rewrites the subject back to the
// concrete one on success.
func TestDatastoreIterator_CheckWildcard(t *testing.T) {
	const sc = `
		definition user {}
		definition resource {
			relation viewer: user | user:*
		}
	`
	ds, rev, dsSchema := setupTestDB(t, sc,
		"resource:doc1#viewer@user:*",
	)

	resourceDef, _ := dsSchema.GetTypeDefinition("resource")
	viewerRel, _ := resourceDef.GetRelation("viewer")
	// BaseRelations[1] is the user:* wildcard branch (BaseRelations[0] is user).
	wildcardBase := NewDatastoreIterator(viewerRel.BaseRelations()[1])

	ctx := NewLocalContext(t.Context(),
		WithRevisionedReader(datalayer.NewDataLayer(ds).SnapshotReader(rev, datalayer.NoSchemaHashForTesting)),
	)

	t.Run("ConcreteSubjectMatchesViaWildcard", func(t *testing.T) {
		path, err := ctx.Check(wildcardBase,
			NewObject("resource", "doc1"),
			NewObject("user", "alice").WithEllipses(),
		)
		require.NoError(t, err)
		require.NotNil(t, path, "wildcard row should match any concrete user")
		// Subject should be rewritten back to the concrete user.
		require.Equal(t, "alice", path.Subject.ObjectID)
	})

	t.Run("ConcreteSubjectMissesWhenNoWildcardRow", func(t *testing.T) {
		path, err := ctx.Check(wildcardBase,
			NewObject("resource", "nonexistent"),
			NewObject("user", "alice").WithEllipses(),
		)
		require.NoError(t, err)
		require.Nil(t, path)
	})
}

// TestDatastoreIterator_IterSubjectsWildcard exercises the post-refactor
// wildcard branch of IterSubjectsImpl (datastore.go:206). The refactor
// simplified this path: it now yields the wildcard subject directly rather
// than enumerating concrete subjects. The top-level FilterWildcardSubjects
// strips it from caller-visible results, so we read the internal stream
// by making a secondary IterSubjects call after tripping topLevelOnce.
func TestDatastoreIterator_IterSubjectsWildcard(t *testing.T) {
	const sc = `
		definition user {}
		definition resource {
			relation viewer: user | user:*
		}
	`
	ds, rev, dsSchema := setupTestDB(t, sc,
		"resource:doc1#viewer@user:*",
	)

	resourceDef, _ := dsSchema.GetTypeDefinition("resource")
	viewerRel, _ := resourceDef.GetRelation("viewer")
	wildcardBase := NewDatastoreIterator(viewerRel.BaseRelations()[1])

	ctx := NewLocalContext(t.Context(),
		WithRevisionedReader(datalayer.NewDataLayer(ds).SnapshotReader(rev, datalayer.NoSchemaHashForTesting)),
	)

	// Trip topLevelOnce so the subsequent call is treated as non-top-level
	// and the wildcard path is not stripped.
	tripTopLevel(t, ctx, NewObject("resource", "doc1"))

	seq, err := ctx.IterSubjects(wildcardBase, NewObject("resource", "doc1"), NoObjectFilter())
	require.NoError(t, err)
	paths, err := CollectAll(seq)
	require.NoError(t, err)
	require.Len(t, paths, 1)
	require.Equal(t, tuple.PublicWildcard, paths[0].Subject.ObjectID)
	require.Equal(t, "user", paths[0].Subject.ObjectType)
}

// TestDatastoreIterator_IterSubjectsWildcard_Empty verifies the wildcard branch
// handles the case where no wildcard row exists for the resource.
func TestDatastoreIterator_IterSubjectsWildcard_Empty(t *testing.T) {
	const sc = `
		definition user {}
		definition resource {
			relation viewer: user | user:*
		}
	`
	// Note: no wildcard relationship.
	ds, rev, dsSchema := setupTestDB(t, sc,
		"resource:doc1#viewer@user:alice",
	)

	resourceDef, _ := dsSchema.GetTypeDefinition("resource")
	viewerRel, _ := resourceDef.GetRelation("viewer")
	wildcardBase := NewDatastoreIterator(viewerRel.BaseRelations()[1])

	ctx := NewLocalContext(t.Context(),
		WithRevisionedReader(datalayer.NewDataLayer(ds).SnapshotReader(rev, datalayer.NoSchemaHashForTesting)),
	)
	tripTopLevel(t, ctx, NewObject("resource", "doc1"))

	seq, err := ctx.IterSubjects(wildcardBase, NewObject("resource", "doc1"), NoObjectFilter())
	require.NoError(t, err)
	paths, err := CollectAll(seq)
	require.NoError(t, err)
	require.Empty(t, paths)
}

// TestDatastoreIterator_IterResourcesWildcard exercises iterResourcesWildcardImpl
// (datastore.go:321) — when the base relation stores a wildcard and the caller
// asks for resources matching a concrete subject, RewriteSubject restores the
// concrete subject on each yielded path.
func TestDatastoreIterator_IterResourcesWildcard(t *testing.T) {
	const sc = `
		definition user {}
		definition resource {
			relation viewer: user | user:*
		}
	`
	ds, rev, dsSchema := setupTestDB(t, sc,
		"resource:doc1#viewer@user:*",
		"resource:doc2#viewer@user:*",
	)

	resourceDef, _ := dsSchema.GetTypeDefinition("resource")
	viewerRel, _ := resourceDef.GetRelation("viewer")
	wildcardBase := NewDatastoreIterator(viewerRel.BaseRelations()[1])

	ctx := NewLocalContext(t.Context(),
		WithRevisionedReader(datalayer.NewDataLayer(ds).SnapshotReader(rev, datalayer.NoSchemaHashForTesting)),
	)

	// Asking "which resources is alice a viewer of via the wildcard?" should
	// match both doc1 and doc2 (because both have user:* on viewer).
	seq, err := ctx.IterResources(wildcardBase, NewObject("user", "alice").WithEllipses(), NoObjectFilter())
	require.NoError(t, err)
	paths, err := CollectAll(seq)
	require.NoError(t, err)

	require.Len(t, paths, 2)
	for _, p := range paths {
		// RewriteSubject should have mapped user:* back to user:alice.
		require.Equal(t, "alice", p.Subject.ObjectID)
	}
}
