package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestWildcardIterator(t *testing.T) {
	require := require.New(t)
	t.Parallel()

	// Create test datastore
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)

	// Add wildcard relationships for testing
	wildcardRels := []string{
		"document:doc1#viewer@user:*#...",
		"document:doc2#viewer@user:*#...",
	}

	ctx := t.Context()
	rels := make([]tuple.Relationship, 0, len(wildcardRels))
	for _, tupleStr := range wildcardRels {
		rel, err := tuple.Parse(tupleStr)
		require.NoError(err)
		require.NotNil(rel)
		rels = append(rels, rel)
	}

	revision, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, rels...)
	require.NoError(err)

	// Create test context
	testCtx := &Context{
		Context:   t.Context(),
		Executor:  LocalExecutor{},
		Datastore: ds,
		Revision:  revision,
	}

	// Create a mock schema relation and base relation for wildcard
	relation := &schema.Relation{
		Name: "viewer",
	}
	definition := &schema.Definition{
		Name: "document",
	}
	relation.Parent = definition

	// Create a wildcard base relation: document:viewer -> user:*
	baseRelation := &schema.BaseRelation{
		Parent:      relation,
		Type:        "user",
		Subrelation: tuple.Ellipsis,
		Caveat:      "",
		Expiration:  false,
		Wildcard:    true,
	}

	// Create the wildcard iterator
	wildcardIt := NewWildcardIterator(baseRelation)

	t.Run("CheckImpl with matching subject type", func(t *testing.T) {
		t.Parallel()
		resources := []Object{
			{ObjectType: "document", ObjectID: "doc1"},
			{ObjectType: "document", ObjectID: "doc2"},
			{ObjectType: "folder", ObjectID: "folder1"}, // Different type, should not match
		}
		subject := ObjectAndRelation{
			ObjectType: "user",
			ObjectID:   "alice",
			Relation:   tuple.Ellipsis,
		}

		seq, err := wildcardIt.CheckImpl(testCtx, resources, subject)
		require.NoError(err)

		relations, err := CollectAll(seq)
		require.NoError(err)
		require.Len(relations, 2) // Should match doc1 and doc2, but not folder1

		// Verify the relations - now they should have the concrete subject, not wildcard
		expected1 := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ObjectAndRelation{
					ObjectType: "document",
					ObjectID:   "doc1",
					Relation:   "viewer",
				},
				Subject: tuple.ObjectAndRelation{
					ObjectType: "user",
					ObjectID:   "alice", // Concrete subject, not wildcard
					Relation:   tuple.Ellipsis,
				},
			},
		}
		expected2 := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ObjectAndRelation{
					ObjectType: "document",
					ObjectID:   "doc2",
					Relation:   "viewer",
				},
				Subject: tuple.ObjectAndRelation{
					ObjectType: "user",
					ObjectID:   "alice", // Concrete subject, not wildcard
					Relation:   tuple.Ellipsis,
				},
			},
		}

		require.Contains(relations, expected1)
		require.Contains(relations, expected2)
	})

	t.Run("CheckImpl with non-matching subject type", func(t *testing.T) {
		t.Parallel()
		resources := []Object{
			{ObjectType: "document", ObjectID: "doc1"},
		}
		subject := ObjectAndRelation{
			ObjectType: "group", // Different type than "user"
			ObjectID:   "group1",
			Relation:   tuple.Ellipsis,
		}

		seq, err := wildcardIt.CheckImpl(testCtx, resources, subject)
		require.NoError(err)

		relations, err := CollectAll(seq)
		require.NoError(err)
		require.Len(relations, 0) // Should not match
	})

	t.Run("Clone", func(t *testing.T) {
		t.Parallel()
		cloned := wildcardIt.Clone()
		require.NotSame(wildcardIt, cloned)
		require.IsType(&WildcardIterator{}, cloned)

		wildcardCloned := cloned.(*WildcardIterator)
		require.Equal(wildcardIt.base, wildcardCloned.base)
	})

	t.Run("Explain", func(t *testing.T) {
		t.Parallel()
		explain := wildcardIt.Explain()
		require.Contains(explain.Info, "Wildcard(document:viewer -> user:*")
		require.Contains(explain.Info, "caveat: false")
		require.Contains(explain.Info, "expiration: false")
		require.Empty(explain.SubExplain)
	})
}

func TestWildcardIteratorWithCaveatAndExpiration(t *testing.T) {
	require := require.New(t)
	t.Parallel()

	// Create test datastore
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(rawDS, require)

	// Add wildcard relationships for testing
	wildcardRels := []string{
		"document:doc1#admin@user:*#...",
	}

	ctx := t.Context()
	rels := make([]tuple.Relationship, 0, len(wildcardRels))
	for _, tupleStr := range wildcardRels {
		rel, err := tuple.Parse(tupleStr)
		require.NoError(err)
		require.NotNil(rel)
		rels = append(rels, rel)
	}

	revision, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, rels...)
	require.NoError(err)

	// Create test context
	testCtx := &Context{
		Context:   t.Context(),
		Executor:  LocalExecutor{},
		Datastore: ds,
		Revision:  revision,
	}

	// Create a mock schema relation and base relation for wildcard with caveat and expiration
	relation := &schema.Relation{
		Name: "admin",
	}
	definition := &schema.Definition{
		Name: "document",
	}
	relation.Parent = definition

	// Create a wildcard base relation with caveat and expiration
	baseRelation := &schema.BaseRelation{
		Parent:      relation,
		Type:        "user",
		Subrelation: tuple.Ellipsis,
		Caveat:      "test_caveat",
		Expiration:  true,
		Wildcard:    true,
	}

	wildcardIt := NewWildcardIterator(baseRelation)

	t.Run("Explain with caveat and expiration", func(t *testing.T) {
		t.Parallel()
		explain := wildcardIt.Explain()
		require.Contains(explain.Info, "Wildcard(document:admin -> user:*")
		require.Contains(explain.Info, "caveat: true")
		require.Contains(explain.Info, "expiration: true")
	})

	t.Run("CheckImpl generates correct wildcard relations", func(t *testing.T) {
		t.Parallel()
		resources := []Object{
			{ObjectType: "document", ObjectID: "doc1"},
		}
		subject := ObjectAndRelation{
			ObjectType: "user",
			ObjectID:   "alice",
			Relation:   tuple.Ellipsis,
		}

		seq, err := wildcardIt.CheckImpl(testCtx, resources, subject)
		require.NoError(err)

		relations, err := CollectAll(seq)
		require.NoError(err)
		require.Len(relations, 1)

		// Verify the relation uses the concrete subject, not wildcard
		rel := relations[0]
		require.Equal("document", rel.Resource.ObjectType)
		require.Equal("doc1", rel.Resource.ObjectID)
		require.Equal("admin", rel.Resource.Relation)
		require.Equal("user", rel.Subject.ObjectType)
		require.Equal("alice", rel.Subject.ObjectID)        // Concrete subject
		require.Equal(tuple.Ellipsis, rel.Subject.Relation) // "..."
	})
}
