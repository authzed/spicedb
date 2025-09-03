package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/tuple"
)

func TestFixedIterator(t *testing.T) {
	require := require.New(t)
	t.Parallel()

	// Create test context
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	// Create test relations
	rel1 := tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{
			Resource: tuple.ObjectAndRelation{
				ObjectType: "document",
				ObjectID:   "doc1",
				Relation:   "viewer",
			},
			Subject: tuple.ObjectAndRelation{
				ObjectType: "user",
				ObjectID:   "alice",
				Relation:   "...",
			},
		},
	}

	rel2 := tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{
			Resource: tuple.ObjectAndRelation{
				ObjectType: "document",
				ObjectID:   "doc2",
				Relation:   "editor",
			},
			Subject: tuple.ObjectAndRelation{
				ObjectType: "user",
				ObjectID:   "bob",
				Relation:   "...",
			},
		},
	}

	rel3 := tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{
			Resource: tuple.ObjectAndRelation{
				ObjectType: "document",
				ObjectID:   "doc1",
				Relation:   "editor",
			},
			Subject: tuple.ObjectAndRelation{
				ObjectType: "user",
				ObjectID:   "charlie",
				Relation:   "...",
			},
		},
	}

	// Create fixed iterator
	fixed := NewFixedIterator(rel1, rel2, rel3)

	t.Run("Check", func(t *testing.T) {
		t.Parallel()

		// Test Check method
		seq, err := ctx.Check(fixed, []string{"doc1", "doc2"}, "alice")
		require.NoError(err)

		results, err := CollectAll(seq)
		require.NoError(err)

		// Should find rel1 (doc1 with alice as viewer)
		require.Len(results, 1)
		require.Equal("doc1", results[0].Resource.ObjectID)
		require.Equal("alice", results[0].Subject.ObjectID)
	})

	t.Run("Check_NoMatches", func(t *testing.T) {
		t.Parallel()

		seq, err := ctx.Check(fixed, []string{"doc1"}, "nonexistent")
		require.NoError(err)

		results, err := CollectAll(seq)
		require.NoError(err)
		require.Empty(results)
	})

	t.Run("IterSubjects", func(t *testing.T) {
		t.Parallel()

		seq, err := ctx.IterSubjects(fixed, "doc1")
		require.NoError(err)

		results, err := CollectAll(seq)
		require.NoError(err)

		// Should find rel1 and rel3 (both have doc1 as resource)
		require.Len(results, 2)

		subjects := []string{results[0].Subject.ObjectID, results[1].Subject.ObjectID}
		require.Contains(subjects, "alice")
		require.Contains(subjects, "charlie")
	})

	t.Run("IterResources", func(t *testing.T) {
		t.Parallel()

		seq, err := ctx.IterResources(fixed, "alice")
		require.NoError(err)

		results, err := CollectAll(seq)
		require.NoError(err)

		// Should find rel1 (alice is subject)
		require.Len(results, 1)
		require.Equal("doc1", results[0].Resource.ObjectID)
		require.Equal("alice", results[0].Subject.ObjectID)
	})

	t.Run("Clone", func(t *testing.T) {
		t.Parallel()

		cloned := fixed.Clone()
		require.NotSame(fixed, cloned)

		// Both should produce the same results
		originalSeq, err := ctx.Check(fixed, []string{"doc1"}, "alice")
		require.NoError(err)
		originalResults, err := CollectAll(originalSeq)
		require.NoError(err)

		clonedSeq, err := ctx.Check(cloned, []string{"doc1"}, "alice")
		require.NoError(err)
		clonedResults, err := CollectAll(clonedSeq)
		require.NoError(err)

		require.Equal(originalResults, clonedResults)
	})

	t.Run("Explain", func(t *testing.T) {
		t.Parallel()

		explain := fixed.Explain()
		require.Equal("Fixed(3 relations)", explain.Info)
		require.Empty(explain.SubExplain)
	})
}
