package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelfIterator(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test context
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}
	t.Run("Check", func(t *testing.T) {
		t.Parallel()

		selfIt := NewSelf("view")

		// Create a resource seq with both Alice and Bob
		pathSeq, err := ctx.Check(selfIt, NewObjects("user", "alice", "bob"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Len(rels, 1)
		rel := rels[0]
		require.Equal("view", rel.Relation, "all relations should be rewritten to 'read'")
		require.Equal("user", rel.Resource.ObjectType)
		require.Equal("alice", rel.Resource.ObjectID)
		require.Equal("user", rel.Subject.ObjectType)
		require.Equal("alice", rel.Subject.ObjectID)
		require.Equal("...", rel.Subject.Relation)
	})

	t.Run("Check_EmptyResults", func(t *testing.T) {
		t.Parallel()

		selfIt := NewSelf("view")

		// Only bob in the list now
		pathSeq, err := ctx.Check(selfIt, NewObjects("user", "bob"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Empty(rels)
	})

	t.Run("IterResources", func(t *testing.T) {
		t.Parallel()

		selfIt := NewSelf("view")
		pathSeq, err := ctx.IterResources(selfIt, NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Len(rels, 1)
		rel := rels[0]
		require.Equal("view", rel.Relation, "all relations should be rewritten to 'read'")
		require.Equal("user", rel.Resource.ObjectType)
		require.Equal("alice", rel.Resource.ObjectID)
		require.Equal("user", rel.Subject.ObjectType)
		require.Equal("alice", rel.Subject.ObjectID)
		require.Equal("...", rel.Subject.Relation)
	})

	t.Run("IterSubjects", func(t *testing.T) {
		t.Parallel()

		selfIt := NewSelf("view")
		pathSeq, err := ctx.IterSubjects(selfIt, NewObject("user", "alice"))
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Len(rels, 1)
		rel := rels[0]
		require.Equal("view", rel.Relation, "all relations should be rewritten to 'read'")
		require.Equal("user", rel.Resource.ObjectType)
		require.Equal("alice", rel.Resource.ObjectID)
		require.Equal("user", rel.Subject.ObjectType)
		require.Equal("alice", rel.Subject.ObjectID)
		require.Equal("...", rel.Subject.Relation)
	})
}

func TestSelfIteratorClone(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	original := NewSelf("original_relation")

	cloned := original.Clone()
	require.NotSame(original, cloned, "cloned iterator should be a different object")

	// Both should have the same structure when explained
	originalExplain := original.Explain()
	clonedExplain := cloned.Explain()
	require.Equal(originalExplain.Info, clonedExplain.Info)
	require.Len(clonedExplain.SubExplain, len(originalExplain.SubExplain))

	// The underlying relation should be the same
	require.Equal("Self(original_relation)", originalExplain.Info)
	require.Equal("Self(original_relation)", clonedExplain.Info)
}

func TestSelfIteratorExplain(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	aliasIt := NewSelf("some_relation")

	explain := aliasIt.Explain()
	require.Equal("Self(some_relation)", explain.Info)
	require.Empty(explain.SubExplain)
}
