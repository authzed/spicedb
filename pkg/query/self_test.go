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

		selfIt := NewSelfIterator("view", "user")

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

		selfIt := NewSelfIterator("view", "user")

		// Only bob in the list now
		pathSeq, err := ctx.Check(selfIt, NewObjects("user", "bob"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Empty(rels)
	})

	t.Run("IterResources", func(t *testing.T) {
		t.Parallel()

		selfIt := NewSelfIterator("view", "user")
		pathSeq, err := ctx.IterResources(selfIt, NewObject("user", "alice").WithEllipses(), NoObjectFilter())
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

		selfIt := NewSelfIterator("view", "user")
		pathSeq, err := ctx.IterSubjects(selfIt, NewObject("user", "alice"), NoObjectFilter())
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

	original := NewSelfIterator("original_relation", "user")

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

	aliasIt := NewSelfIterator("some_relation", "user")

	explain := aliasIt.Explain()
	require.Equal("Self(some_relation)", explain.Info)
	require.Empty(explain.SubExplain)
}

func TestSelf_Types(t *testing.T) {
	t.Parallel()

	t.Run("ResourceType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		selfIt := NewSelfIterator("view", "user")

		resourceType, err := selfIt.ResourceType()
		require.NoError(err)
		require.Len(resourceType, 1)
		require.Equal("user", resourceType[0].Type)
	})

	t.Run("SubjectTypes", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		selfIt := NewSelfIterator("view", "user")

		subjectTypes, err := selfIt.SubjectTypes()
		require.NoError(err)
		require.Len(subjectTypes, 1)
		require.Equal("user", subjectTypes[0].Type)
	})

	t.Run("SubjectTypes_SameAsResourceType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		selfIt := NewSelfIterator("edit", "document")

		resourceType, err := selfIt.ResourceType()
		require.NoError(err)
		subjectTypes, err := selfIt.SubjectTypes()
		require.NoError(err)

		require.Len(subjectTypes, 1)
		require.Len(resourceType, 1)
		require.Equal(resourceType[0], subjectTypes[0], "Self iterator should have same resource and subject types")
	})
}
