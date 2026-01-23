package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFixedIterator(t *testing.T) {
	require := require.New(t)
	t.Parallel()

	// Create test context
	ctx := NewLocalContext(t.Context())

	// Create test paths
	path1 := MustPathFromString("document:doc1#viewer@user:alice")
	path2 := MustPathFromString("document:doc2#editor@user:bob")
	path3 := MustPathFromString("document:doc1#editor@user:charlie")

	// Create fixed iterator
	fixed := NewFixedIterator(path1, path2, path3)

	t.Run("Check", func(t *testing.T) {
		t.Parallel()

		// Test Check method
		seq, err := ctx.Check(fixed, NewObjects("document", "doc1", "doc2"), NewObject("user", "alice").WithEllipses())
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

		seq, err := ctx.Check(fixed, NewObjects("document", "doc1"), NewObject("user", "nonexistent").WithEllipses())
		require.NoError(err)

		results, err := CollectAll(seq)
		require.NoError(err)
		require.Empty(results)
	})

	t.Run("IterSubjects", func(t *testing.T) {
		t.Parallel()

		seq, err := ctx.IterSubjects(fixed, NewObject("document", "doc1"))
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

		seq, err := ctx.IterResources(fixed, NewObject("user", "alice").WithEllipses())
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
		originalSeq, err := ctx.Check(fixed, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		originalResults, err := CollectAll(originalSeq)
		require.NoError(err)

		clonedSeq, err := ctx.Check(cloned, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		clonedResults, err := CollectAll(clonedSeq)
		require.NoError(err)

		require.Equal(originalResults, clonedResults)
	})

	t.Run("Explain", func(t *testing.T) {
		t.Parallel()

		explain := fixed.Explain()
		require.Equal("Fixed(3 paths)", explain.Info)
		require.Empty(explain.SubExplain)
	})
}

func TestFixedIterator_Types(t *testing.T) {
	t.Parallel()

	t.Run("ResourceType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("document:doc2#editor@user:bob")
		fixed := NewFixedIterator(path1, path2)

		resourceType := fixed.ResourceType()
		require.Equal("document", resourceType.Type)
		require.Empty(resourceType.Subrelation) // Relations vary, so subrelation is empty
	})

	t.Run("SubjectTypes", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("document:doc2#editor@user:bob")
		path3 := MustPathFromString("document:doc3#owner@group:engineers#member")
		fixed := NewFixedIterator(path1, path2, path3)

		subjectTypes := fixed.SubjectTypes()
		require.Len(subjectTypes, 2, "Should have 2 unique subject types")

		// Check that both user and group types are present
		typeMap := make(map[string]bool)
		for _, st := range subjectTypes {
			key := st.Type + "#" + st.Subrelation
			typeMap[key] = true
		}

		require.True(typeMap["user#..."] || typeMap["user#"])
		require.True(typeMap["group#member"])
	})

	t.Run("EmptyIterator", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		fixed := NewFixedIterator()

		resourceType := fixed.ResourceType()
		require.Empty(resourceType.Type)
		require.Empty(resourceType.Subrelation)

		subjectTypes := fixed.SubjectTypes()
		require.Empty(subjectTypes)
	})
}
