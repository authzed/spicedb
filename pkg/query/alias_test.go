package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAliasIterator(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test context
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	t.Run("Check_BasicRelationRewriting", func(t *testing.T) {
		t.Parallel()

		// Create a sub-iterator with document relations
		subIt := NewDocumentAccessFixedIterator()

		// Create an alias iterator that rewrites all relations to "read"
		aliasIt := NewAlias("read", subIt)

		relSeq, err := ctx.Check(aliasIt, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		// All relations should have "read" as the resource relation
		for _, rel := range rels {
			require.Equal("read", rel.Resource.Relation, "all relations should be rewritten to 'read'")
			require.Equal("document", rel.Resource.ObjectType)
			require.Equal("doc1", rel.Resource.ObjectID)
			require.Equal("user", rel.Subject.ObjectType)
			require.Equal("alice", rel.Subject.ObjectID)
			require.Equal("...", rel.Subject.Relation)
		}

		// Should have same number of relations as original sub-iterator
		// (alice has viewer, editor, owner on doc1)
		require.Len(rels, 3, "should have 3 rewritten relations")
	})

	t.Run("Check_SelfEdgeDetection", func(t *testing.T) {
		t.Parallel()

		// Create an empty sub-iterator since we only want to test self-edge detection
		subIt := NewEmptyFixedIterator()

		// Create an alias iterator that rewrites to "admin"
		aliasIt := NewAlias("admin", subIt)

		// Check for a self-edge: user:alice#admin@user:alice#admin
		subject := NewObjectAndRelation("alice", "user", "admin")
		relSeq, err := ctx.Check(aliasIt, NewObjects("user", "alice"), subject)
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		// Should find exactly one relation (the self-edge)
		require.Len(rels, 1, "should find exactly one self-edge relation")

		// Verify it's the correct self-edge relation
		rel := rels[0]
		expectedResource := NewObjectAndRelation("alice", "user", "admin")
		require.Equal(expectedResource.ObjectID, rel.Resource.ObjectID)
		require.Equal(expectedResource.ObjectType, rel.Resource.ObjectType)
		require.Equal(expectedResource.Relation, rel.Resource.Relation)
		require.Equal(subject.ObjectID, rel.Subject.ObjectID)
		require.Equal(subject.ObjectType, rel.Subject.ObjectType)
		require.Equal(subject.Relation, rel.Subject.Relation)
	})

	t.Run("Check_NoSelfEdge", func(t *testing.T) {
		t.Parallel()

		// Create a sub-iterator
		subIt := NewSingleUserFixedIterator("bob")

		// Create an alias iterator
		aliasIt := NewAlias("admin", subIt)

		// Check with a subject that doesn't match any resource
		subject := NewObjectAndRelation("alice", "user", "viewer")
		relSeq, err := ctx.Check(aliasIt, NewObjects("document", "doc1"), subject)
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		// All relations should be rewritten but no self-edge
		for _, rel := range rels {
			require.Equal("admin", rel.Resource.Relation, "all relations should be rewritten to 'admin'")
		}
	})

	t.Run("Check_MultipleResources", func(t *testing.T) {
		t.Parallel()

		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAlias("access", subIt)

		relSeq, err := ctx.Check(aliasIt, NewObjects("document", "doc1", "doc2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		// All relations should have "access" as the resource relation
		for _, rel := range rels {
			require.Equal("access", rel.Resource.Relation)
		}

		// Should find relations for both doc1 and doc2
		docIDs := make(map[string]bool)
		for _, rel := range rels {
			docIDs[rel.Resource.ObjectID] = true
		}
		require.True(docIDs["doc1"], "should find relations for doc1")
		require.True(docIDs["doc2"], "should find relations for doc2")
	})

	t.Run("IterSubjects_RelationRewriting", func(t *testing.T) {
		t.Parallel()

		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAlias("permission", subIt)

		relSeq, err := ctx.IterSubjects(aliasIt, NewObject("document", "doc1"))
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		// All relations should have "permission" as the resource relation
		for _, rel := range rels {
			require.Equal("permission", rel.Resource.Relation)
			require.Equal("document", rel.Resource.ObjectType)
			require.Equal("doc1", rel.Resource.ObjectID)
		}
	})

	t.Run("IterResources_RelationRewriting", func(t *testing.T) {
		t.Parallel()

		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAlias("can_view", subIt)

		relSeq, err := ctx.IterResources(aliasIt, NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		// All relations should have "can_view" as the resource relation
		for _, rel := range rels {
			require.Equal("can_view", rel.Resource.Relation)
		}
	})

	t.Run("Check_EmptySubIterator", func(t *testing.T) {
		t.Parallel()

		subIt := NewEmptyFixedIterator()
		aliasIt := NewAlias("empty_alias", subIt)

		relSeq, err := ctx.Check(aliasIt, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)
		require.Empty(rels, "empty sub-iterator should return no results")
	})

	t.Run("Check_SelfEdgeWithEmptySubIterator", func(t *testing.T) {
		t.Parallel()

		// Create empty sub-iterator
		subIt := NewEmptyFixedIterator()
		aliasIt := NewAlias("self", subIt)

		// Create a self-edge scenario: document:doc1#self@user:alice#self
		subject := NewObjectAndRelation("alice", "user", "self")

		// But we're checking for document:doc1, which won't match user:alice
		// So no self-edge should be detected
		relSeq, err := ctx.Check(aliasIt, NewObjects("document", "doc1"), subject)
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)
		require.Empty(rels, "no self-edge should be detected for different object types")
	})

	t.Run("Check_SelfEdgeExactMatch", func(t *testing.T) {
		t.Parallel()

		subIt := NewEmptyFixedIterator()
		aliasIt := NewAlias("owner", subIt)

		// Create a perfect self-edge: user:alice#owner@user:alice#owner
		subject := NewObjectAndRelation("alice", "user", "owner")
		relSeq, err := ctx.Check(aliasIt, NewObjects("user", "alice"), subject)
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		// Should find exactly one self-edge relation
		require.Len(rels, 1, "should find exactly one self-edge relation")

		rel := rels[0]
		expectedResource := NewObjectAndRelation("alice", "user", "owner")
		require.Equal(expectedResource.ObjectID, rel.Resource.ObjectID)
		require.Equal(expectedResource.ObjectType, rel.Resource.ObjectType)
		require.Equal(expectedResource.Relation, rel.Resource.Relation)
		require.Equal(subject.ObjectID, rel.Subject.ObjectID)
		require.Equal(subject.ObjectType, rel.Subject.ObjectType)
		require.Equal(subject.Relation, rel.Subject.Relation)
	})
}

func TestAliasIteratorClone(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	subIt := NewDocumentAccessFixedIterator()
	original := NewAlias("original_relation", subIt)

	cloned := original.Clone()
	require.NotSame(original, cloned, "cloned iterator should be a different object")

	// Both should have the same structure when explained
	originalExplain := original.Explain()
	clonedExplain := cloned.Explain()
	require.Equal(originalExplain.Info, clonedExplain.Info)
	require.Equal(len(originalExplain.SubExplain), len(clonedExplain.SubExplain))

	// The underlying relation should be the same
	require.Equal("Alias(original_relation)", originalExplain.Info)
	require.Equal("Alias(original_relation)", clonedExplain.Info)
}

func TestAliasIteratorExplain(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	t.Run("ExplainWithSubIterator", func(t *testing.T) {
		t.Parallel()

		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAlias("test_relation", subIt)

		explain := aliasIt.Explain()
		require.Equal("Alias(test_relation)", explain.Info)
		require.Len(explain.SubExplain, 1, "alias should have exactly 1 sub-explain")

		explainStr := explain.String()
		require.Contains(explainStr, "Alias(test_relation)")
		require.NotEmpty(explainStr)
	})

	t.Run("ExplainWithEmptySubIterator", func(t *testing.T) {
		t.Parallel()

		subIt := NewEmptyFixedIterator()
		aliasIt := NewAlias("empty_test", subIt)

		explain := aliasIt.Explain()
		require.Equal("Alias(empty_test)", explain.Info)
		require.Len(explain.SubExplain, 1, "alias should have exactly 1 sub-explain even with empty sub-iterator")
	})
}

func TestAliasIteratorErrorHandling(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test context
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	t.Run("Check_SubIteratorError", func(t *testing.T) {
		t.Parallel()

		// Create a faulty sub-iterator
		faultyIt := NewFaultyIterator(true, false)
		aliasIt := NewAlias("error_test", faultyIt)

		_, err := ctx.Check(aliasIt, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.Error(err, "should propagate error from sub-iterator")
	})

	t.Run("Check_SubIteratorCollectionError", func(t *testing.T) {
		t.Parallel()

		// Create a faulty sub-iterator that fails during collection
		faultyIt := NewFaultyIterator(false, true)
		aliasIt := NewAlias("collection_error_test", faultyIt)

		relSeq, err := ctx.Check(aliasIt, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err, "initial check should succeed")

		// Error should occur during collection
		_, err = CollectAll(relSeq)
		require.Error(err, "should propagate collection error from sub-iterator")
	})
}
