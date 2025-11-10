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

		pathSeq, err := ctx.Check(aliasIt, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)

		// All relations should have "read" as the resource relation
		for _, rel := range rels {
			require.Equal("read", rel.Relation, "all relations should be rewritten to 'read'")
			require.Equal("document", rel.Resource.ObjectType)
			require.Equal("doc1", rel.Resource.ObjectID)
			require.Equal("user", rel.Subject.ObjectType)
			require.Equal("alice", rel.Subject.ObjectID)
			require.Equal("...", rel.Subject.Relation)
		}

		// Should have 1 deduplicated relation after rewriting
		// (alice has viewer, editor, owner on doc1, but all get rewritten to "read" and deduplicated)
		require.Len(rels, 1, "should have 1 deduplicated relation after rewriting")
	})

	t.Run("Check_SelfEdgeDetection", func(t *testing.T) {
		t.Parallel()

		// Create an empty sub-iterator since we only want to test self-edge detection
		subIt := NewEmptyFixedIterator()

		// Create an alias iterator that rewrites to "admin"
		aliasIt := NewAlias("admin", subIt)

		// Check for a self-edge: user:alice#admin@user:alice#admin
		subject := NewObjectAndRelation("alice", "user", "admin")
		pathSeq, err := ctx.Check(aliasIt, NewObjects("user", "alice"), subject)
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should find exactly one relation (the self-edge)
		require.Len(rels, 1, "should find exactly one self-edge relation")

		// Verify it's the correct self-edge relation
		rel := rels[0]
		expectedResource := NewObjectAndRelation("alice", "user", "admin")
		require.Equal(expectedResource.ObjectID, rel.Resource.ObjectID)
		require.Equal(expectedResource.ObjectType, rel.Resource.ObjectType)
		require.Equal(expectedResource.Relation, rel.Relation)
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
		pathSeq, err := ctx.Check(aliasIt, NewObjects("document", "doc1"), subject)
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)

		// All relations should be rewritten but no self-edge
		for _, rel := range rels {
			require.Equal("admin", rel.Relation, "all relations should be rewritten to 'admin'")
		}
	})

	t.Run("Check_MultipleResources", func(t *testing.T) {
		t.Parallel()

		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAlias("access", subIt)

		pathSeq, err := ctx.Check(aliasIt, NewObjects("document", "doc1", "doc2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)

		// All relations should have "access" as the resource relation
		for _, rel := range rels {
			require.Equal("access", rel.Relation)
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

		pathSeq, err := ctx.IterSubjects(aliasIt, NewObject("document", "doc1"))
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)

		// All relations should have "permission" as the resource relation
		for _, rel := range rels {
			require.Equal("permission", rel.Relation)
			require.Equal("document", rel.Resource.ObjectType)
			require.Equal("doc1", rel.Resource.ObjectID)
		}
	})

	t.Run("IterResources_RelationRewriting", func(t *testing.T) {
		t.Parallel()

		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAlias("can_view", subIt)

		pathSeq, err := ctx.IterResources(aliasIt, NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)

		// All relations should have "can_view" as the resource relation
		for _, rel := range rels {
			require.Equal("can_view", rel.Relation)
		}
	})

	t.Run("Check_EmptySubIterator", func(t *testing.T) {
		t.Parallel()

		subIt := NewEmptyFixedIterator()
		aliasIt := NewAlias("empty_alias", subIt)

		pathSeq, err := ctx.Check(aliasIt, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
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
		pathSeq, err := ctx.Check(aliasIt, NewObjects("document", "doc1"), subject)
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Empty(rels, "no self-edge should be detected for different object types")
	})

	t.Run("Check_SelfEdgeExactMatch", func(t *testing.T) {
		t.Parallel()

		subIt := NewEmptyFixedIterator()
		aliasIt := NewAlias("owner", subIt)

		// Create a perfect self-edge: user:alice#owner@user:alice#owner
		subject := NewObjectAndRelation("alice", "user", "owner")
		pathSeq, err := ctx.Check(aliasIt, NewObjects("user", "alice"), subject)
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should find exactly one self-edge relation
		require.Len(rels, 1, "should find exactly one self-edge relation")

		rel := rels[0]
		expectedResource := NewObjectAndRelation("alice", "user", "owner")
		require.Equal(expectedResource.ObjectID, rel.Resource.ObjectID)
		require.Equal(expectedResource.ObjectType, rel.Resource.ObjectType)
		require.Equal(expectedResource.Relation, rel.Relation)
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

		pathSeq, err := ctx.Check(aliasIt, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err, "initial check should succeed")

		// Error should occur during collection
		_, err = CollectAll(pathSeq)
		require.Error(err, "should propagate collection error from sub-iterator")
	})

	t.Run("IterSubjects_SubIteratorError", func(t *testing.T) {
		t.Parallel()

		// Create a faulty sub-iterator that fails on IterSubjectsImpl
		faultyIt := NewFaultyIterator(true, false)
		aliasIt := NewAlias("error_test", faultyIt)

		_, err := ctx.IterSubjects(aliasIt, NewObject("document", "doc1"))
		require.Error(err, "should propagate error from sub-iterator")
		require.Contains(err.Error(), "faulty iterator error", "should get faulty iterator error")
	})

	t.Run("IterSubjects_SubIteratorCollectionError", func(t *testing.T) {
		t.Parallel()

		// Create a faulty sub-iterator that fails during collection
		faultyIt := NewFaultyIterator(false, true)
		aliasIt := NewAlias("collection_error_test", faultyIt)

		pathSeq, err := ctx.IterSubjects(aliasIt, NewObject("document", "doc1"))
		require.NoError(err, "initial IterSubjects should succeed")

		// Error should occur during collection
		_, err = CollectAll(pathSeq)
		require.Error(err, "should propagate collection error from sub-iterator")
		require.Contains(err.Error(), "faulty iterator collection error", "should get collection error")
	})

	t.Run("IterResources_SubIteratorError", func(t *testing.T) {
		t.Parallel()

		// Create a faulty sub-iterator that fails on IterResourcesImpl
		faultyIt := NewFaultyIterator(true, false)
		aliasIt := NewAlias("error_test", faultyIt)

		_, err := ctx.IterResources(aliasIt, NewObject("user", "alice").WithEllipses())
		require.Error(err, "should propagate error from sub-iterator")
		require.Contains(err.Error(), "faulty iterator error", "should get faulty iterator error")
	})

	t.Run("IterResources_SubIteratorCollectionError", func(t *testing.T) {
		t.Parallel()

		// Create a faulty sub-iterator that fails during collection
		faultyIt := NewFaultyIterator(false, true)
		aliasIt := NewAlias("collection_error_test", faultyIt)

		pathSeq, err := ctx.IterResources(aliasIt, NewObject("user", "alice").WithEllipses())
		require.NoError(err, "initial IterResources should succeed")

		// Error should occur during collection
		_, err = CollectAll(pathSeq)
		require.Error(err, "should propagate collection error from sub-iterator")
		require.Contains(err.Error(), "faulty iterator collection error", "should get collection error")
	})

	t.Run("Check_SelfEdgeWithSubIteratorError", func(t *testing.T) {
		t.Parallel()

		// Create a faulty sub-iterator that errors on CheckImpl
		faultyIt := NewFaultyIterator(true, false)
		aliasIt := NewAlias("self", faultyIt)

		// Create a self-edge scenario
		subject := NewObjectAndRelation("alice", "user", "self")

		// Even with self-edge match, sub-iterator error should be propagated
		_, err := ctx.Check(aliasIt, NewObjects("user", "alice"), subject)
		require.Error(err, "should propagate sub-iterator error even with self-edge")
	})

	t.Run("Check_SelfEdgeWithSubIteratorCollectionError", func(t *testing.T) {
		t.Parallel()

		// Create a faulty sub-iterator that fails during collection
		faultyIt := NewFaultyIterator(false, true)
		aliasIt := NewAlias("self", faultyIt)

		// Create a self-edge scenario
		subject := NewObjectAndRelation("alice", "user", "self")

		pathSeq, err := ctx.Check(aliasIt, NewObjects("user", "alice"), subject)
		require.NoError(err, "initial check should succeed")

		// Error should occur during collection, even with self-edge
		_, err = CollectAll(pathSeq)
		require.Error(err, "should propagate collection error from sub-iterator even with self-edge")
	})
}

func TestAliasIteratorAdvancedScenarios(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test context
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	t.Run("Check_MultipleResourcesSelfEdgeWithSubResults", func(t *testing.T) {
		t.Parallel()

		// Create sub-iterator with real data
		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAlias("admin", subIt)

		// Check multiple resources where one matches the subject for self-edge
		subject := NewObjectAndRelation("doc1", "document", "admin")
		pathSeq, err := ctx.Check(aliasIt, NewObjects("document", "doc1", "doc2"), subject)
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should find self-edge plus relations from sub-iterator
		require.NotEmpty(rels, "should find relations including self-edge")

		// Verify all relations have the correct alias relation
		for _, rel := range rels {
			require.Equal("admin", rel.Relation, "all relations should use alias relation")
		}

		// Should find at least one self-edge relation
		foundSelfEdge := false
		for _, rel := range rels {
			if rel.Resource.ObjectID == "doc1" &&
				rel.Resource.ObjectType == "document" &&
				rel.Relation == "admin" &&
				rel.Subject.ObjectID == "doc1" &&
				rel.Subject.ObjectType == "document" &&
				rel.Subject.Relation == "admin" {
				foundSelfEdge = true
				break
			}
		}
		require.True(foundSelfEdge, "should find self-edge relation")
	})

	t.Run("Check_MultipleResourcesMultipleSelfEdges", func(t *testing.T) {
		t.Parallel()

		// Create empty sub-iterator to isolate self-edge logic
		subIt := NewEmptyFixedIterator()
		aliasIt := NewAlias("owner", subIt)

		// Check multiple resources where multiple match the subject
		subject := NewObjectAndRelation("user1", "user", "owner")
		pathSeq, err := ctx.Check(aliasIt, NewObjects("user", "user1", "user2", "user1"), subject)
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should find exactly one self-edge (despite duplicate "user1" in resources)
		require.Len(rels, 1, "should find exactly one self-edge despite multiple matching resources")

		rel := rels[0]
		require.Equal("user1", rel.Resource.ObjectID)
		require.Equal("user", rel.Resource.ObjectType)
		require.Equal("owner", rel.Relation)
		require.Equal(subject.ObjectID, rel.Subject.ObjectID)
		require.Equal(subject.ObjectType, rel.Subject.ObjectType)
		require.Equal(subject.Relation, rel.Subject.Relation)
	})

	t.Run("IterSubjects_EmptySubIterator", func(t *testing.T) {
		t.Parallel()

		subIt := NewEmptyFixedIterator()
		aliasIt := NewAlias("empty_relation", subIt)

		pathSeq, err := ctx.IterSubjects(aliasIt, NewObject("document", "doc1"))
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Empty(rels, "empty sub-iterator should return no results for IterSubjects")
	})

	t.Run("IterResources_EmptySubIterator", func(t *testing.T) {
		t.Parallel()

		subIt := NewEmptyFixedIterator()
		aliasIt := NewAlias("empty_relation", subIt)

		pathSeq, err := ctx.IterResources(aliasIt, NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Empty(rels, "empty sub-iterator should return no results for IterResources")
	})

	t.Run("Check_LargeResultSet", func(t *testing.T) {
		t.Parallel()

		// Use iterator with many results
		subIt := NewLargeFixedIterator()
		aliasIt := NewAlias("massive", subIt)

		// NewLargeFixedIterator creates relations for user0, user1, etc. on doc0, doc1, etc.
		pathSeq, err := ctx.Check(aliasIt, NewObjects("document", "doc0"), NewObject("user", "user0").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)

		// All relations should be rewritten correctly
		for _, rel := range rels {
			require.Equal("massive", rel.Relation, "all relations should use alias relation")
		}

		// Should have at least one relation (user0 has viewer access to doc0)
		require.NotEmpty(rels, "should have relations from large iterator")
	})

	t.Run("Clone_Independence", func(t *testing.T) {
		t.Parallel()

		subIt := NewDocumentAccessFixedIterator()
		original := NewAlias("original", subIt)

		// Clone the iterator
		cloned := original.Clone().(*Alias)
		require.NotSame(original, cloned, "clone should be different instance")
		require.NotSame(original.subIt, cloned.subIt, "sub-iterator should also be cloned")

		// Both should work independently
		subject := NewObject("user", "alice").WithEllipses()

		// Test original
		pathSeq1, err := ctx.Check(original, NewObjects("document", "doc1"), subject)
		require.NoError(err)
		rels1, err := CollectAll(pathSeq1)
		require.NoError(err)

		// Test clone
		pathSeq2, err := ctx.Check(cloned, NewObjects("document", "doc1"), subject)
		require.NoError(err)
		rels2, err := CollectAll(pathSeq2)
		require.NoError(err)

		// Results should be the same
		require.Len(rels1, len(rels2), "original and clone should return same number of results")

		for i, rel := range rels1 {
			require.Equal("original", rel.Relation, "original should use original relation")
			require.Equal("original", rels2[i].Relation, "clone should use original relation")
		}
	})

	t.Run("Check_EarlyReturnBasicRewriting", func(t *testing.T) {
		t.Parallel()

		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAlias("early_test", subIt)

		pathSeq, err := ctx.Check(aliasIt, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		// Consume only the first path and stop iteration
		count := 0
		for path, err := range pathSeq {
			require.NoError(err)
			require.Equal("early_test", path.Relation, "path should be rewritten")
			count++
			if count == 1 {
				break // Early return - this should trigger the uncovered yield return false path
			}
		}
		require.Equal(1, count, "should have consumed exactly one path")
	})

	t.Run("Check_EarlyReturnSelfEdgeWithSubResults", func(t *testing.T) {
		t.Parallel()

		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAlias("admin", subIt)

		// Create scenario with self-edge and sub-results
		subject := NewObjectAndRelation("doc1", "document", "admin")
		pathSeq, err := ctx.Check(aliasIt, NewObjects("document", "doc1"), subject)
		require.NoError(err)

		// Consume only the first path (self-edge) and stop iteration
		count := 0
		for path, err := range pathSeq {
			require.NoError(err)
			require.Equal("admin", path.Relation, "path should use alias relation")
			count++
			if count == 1 {
				break // Early return after self-edge - this triggers uncovered sub-iterator yield return
			}
		}
		require.Equal(1, count, "should have consumed exactly one path")
	})

	t.Run("IterSubjects_EarlyReturn", func(t *testing.T) {
		t.Parallel()

		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAlias("early_subjects", subIt)

		pathSeq, err := ctx.IterSubjects(aliasIt, NewObject("document", "doc1"))
		require.NoError(err)

		// Consume only the first path and stop iteration
		count := 0
		for path, err := range pathSeq {
			require.NoError(err)
			require.Equal("early_subjects", path.Relation, "path should be rewritten")
			count++
			if count == 1 {
				break // Early return - this should trigger the uncovered yield return false path
			}
		}
		require.Equal(1, count, "should have consumed exactly one path")
	})

	t.Run("IterResources_EarlyReturn", func(t *testing.T) {
		t.Parallel()

		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAlias("early_resources", subIt)

		pathSeq, err := ctx.IterResources(aliasIt, NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		// Consume only the first path and stop iteration
		count := 0
		for path, err := range pathSeq {
			require.NoError(err)
			require.Equal("early_resources", path.Relation, "path should be rewritten")
			count++
			if count == 1 {
				break // Early return - this should trigger the uncovered yield return false path
			}
		}
		require.Equal(1, count, "should have consumed exactly one path")
	})

	t.Run("Check_SelfEdgeWithSubIteratorPathRewriting", func(t *testing.T) {
		t.Parallel()

		// Use sub-iterator that will return paths that need rewriting
		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAlias("admin", subIt)

		// Create a self-edge scenario where sub-iterator also returns paths
		subject := NewObjectAndRelation("doc1", "document", "admin")
		pathSeq, err := ctx.Check(aliasIt, NewObjects("document", "doc1"), subject)
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should find at least self-edge plus rewritten paths from sub-iterator
		require.NotEmpty(rels, "should find both self-edge and sub-iterator paths")

		// All relations should have the alias relation
		for _, rel := range rels {
			require.Equal("admin", rel.Relation, "all paths should use alias relation")
		}

		// Should find at least one self-edge
		foundSelfEdge := false
		for _, rel := range rels {
			if rel.Resource.ObjectID == "doc1" &&
				rel.Resource.ObjectType == "document" &&
				rel.Relation == "admin" &&
				rel.Subject.ObjectID == "doc1" &&
				rel.Subject.ObjectType == "document" &&
				rel.Subject.Relation == "admin" {
				foundSelfEdge = true
				break
			}
		}
		require.True(foundSelfEdge, "should find self-edge relation")
	})
}
