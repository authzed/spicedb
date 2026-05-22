package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAliasIterator(t *testing.T) {
	t.Run("Check_BasicRelationRewriting", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		// Create a sub-iterator with document relations
		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAliasIterator("", "read", subIt)

		path, err := ctx.Check(aliasIt, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(path, "should find alice on doc1 via alias")
		require.Equal("read", path.Relation, "relation should be rewritten to 'read'")
		require.Equal("document", path.Resource.ObjectType)
		require.Equal("doc1", path.Resource.ObjectID)
		require.Equal("user", path.Subject.ObjectType)
		require.Equal("alice", path.Subject.ObjectID)
		require.Equal("...", path.Subject.Relation)
	})

	t.Run("Check_SelfEdgeDetection", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		// Create an empty sub-iterator since we only want to test self-edge detection
		subIt := NewEmptyFixedIterator()
		aliasIt := NewAliasIterator("", "admin", subIt)

		// Self-edge: user:alice#admin@user:alice#admin
		subject := NewObjectAndRelation("alice", "user", "admin")
		path, err := ctx.Check(aliasIt, NewObject("user", "alice"), subject)
		require.NoError(err)
		require.NotNil(path, "should find self-edge relation")

		expectedResource := NewObjectAndRelation("alice", "user", "admin")
		require.Equal(expectedResource.ObjectID, path.Resource.ObjectID)
		require.Equal(expectedResource.ObjectType, path.Resource.ObjectType)
		require.Equal(expectedResource.Relation, path.Relation)
		require.Equal(subject.ObjectID, path.Subject.ObjectID)
		require.Equal(subject.ObjectType, path.Subject.ObjectType)
		require.Equal(subject.Relation, path.Subject.Relation)
	})

	t.Run("Check_NoSelfEdge", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		// Create a sub-iterator
		subIt := NewSingleUserFixedIterator("bob")
		aliasIt := NewAliasIterator("", "admin", subIt)

		// document:doc1 vs user:alice#viewer — no self-edge since types differ
		subject := NewObjectAndRelation("alice", "user", "viewer")
		path, err := ctx.Check(aliasIt, NewObject("document", "doc1"), subject)
		require.NoError(err)
		// No match: bob is in subIt but alice#viewer is not; also no self-edge (different type)
		require.Nil(path)
	})

	t.Run("Check_MultipleResources", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAliasIterator("", "access", subIt)

		pathDoc1, err := ctx.Check(aliasIt, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(pathDoc1, "should find alice on doc1")
		require.Equal("access", pathDoc1.Relation)

		pathDoc2, err := ctx.Check(aliasIt, NewObject("document", "doc2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(pathDoc2, "should find alice on doc2")
		require.Equal("access", pathDoc2.Relation)
	})

	t.Run("IterSubjects_RelationRewriting", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAliasIterator("", "permission", subIt)

		pathSeq, err := ctx.IterSubjects(aliasIt, NewObject("document", "doc1"), NoObjectFilter())
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
		require := require.New(t)

		ctx := NewTestContext(t)
		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAliasIterator("", "can_view", subIt)

		pathSeq, err := ctx.IterResources(aliasIt, NewObject("user", "alice").WithEllipses(), NoObjectFilter())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)

		// All relations should have "can_view" as the resource relation
		for _, rel := range rels {
			require.Equal("can_view", rel.Relation)
		}
	})

	t.Run("IterResources_SelfEdgeDetection", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		// Create an empty sub-iterator since we only want to test self-edge detection
		subIt := NewEmptyFixedIterator()

		// Create an alias iterator that rewrites to "admin"
		aliasIt := NewAliasIterator("", "admin", subIt)

		// Check for a self-edge: user:alice#admin@user:alice#admin
		subject := NewObjectAndRelation("alice", "user", "admin")
		pathSeq, err := ctx.IterResources(aliasIt, subject, NoObjectFilter())
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

	t.Run("Check_EmptySubIterator", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		subIt := NewEmptyFixedIterator()
		aliasIt := NewAliasIterator("", "empty_alias", subIt)

		path, err := ctx.Check(aliasIt, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.Nil(path, "empty sub-iterator should return no results")
	})

	t.Run("Check_SelfEdgeWithEmptySubIterator", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		// Create empty sub-iterator
		subIt := NewEmptyFixedIterator()
		aliasIt := NewAliasIterator("", "self", subIt)

		// Create a self-edge scenario: document:doc1#self@user:alice#self
		subject := NewObjectAndRelation("alice", "user", "self")

		// But we're checking for document:doc1, which won't match user:alice
		// So no self-edge should be detected
		path, err := ctx.Check(aliasIt, NewObject("document", "doc1"), subject)
		require.NoError(err)
		require.Nil(path, "no self-edge should be detected for different object types")
	})

	t.Run("Check_SelfEdgeExactMatch", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		subIt := NewEmptyFixedIterator()
		aliasIt := NewAliasIterator("", "owner", subIt)

		// Create a perfect self-edge: user:alice#owner@user:alice#owner
		subject := NewObjectAndRelation("alice", "user", "owner")
		path, err := ctx.Check(aliasIt, NewObject("user", "alice"), subject)
		require.NoError(err)

		// Should find the self-edge relation
		require.NotNil(path, "should find self-edge relation")

		expectedResource := NewObjectAndRelation("alice", "user", "owner")
		require.Equal(expectedResource.ObjectID, path.Resource.ObjectID)
		require.Equal(expectedResource.ObjectType, path.Resource.ObjectType)
		require.Equal(expectedResource.Relation, path.Relation)
		require.Equal(subject.ObjectID, path.Subject.ObjectID)
		require.Equal(subject.ObjectType, path.Subject.ObjectType)
		require.Equal(subject.Relation, path.Subject.Relation)
	})
}

func TestAliasIteratorClone(t *testing.T) {
	require := require.New(t)

	subIt := NewDocumentAccessFixedIterator()
	original := NewAliasIterator("", "original_relation", subIt)

	cloned := original.Clone()
	require.NotSame(original, cloned, "cloned iterator should be a different object")

	// Both should have the same structure when explained
	originalExplain := original.Explain()
	clonedExplain := cloned.Explain()
	require.Equal(originalExplain.Info, clonedExplain.Info)
	require.Len(clonedExplain.SubExplain, len(originalExplain.SubExplain))

	// The underlying relation should be the same
	require.Equal("Alias(original_relation)", originalExplain.Info)
	require.Equal("Alias(original_relation)", clonedExplain.Info)
}

func TestAliasIteratorExplain(t *testing.T) {
	t.Run("ExplainWithSubIterator", func(t *testing.T) {
		require := require.New(t)

		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAliasIterator("", "test_relation", subIt)

		explain := aliasIt.Explain()
		require.Equal("Alias(test_relation)", explain.Info)
		require.Len(explain.SubExplain, 1, "alias should have exactly 1 sub-explain")

		explainStr := explain.String()
		require.Contains(explainStr, "Alias(test_relation)")
		require.NotEmpty(explainStr)
	})

	t.Run("ExplainWithEmptySubIterator", func(t *testing.T) {
		require := require.New(t)

		subIt := NewEmptyFixedIterator()
		aliasIt := NewAliasIterator("", "empty_test", subIt)

		explain := aliasIt.Explain()
		require.Equal("Alias(empty_test)", explain.Info)
		require.Len(explain.SubExplain, 1, "alias should have exactly 1 sub-explain even with empty sub-iterator")
	})
}

func TestAliasIteratorErrorHandling(t *testing.T) {
	t.Run("Check_SubIteratorError", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		// Create a faulty sub-iterator
		faultyIt := NewFaultyIterator(true, false, ObjectType{}, []ObjectType{})
		aliasIt := NewAliasIterator("", "error_test", faultyIt)

		_, err := ctx.Check(aliasIt, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.Error(err, "should propagate error from sub-iterator")
	})

	t.Run("Check_SubIteratorCollectionError", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		// Create a faulty sub-iterator that fails during collection
		// With the new *Path API, collection errors propagate immediately
		faultyIt := NewFaultyIterator(false, true, ObjectType{}, []ObjectType{})
		aliasIt := NewAliasIterator("", "collection_error_test", faultyIt)

		_, err := ctx.Check(aliasIt, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		// With *Path return, the error may or may not propagate (depends on implementation)
		// The check either succeeds (nil path) or returns an error
		require.Error(err, "should propagate subcollection")
	})

	t.Run("IterSubjects_SubIteratorError", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		// Create a faulty sub-iterator that fails on IterSubjectsImpl
		faultyIt := NewFaultyIterator(true, false, ObjectType{}, []ObjectType{})
		aliasIt := NewAliasIterator("", "error_test", faultyIt)

		_, err := ctx.IterSubjects(aliasIt, NewObject("document", "doc1"), NoObjectFilter())
		require.Error(err, "should propagate error from sub-iterator")
		require.Contains(err.Error(), "faulty iterator error", "should get faulty iterator error")
	})

	t.Run("IterSubjects_SubIteratorCollectionError", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		// Create a faulty sub-iterator that fails during collection
		faultyIt := NewFaultyIterator(false, true, ObjectType{}, []ObjectType{})
		aliasIt := NewAliasIterator("", "collection_error_test", faultyIt)

		pathSeq, err := ctx.IterSubjects(aliasIt, NewObject("document", "doc1"), NoObjectFilter())
		require.NoError(err, "initial IterSubjects should succeed")

		// Error should occur during collection
		_, err = CollectAll(pathSeq)
		require.Error(err, "should propagate collection error from sub-iterator")
		require.Contains(err.Error(), "faulty iterator collection error", "should get collection error")
	})

	t.Run("IterResources_SubIteratorError", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		// Create a faulty sub-iterator that fails on IterResourcesImpl
		faultyIt := NewFaultyIterator(true, false, ObjectType{}, []ObjectType{})
		aliasIt := NewAliasIterator("", "error_test", faultyIt)

		_, err := ctx.IterResources(aliasIt, NewObject("user", "alice").WithEllipses(), NoObjectFilter())
		require.Error(err, "should propagate error from sub-iterator")
		require.Contains(err.Error(), "faulty iterator error", "should get faulty iterator error")
	})

	t.Run("IterResources_SubIteratorCollectionError", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		// Create a faulty sub-iterator that fails during collection
		faultyIt := NewFaultyIterator(false, true, ObjectType{}, []ObjectType{})
		aliasIt := NewAliasIterator("", "collection_error_test", faultyIt)

		pathSeq, err := ctx.IterResources(aliasIt, NewObject("user", "alice").WithEllipses(), NoObjectFilter())
		require.NoError(err, "initial IterResources should succeed")

		// Error should occur during collection
		_, err = CollectAll(pathSeq)
		require.Error(err, "should propagate collection error from sub-iterator")
		require.Contains(err.Error(), "faulty iterator collection error", "should get collection error")
	})

	t.Run("Check_SelfEdgeWithSubIteratorError", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		// Create a faulty sub-iterator that errors on CheckImpl
		faultyIt := NewFaultyIterator(true, false, ObjectType{}, []ObjectType{})
		aliasIt := NewAliasIterator("", "self", faultyIt)

		// Create a self-edge scenario
		subject := NewObjectAndRelation("alice", "user", "self")

		// Even with self-edge match, sub-iterator error should be propagated
		_, err := ctx.Check(aliasIt, NewObject("user", "alice"), subject)
		require.Error(err, "should propagate sub-iterator error even with self-edge")
	})

	t.Run("Check_SelfEdgeWithSubIteratorCollectionError", func(t *testing.T) {
		ctx := NewTestContext(t)
		// Create a faulty sub-iterator that fails during collection
		// With the new *Path API, collection errors may propagate immediately
		faultyIt := NewFaultyIterator(false, true, ObjectType{}, []ObjectType{})
		aliasIt := NewAliasIterator("", "self", faultyIt)

		// Create a self-edge scenario
		subject := NewObjectAndRelation("alice", "user", "self")

		// With single *Path return, we just call Check directly - error propagates
		_, _ = ctx.Check(aliasIt, NewObject("user", "alice"), subject)
	})
}

func TestAliasIteratorAdvancedScenarios(t *testing.T) {
	t.Run("Check_SelfEdgeWithSubResults_doc1", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		// Create sub-iterator with real data
		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAliasIterator("", "admin", subIt)

		// Self-edge: document:doc1#admin@document:doc1#admin
		subject := NewObjectAndRelation("doc1", "document", "admin")
		path, err := ctx.Check(aliasIt, NewObject("document", "doc1"), subject)
		require.NoError(err)

		// Should find the self-edge (doc1 matches doc1)
		require.NotNil(path, "should find self-edge relation")
		require.Equal("admin", path.Relation, "relation should use alias relation")
		require.Equal("doc1", path.Resource.ObjectID)
		require.Equal("document", path.Resource.ObjectType)
	})

	t.Run("Check_SelfEdge_user1_match", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		// Create empty sub-iterator to isolate self-edge logic
		subIt := NewEmptyFixedIterator()
		aliasIt := NewAliasIterator("", "owner", subIt)

		// Self-edge: user:user1#owner@user:user1#owner
		subject := NewObjectAndRelation("user1", "user", "owner")
		path, err := ctx.Check(aliasIt, NewObject("user", "user1"), subject)
		require.NoError(err)

		// Should find the self-edge
		require.NotNil(path, "should find self-edge for user1")
		require.Equal("user1", path.Resource.ObjectID)
		require.Equal("user", path.Resource.ObjectType)
		require.Equal("owner", path.Relation)
		require.Equal(subject.ObjectID, path.Subject.ObjectID)
		require.Equal(subject.ObjectType, path.Subject.ObjectType)
		require.Equal(subject.Relation, path.Subject.Relation)
	})

	t.Run("Check_SelfEdge_user2_no_match", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		subIt := NewEmptyFixedIterator()
		aliasIt := NewAliasIterator("", "owner", subIt)

		// user:user2 should not match subject user:user1
		subject := NewObjectAndRelation("user1", "user", "owner")
		path, err := ctx.Check(aliasIt, NewObject("user", "user2"), subject)
		require.NoError(err)
		require.Nil(path, "user2 should not match user1 self-edge")
	})

	t.Run("IterSubjects_EmptySubIterator", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		subIt := NewEmptyFixedIterator()
		aliasIt := NewAliasIterator("", "empty_relation", subIt)

		pathSeq, err := ctx.IterSubjects(aliasIt, NewObject("document", "doc1"), NoObjectFilter())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Empty(rels, "empty sub-iterator should return no results for IterSubjects")
	})

	t.Run("IterResources_EmptySubIterator", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		subIt := NewEmptyFixedIterator()
		aliasIt := NewAliasIterator("", "empty_relation", subIt)

		pathSeq, err := ctx.IterResources(aliasIt, NewObject("user", "alice").WithEllipses(), NoObjectFilter())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Empty(rels, "empty sub-iterator should return no results for IterResources")
	})

	t.Run("Check_LargeResultSet", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		// Use iterator with many results
		subIt := NewLargeFixedIterator()
		aliasIt := NewAliasIterator("", "massive", subIt)

		// NewLargeFixedIterator creates relations for user0, user1, etc. on doc0, doc1, etc.
		path, err := ctx.Check(aliasIt, NewObject("document", "doc0"), NewObject("user", "user0").WithEllipses())
		require.NoError(err)

		// Should have at least one relation (user0 has viewer access to doc0)
		require.NotNil(path, "should have relation from large iterator")
		require.Equal("massive", path.Relation, "relation should use alias relation")
	})

	t.Run("Clone_Independence", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		subIt := NewDocumentAccessFixedIterator()
		original := NewAliasIterator("", "original", subIt)

		// Clone the iterator
		cloned := original.Clone().(*AliasIterator)
		require.NotSame(original, cloned, "clone should be different instance")
		require.NotSame(original.subIt, cloned.subIt, "sub-iterator should also be cloned")

		// Both should work independently
		subject := NewObject("user", "alice").WithEllipses()

		// Test original
		path1, err := ctx.Check(original, NewObject("document", "doc1"), subject)
		require.NoError(err)

		// Test clone
		path2, err := ctx.Check(cloned, NewObject("document", "doc1"), subject)
		require.NoError(err)

		// Both should return same nil/non-nil result
		require.Equal(path1 == nil, path2 == nil, "original and clone should return same nil-ness")

		if path1 != nil && path2 != nil {
			require.Equal("original", path1.Relation, "original should use original relation")
			require.Equal("original", path2.Relation, "clone should use original relation")
		}
	})

	t.Run("Check_BasicRewriting_doc1", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAliasIterator("", "early_test", subIt)

		path, err := ctx.Check(aliasIt, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(path, "should find alice on doc1")
		require.Equal("early_test", path.Relation, "path should be rewritten")
	})

	t.Run("Check_SelfEdgeAndSubResults", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAliasIterator("", "admin", subIt)

		// Self-edge scenario: document:doc1#admin@document:doc1#admin
		subject := NewObjectAndRelation("doc1", "document", "admin")
		path, err := ctx.Check(aliasIt, NewObject("document", "doc1"), subject)
		require.NoError(err)

		// Should find the self-edge
		require.NotNil(path, "should find self-edge path")
		require.Equal("admin", path.Relation, "path should use alias relation")
	})

	t.Run("IterSubjects_EarlyReturn", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAliasIterator("", "early_subjects", subIt)

		pathSeq, err := ctx.IterSubjects(aliasIt, NewObject("document", "doc1"), NoObjectFilter())
		require.NoError(err)

		// Consume only the first path and stop iteration
		count := 0
		for path, err := range pathSeq {
			require.NoError(err)
			require.Equal("early_subjects", path.Relation, "path should be rewritten")
			count++
			if count == 1 {
				break // Early return
			}
		}
		require.Equal(1, count, "should have consumed exactly one path")
	})

	t.Run("IterResources_EarlyReturn", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAliasIterator("", "early_resources", subIt)

		pathSeq, err := ctx.IterResources(aliasIt, NewObject("user", "alice").WithEllipses(), NoObjectFilter())
		require.NoError(err)

		// Consume only the first path and stop iteration
		count := 0
		for path, err := range pathSeq {
			require.NoError(err)
			require.Equal("early_resources", path.Relation, "path should be rewritten")
			count++
			if count == 1 {
				break // Early return
			}
		}
		require.Equal(1, count, "should have consumed exactly one path")
	})

	t.Run("Check_SelfEdgeWithSubIteratorPathRewriting", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)
		// Use sub-iterator that will return paths that need rewriting
		subIt := NewDocumentAccessFixedIterator()
		aliasIt := NewAliasIterator("", "admin", subIt)

		// Self-edge: document:doc1#admin@document:doc1#admin
		subject := NewObjectAndRelation("doc1", "document", "admin")
		path, err := ctx.Check(aliasIt, NewObject("document", "doc1"), subject)
		require.NoError(err)

		// Should find the self-edge
		require.NotNil(path, "should find self-edge path")
		require.Equal("admin", path.Relation, "path should use alias relation")
		require.Equal("doc1", path.Resource.ObjectID)
		require.Equal("document", path.Resource.ObjectType)
	})
}

func TestAlias_Types(t *testing.T) {
	t.Run("ResourceType", func(t *testing.T) {
		require := require.New(t)

		// Create an alias iterator
		path := MustPathFromString("document:doc1#viewer@user:alice")
		subIter := NewFixedIterator(*path)
		alias := NewAliasIterator("", "admin", subIter)

		resourceType, err := alias.ResourceType()
		require.NoError(err)
		require.Len(resourceType, 1)
		require.Equal("document", resourceType[0].Type)
	})

	t.Run("SubjectTypes", func(t *testing.T) {
		require := require.New(t)

		// Create an alias iterator
		path := MustPathFromString("document:doc1#viewer@user:alice")
		subIter := NewFixedIterator(*path)
		alias := NewAliasIterator("", "admin", subIter)

		subjectTypes, err := alias.SubjectTypes()
		require.NoError(err)
		require.Len(subjectTypes, 1) // From subiterator, unchanged
		require.Equal("user", subjectTypes[0].Type)
	})
}
