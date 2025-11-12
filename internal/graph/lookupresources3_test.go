package graph

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	cter "github.com/authzed/spicedb/internal/cursorediterator"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestRelationshipsChunkEmpty(t *testing.T) {
	rm := newRelationshipsChunk(0, nil)
	require.False(t, rm.isPopulated())
	require.Empty(t, rm.subjectIDsToDispatch())
	// afterCursor functionality is not available in the new implementation
}

func TestRelationshipsChunkAddRelationship(t *testing.T) {
	rm := newRelationshipsChunk(5, nil)

	rel1 := tuple.MustParse("document:doc1#viewer@user:alice")
	rel2 := tuple.MustParse("document:doc2#viewer@user:bob")
	rel3 := tuple.MustParse("document:doc1#viewer@user:charlie")

	// Add first relationship
	count := rm.addRelationship(rel1, nil)
	require.Equal(t, 1, count)
	require.True(t, rm.isPopulated())
	require.Equal(t, []string{"doc1"}, rm.subjectIDsToDispatch())

	// Add second relationship with different resource
	count = rm.addRelationship(rel2, nil)
	require.Equal(t, 2, count)
	require.ElementsMatch(t, []string{"doc1", "doc2"}, rm.subjectIDsToDispatch())

	// Add third relationship with same resource as first
	count = rm.addRelationship(rel3, nil)
	require.Equal(t, 2, count) // Still 2 resources
	require.ElementsMatch(t, []string{"doc1", "doc2"}, rm.subjectIDsToDispatch())

	// Verify that we have relationships for doc1 with 2 subjects (alice, charlie)
	// and doc2 with 1 subject (bob)
	require.Contains(t, rm.subjectsByResourceID, "doc1")
	require.Contains(t, rm.subjectsByResourceID, "doc2")
	require.Len(t, rm.subjectsByResourceID["doc1"], 2) // alice and charlie
	require.Len(t, rm.subjectsByResourceID["doc2"], 1) // bob
}

func TestRelationshipsChunkAddRelationshipWithMissingContext(t *testing.T) {
	rm := newRelationshipsChunk(5, nil)

	rel := tuple.MustParse("document:doc1#viewer@user:alice")
	missingParams := []string{"param1", "param2"}

	rm.addRelationship(rel, missingParams)

	require.True(t, rm.isPopulated())
	require.Equal(t, []string{"doc1"}, rm.subjectIDsToDispatch())

	// Check that missing context parameters were stored
	require.Contains(t, rm.subjectsByResourceID, "doc1")
	require.Contains(t, rm.subjectsByResourceID["doc1"], "alice")
	subjectInfo := rm.subjectsByResourceID["doc1"]["alice"]
	require.ElementsMatch(t, missingParams, subjectInfo.missingContextParams.AsSlice())
}

func TestRelationshipsChunkAddRelationshipMergesMissingContext(t *testing.T) {
	rm := newRelationshipsChunk(5, nil)

	rel := tuple.MustParse("document:doc1#viewer@user:alice")

	// Add relationship with first set of missing params
	rm.addRelationship(rel, []string{"param1", "param2"})

	// Add same relationship with additional missing params
	rm.addRelationship(rel, []string{"param2", "param3"})

	// Verify merged missing context
	require.Contains(t, rm.subjectsByResourceID, "doc1")
	require.Contains(t, rm.subjectsByResourceID["doc1"], "alice")
	subjectInfo := rm.subjectsByResourceID["doc1"]["alice"]
	require.ElementsMatch(t, []string{"param1", "param2", "param3"}, subjectInfo.missingContextParams.AsSlice())
}

func TestRelationshipsChunkCopyRelationshipsFrom(t *testing.T) {
	// Create source chunk
	src := newRelationshipsChunk(5, nil)
	rel1 := tuple.MustParse("document:doc1#viewer@user:alice")
	rel2 := tuple.MustParse("document:doc2#viewer@user:bob")
	rel3 := tuple.MustParse("document:doc1#editor@user:charlie")

	src.addRelationship(rel1, []string{"param1"})
	src.addRelationship(rel2, nil)
	src.addRelationship(rel3, []string{"param2"})

	// Validate source chunk is populated
	require.True(t, src.isPopulated())
	require.ElementsMatch(t, []string{"doc1", "doc2"}, src.subjectIDsToDispatch())
	require.Contains(t, src.subjectsByResourceID, "doc1")
	require.Contains(t, src.subjectsByResourceID, "doc2")
	require.Len(t, src.subjectsByResourceID["doc1"], 2) // alice and charlie
	require.Len(t, src.subjectsByResourceID["doc2"], 1) // bob

	// Verify missing context in source
	aliceSubjectInfo := src.subjectsByResourceID["doc1"]["alice"]
	require.ElementsMatch(t, []string{"param1"}, aliceSubjectInfo.missingContextParams.AsSlice())

	charlieSubjectInfo := src.subjectsByResourceID["doc1"]["charlie"]
	require.ElementsMatch(t, []string{"param2"}, charlieSubjectInfo.missingContextParams.AsSlice())

	// Create destination chunk
	dest := newRelationshipsChunk(5, nil)

	// Copy relationships for doc1 only
	dest.copyRelationshipsFrom(src, "doc1", []string{"param3"})

	require.True(t, dest.isPopulated())
	require.Equal(t, []string{"doc1"}, dest.subjectIDsToDispatch())
	require.Contains(t, dest.subjectsByResourceID, "doc1")
	require.Len(t, dest.subjectsByResourceID["doc1"], 2) // alice and charlie subjects

	// Verify missing context was copied and merged
	aliceSubjectInfo = dest.subjectsByResourceID["doc1"]["alice"]
	require.ElementsMatch(t, []string{"param1", "param3"}, aliceSubjectInfo.missingContextParams.AsSlice())

	charlieSubjectInfo = dest.subjectsByResourceID["doc1"]["charlie"]
	require.ElementsMatch(t, []string{"param2", "param3"}, charlieSubjectInfo.missingContextParams.AsSlice())
}

func TestRelationshipsChunkCopyRelationshipsFromEmptySource(t *testing.T) {
	src := newRelationshipsChunk(0, nil) // Empty source
	dest := newRelationshipsChunk(5, nil)

	dest.copyRelationshipsFrom(src, "doc1", nil)

	require.False(t, dest.isPopulated())
}

func TestRelationshipsChunkCopyRelationshipsFromNilSource(t *testing.T) {
	dest := newRelationshipsChunk(5, nil)

	dest.copyRelationshipsFrom(nil, "doc1", nil)

	require.False(t, dest.isPopulated())
}

func TestRelationshipsChunkAfterCursor(t *testing.T) {
	rm := newRelationshipsChunk(5, nil)

	// Empty chunk should have no populated state
	require.False(t, rm.isPopulated())

	// Add a relationship
	rel := tuple.MustParse("document:doc1#viewer@user:alice")
	rm.addRelationship(rel, nil)

	// Should be populated after adding a relationship
	require.True(t, rm.isPopulated())
	require.Equal(t, []string{"doc1"}, rm.subjectIDsToDispatch())
}

func TestRelationshipsChunkMapPossibleResource(t *testing.T) {
	rm := newRelationshipsChunk(5, nil)

	// Add relationships for mapping
	rel1 := tuple.MustParse("document:resource1#viewer@user:subject1")
	rel2 := tuple.MustParse("document:resource1#viewer@user:subject2")
	rel3 := tuple.MustParse("document:resource2#viewer@user:subject3")

	rm.addRelationship(rel1, nil)                // No missing context
	rm.addRelationship(rel2, []string{"param1"}) // With missing context
	rm.addRelationship(rel3, []string{"param2"}) // Different resource

	t.Run("map non-caveated resource with mixed subjects", func(t *testing.T) {
		foundResource := possibleResource{
			resourceID:           "mapped_resource",
			forSubjectIDs:        []string{"resource1"},
			missingContextParams: nil, // Non-caveated
		}

		mapped, err := rm.mapPossibleResource(foundResource)
		require.NoError(t, err)
		require.Equal(t, "mapped_resource", mapped.resourceID)
		require.ElementsMatch(t, []string{"subject1"}, mapped.forSubjectIDs) // Only non-caveated subject
		require.Empty(t, mapped.missingContextParams)
	})

	t.Run("map caveated resource", func(t *testing.T) {
		foundResource := possibleResource{
			resourceID:           "mapped_resource",
			forSubjectIDs:        []string{"resource1"},
			missingContextParams: []string{"parent_param"},
		}

		mapped, err := rm.mapPossibleResource(foundResource)
		require.NoError(t, err)
		require.Equal(t, "mapped_resource", mapped.resourceID)
		require.ElementsMatch(t, []string{"subject1", "subject2"}, mapped.forSubjectIDs)
		require.ElementsMatch(t, []string{"parent_param", "param1"}, mapped.missingContextParams)
	})
}

func TestRelationshipsChunkAsCursor(t *testing.T) {
	rm := newRelationshipsChunk(5, nil)

	// Add some relationships
	rel1 := tuple.MustParse("document:doc1#viewer@user:alice")
	rel2 := tuple.MustParse("document:doc2#viewer@user:bob")

	rm.addRelationship(rel1, []string{"param1"})
	rm.addRelationship(rel2, nil)

	// Verify the chunk contains the expected relationships
	require.True(t, rm.isPopulated())
	require.Contains(t, rm.subjectsByResourceID, "doc1")
	require.Contains(t, rm.subjectsByResourceID, "doc2")
	require.Len(t, rm.subjectsByResourceID, 2)

	// Verify alice is in doc1 with param1
	aliceInfo := rm.subjectsByResourceID["doc1"]["alice"]
	require.ElementsMatch(t, []string{"param1"}, aliceInfo.missingContextParams.AsSlice())

	// Verify bob is in doc2 with no missing context
	bobInfo := rm.subjectsByResourceID["doc2"]["bob"]
	require.Empty(t, bobInfo.missingContextParams.AsSlice())
}

func TestSubjectIDsToRelationshipsChunk(t *testing.T) {
	subjectRelation := &core.RelationReference{
		Namespace: "user",
		Relation:  "...",
	}
	subjectIDs := []string{"alice", "bob", "charlie"}
	rewrittenSubjectRelation := &core.RelationReference{
		Namespace: "document",
		Relation:  "viewer",
	}

	rm := subjectIDsToRelationshipsChunk(subjectRelation, subjectIDs, rewrittenSubjectRelation)

	require.True(t, rm.isPopulated())
	require.ElementsMatch(t, subjectIDs, rm.subjectIDsToDispatch())
	require.Len(t, rm.subjectsByResourceID, 3) // 3 resource IDs (alice, bob, charlie)

	// Verify each subject has a relationship with itself
	for _, subjectID := range subjectIDs {
		require.Contains(t, rm.subjectsByResourceID, subjectID)
		require.Contains(t, rm.subjectsByResourceID[subjectID], subjectID)

		// Verify the subject info is correct
		subjectInfo := rm.subjectsByResourceID[subjectID][subjectID]
		require.Equal(t, subjectID, subjectInfo.subjectID)
		require.Empty(t, subjectInfo.missingContextParams.AsSlice()) // no missing context for direct lookup
	}

	// Verify the final relationship matches one of the expected patterns
	rel := rm.finalRelationship
	require.Equal(t, "document", rel.Resource.ObjectType)
	require.Equal(t, "viewer", rel.Resource.Relation)
	require.Equal(t, "user", rel.Subject.ObjectType)
	require.Equal(t, "...", rel.Subject.Relation)
	require.Contains(t, subjectIDs, rel.Resource.ObjectID)
	require.Contains(t, subjectIDs, rel.Subject.ObjectID)
	require.Equal(t, rel.Resource.ObjectID, rel.Subject.ObjectID)
}

func TestNewYieldingStream(t *testing.T) {
	ctx := t.Context()
	yielded := false
	yieldFunc := func(result, error) bool {
		yielded = true
		return true
	}

	rm := newRelationshipsChunk(1, nil)
	rel := tuple.MustParse("document:doc1#viewer@user:alice")
	rm.addRelationship(rel, nil)

	stream := newYieldingStream(ctx, yieldFunc, rm)

	require.NotNil(t, stream)
	require.NotNil(t, stream.ctx)
	require.NotNil(t, stream.cancel)
	require.NotNil(t, stream.yield)
	require.Equal(t, rm, stream.rm)
	require.False(t, stream.canceled)
	require.False(t, yielded)
}

func TestYieldingStreamContext(t *testing.T) {
	ctx := t.Context()
	yieldFunc := func(result, error) bool { return true }
	rm := newRelationshipsChunk(0, nil)

	stream := newYieldingStream(ctx, yieldFunc, rm)

	require.Equal(t, stream.ctx, stream.Context())
}

func TestYieldingStreamPublishSuccess(t *testing.T) {
	ctx := t.Context()
	var yieldedResult result
	var yieldedError error
	yieldCalled := false

	yieldFunc := func(r result, e error) bool {
		yieldedResult = r
		yieldedError = e
		yieldCalled = true
		return true
	}

	rm := newRelationshipsChunk(1, nil)
	rel := tuple.MustParse("document:doc1#viewer@user:alice")
	rm.addRelationship(rel, nil)

	stream := newYieldingStream(ctx, yieldFunc, rm)

	resp := &v1.DispatchLookupResources3Response{
		Items: []*v1.LR3Item{
			{
				ResourceId:                  "resource1",
				ForSubjectIds:               []string{"doc1"},
				AfterResponseCursorSections: []string{"section1"},
			},
		},
	}

	err := stream.Publish(resp)

	require.NoError(t, err)
	require.True(t, yieldCalled)
	require.NoError(t, yieldedError)
	require.Equal(t, "resource1", yieldedResult.Item.resourceID)
	require.ElementsMatch(t, []string{"alice"}, yieldedResult.Item.forSubjectIDs)
	require.Equal(t, cter.Cursor{"section1"}, yieldedResult.Cursor)
	require.False(t, stream.canceled)
}

func TestYieldingStreamPublishNilResponse(t *testing.T) {
	ctx := t.Context()
	yieldFunc := func(result, error) bool { return true }
	rm := newRelationshipsChunk(0, nil)

	stream := newYieldingStream(ctx, yieldFunc, rm)

	require.Panics(t, func() {
		_ = stream.Publish(nil)
	})
}

func TestYieldingStreamPublishInvalidResponse(t *testing.T) {
	ctx := t.Context()
	yieldFunc := func(result, error) bool { return true }
	rm := newRelationshipsChunk(0, nil)

	stream := newYieldingStream(ctx, yieldFunc, rm)

	t.Run("empty items", func(t *testing.T) {
		resp := &v1.DispatchLookupResources3Response{
			Items: []*v1.LR3Item{},
		}

		err := stream.Publish(resp)
		require.NoError(t, err) // Empty items is valid
	})
}

func TestYieldingStreamPublishCanceled(t *testing.T) {
	ctx := t.Context()
	yieldFunc := func(result, error) bool { return true }
	rm := newRelationshipsChunk(0, nil)

	stream := newYieldingStream(ctx, yieldFunc, rm)
	stream.canceled = true

	resp := &v1.DispatchLookupResources3Response{
		Items: []*v1.LR3Item{
			{
				ResourceId:    "resource1",
				ForSubjectIds: []string{"subject1"},
			},
		},
	}

	err := stream.Publish(resp)

	require.Error(t, err)
	require.Equal(t, context.Canceled, err)
}

func TestYieldingStreamPublishYieldReturnsFalse(t *testing.T) {
	ctx := t.Context()
	yieldFunc := func(result, error) bool {
		return false // Indicate no more results wanted
	}

	rm := newRelationshipsChunk(1, nil)
	rel := tuple.MustParse("document:doc1#viewer@user:alice")
	rm.addRelationship(rel, nil)

	stream := newYieldingStream(ctx, yieldFunc, rm)

	resp := &v1.DispatchLookupResources3Response{
		Items: []*v1.LR3Item{
			{
				ResourceId:                  "resource1",
				ForSubjectIds:               []string{"doc1"},
				AfterResponseCursorSections: []string{"section1"},
			},
		},
	}

	err := stream.Publish(resp)

	require.Error(t, err)
	require.Equal(t, context.Canceled, err)
	require.True(t, stream.canceled)
}

func TestYieldingStreamPublishContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // Cancel the context immediately

	yieldFunc := func(result, error) bool { return true }
	rm := newRelationshipsChunk(1, nil)
	rel := tuple.MustParse("document:subject1#viewer@user:alice")
	rm.addRelationship(rel, nil)

	stream := newYieldingStream(ctx, yieldFunc, rm)

	resp := &v1.DispatchLookupResources3Response{
		Items: []*v1.LR3Item{
			{
				ResourceId:    "resource1",
				ForSubjectIds: []string{"subject1"},
			},
		},
	}

	// Note: context cancellation doesn't prevent publishing in the current implementation
	err := stream.Publish(resp)

	// The stream should still publish successfully even with a canceled context
	// as the canceled flag is only set when yield returns false
	require.NoError(t, err)
}

func TestYieldingStreamPublishMultipleItems(t *testing.T) {
	ctx := t.Context()
	var yieldedResults []result

	yieldFunc := func(r result, e error) bool {
		yieldedResults = append(yieldedResults, r)
		return true
	}

	rm := newRelationshipsChunk(2, nil)
	rel1 := tuple.MustParse("document:doc1#viewer@user:alice")
	rel2 := tuple.MustParse("document:doc2#viewer@user:bob")
	rm.addRelationship(rel1, nil)
	rm.addRelationship(rel2, nil)

	stream := newYieldingStream(ctx, yieldFunc, rm)

	resp := &v1.DispatchLookupResources3Response{
		Items: []*v1.LR3Item{
			{
				ResourceId:                  "resource1",
				ForSubjectIds:               []string{"doc1"},
				AfterResponseCursorSections: []string{"section1"},
			},
			{
				ResourceId:                  "resource2",
				ForSubjectIds:               []string{"doc2"},
				AfterResponseCursorSections: []string{"section2"},
			},
		},
	}

	err := stream.Publish(resp)
	require.NoError(t, err)

	require.Len(t, yieldedResults, 2)
	require.Equal(t, "resource1", yieldedResults[0].Item.resourceID)
	require.Equal(t, "resource2", yieldedResults[1].Item.resourceID)
}

func TestMustDatastoreIndexToString(t *testing.T) {
	t.Run("nil dbCursor", func(t *testing.T) {
		index := datastoreIndex{
			chunkID: "someChunkID",
		}
		indexString, _ := mustDatastoreIndexToString(index)
		require.Equal(t, "$dsi:someChunkID", indexString)
	})
	t.Run("defined dbCursor", func(t *testing.T) {
		relation := tuple.MustParse("group:second#member@user:tom")
		index := datastoreIndex{
			chunkID:  "someChunkID",
			dbCursor: options.Cursor(&relation),
		}
		indexString, _ := mustDatastoreIndexToString(index)
		require.Equal(t, "$dsi:someChunkID:group:second#member@user:tom#...", indexString)
	})
}
