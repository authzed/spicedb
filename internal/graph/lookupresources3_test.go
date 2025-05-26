package graph

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	cter "github.com/authzed/spicedb/internal/cursorediterator"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestRelationshipsChunkEmpty(t *testing.T) {
	rm := newRelationshipsChunk(0)
	require.False(t, rm.isPopulated())
	require.Empty(t, rm.subjectIDsToDispatch())
	require.Nil(t, rm.afterCursor())
}

func TestRelationshipsChunkAddRelationship(t *testing.T) {
	rm := newRelationshipsChunk(5)

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

	// Verify relationship count
	require.Len(t, rm.underlyingCursor.OrderedRelationships, 3)
}

func TestRelationshipsChunkAddRelationshipWithMissingContext(t *testing.T) {
	rm := newRelationshipsChunk(5)

	rel := tuple.MustParse("document:doc1#viewer@user:alice")
	missingParams := []string{"param1", "param2"}

	rm.addRelationship(rel, missingParams)

	require.True(t, rm.isPopulated())
	require.Equal(t, []string{"doc1"}, rm.subjectIDsToDispatch())

	// Check that missing context parameters were stored
	resource := rm.underlyingCursor.Resources["doc1"]
	require.NotNil(t, resource)
	subject := resource.MissingContextBySubjectId["alice"]
	require.NotNil(t, subject)
	require.ElementsMatch(t, missingParams, subject.MissingContextParams)
}

func TestRelationshipsChunkAddRelationshipMergesMissingContext(t *testing.T) {
	rm := newRelationshipsChunk(5)

	rel := tuple.MustParse("document:doc1#viewer@user:alice")

	// Add relationship with first set of missing params
	rm.addRelationship(rel, []string{"param1", "param2"})

	// Add same relationship with additional missing params
	rm.addRelationship(rel, []string{"param2", "param3"})

	// Verify merged missing context
	resource := rm.underlyingCursor.Resources["doc1"]
	subject := resource.MissingContextBySubjectId["alice"]
	require.ElementsMatch(t, []string{"param1", "param2", "param3"}, subject.MissingContextParams)
}

func TestRelationshipsChunkCopyRelationshipsFrom(t *testing.T) {
	// Create source chunk
	src := newRelationshipsChunk(5)
	rel1 := tuple.MustParse("document:doc1#viewer@user:alice")
	rel2 := tuple.MustParse("document:doc2#viewer@user:bob")
	rel3 := tuple.MustParse("document:doc1#editor@user:charlie")

	src.addRelationship(rel1, []string{"param1"})
	src.addRelationship(rel2, nil)
	src.addRelationship(rel3, []string{"param2"})

	// Validate source chunk is populated
	require.True(t, src.isPopulated())
	require.ElementsMatch(t, []string{"doc1", "doc2"}, src.subjectIDsToDispatch())
	require.Len(t, src.underlyingCursor.OrderedRelationships, 3)

	// Verify missing context in source
	resource := src.underlyingCursor.Resources["doc1"]
	require.NotNil(t, resource)

	aliceSubject := resource.MissingContextBySubjectId["alice"]
	require.NotNil(t, aliceSubject)
	require.ElementsMatch(t, []string{"param1"}, aliceSubject.MissingContextParams)

	charlieSubject := resource.MissingContextBySubjectId["charlie"]
	require.NotNil(t, charlieSubject)
	require.ElementsMatch(t, []string{"param2"}, charlieSubject.MissingContextParams)

	// Create destination chunk
	dest := newRelationshipsChunk(5)

	// Copy relationships for doc1 only
	dest.copyRelationshipsFrom(src, "doc1", []string{"param3"})

	require.True(t, dest.isPopulated())
	require.Equal(t, []string{"doc1"}, dest.subjectIDsToDispatch())
	require.Len(t, dest.underlyingCursor.OrderedRelationships, 2) // rel1 and rel3

	// Verify missing context was copied and merged
	resource = dest.underlyingCursor.Resources["doc1"]
	require.NotNil(t, resource)

	aliceSubject = resource.MissingContextBySubjectId["alice"]
	require.NotNil(t, aliceSubject)
	require.ElementsMatch(t, []string{"param1", "param3"}, aliceSubject.MissingContextParams)

	charlieSubject = resource.MissingContextBySubjectId["charlie"]
	require.NotNil(t, charlieSubject)
	require.ElementsMatch(t, []string{"param2", "param3"}, charlieSubject.MissingContextParams)
}

func TestRelationshipsChunkCopyRelationshipsFromEmptySource(t *testing.T) {
	src := newRelationshipsChunk(0) // Empty source
	dest := newRelationshipsChunk(5)

	dest.copyRelationshipsFrom(src, "doc1", nil)

	require.False(t, dest.isPopulated())
}

func TestRelationshipsChunkCopyRelationshipsFromNilSource(t *testing.T) {
	dest := newRelationshipsChunk(5)

	dest.copyRelationshipsFrom(nil, "doc1", nil)

	require.False(t, dest.isPopulated())
}

func TestRelationshipsChunkAfterCursor(t *testing.T) {
	rm := newRelationshipsChunk(5)

	// Empty chunk should return nil cursor
	require.Nil(t, rm.afterCursor())

	// Add a relationship
	rel := tuple.MustParse("document:doc1#viewer@user:alice")
	rm.addRelationship(rel, nil)

	// Should return cursor for last relationship
	cursor := rm.afterCursor()
	require.NotNil(t, cursor)
}

func TestRelationshipsChunkMapPossibleResource(t *testing.T) {
	rm := newRelationshipsChunk(5)

	// Add relationships for mapping
	rel1 := tuple.MustParse("document:resource1#viewer@user:subject1")
	rel2 := tuple.MustParse("document:resource1#viewer@user:subject2")
	rel3 := tuple.MustParse("document:resource2#viewer@user:subject3")

	rm.addRelationship(rel1, nil)                // No missing context
	rm.addRelationship(rel2, []string{"param1"}) // With missing context
	rm.addRelationship(rel3, []string{"param2"}) // Different resource

	t.Run("map non-caveated resource with mixed subjects", func(t *testing.T) {
		foundResource := &v1.PossibleResource{
			ResourceId:           "mapped_resource",
			ForSubjectIds:        []string{"resource1"},
			MissingContextParams: nil, // Non-caveated
		}

		mapped, err := rm.mapPossibleResource(foundResource)
		require.NoError(t, err)
		require.Equal(t, "mapped_resource", mapped.ResourceId)
		require.ElementsMatch(t, []string{"subject1"}, mapped.ForSubjectIds) // Only non-caveated subject
		require.Empty(t, mapped.MissingContextParams)
	})

	t.Run("map caveated resource", func(t *testing.T) {
		foundResource := &v1.PossibleResource{
			ResourceId:           "mapped_resource",
			ForSubjectIds:        []string{"resource1"},
			MissingContextParams: []string{"parent_param"},
		}

		mapped, err := rm.mapPossibleResource(foundResource)
		require.NoError(t, err)
		require.Equal(t, "mapped_resource", mapped.ResourceId)
		require.ElementsMatch(t, []string{"subject1", "subject2"}, mapped.ForSubjectIds)
		require.ElementsMatch(t, []string{"parent_param", "param1"}, mapped.MissingContextParams)
	})
}

func TestRelationshipsChunkAsCursor(t *testing.T) {
	rm := newRelationshipsChunk(5)

	// Add some relationships
	rel1 := tuple.MustParse("document:doc1#viewer@user:alice")
	rel2 := tuple.MustParse("document:doc2#viewer@user:bob")

	rm.addRelationship(rel1, []string{"param1"})
	rm.addRelationship(rel2, nil)

	cursor := rm.asCursor()
	require.NotNil(t, cursor)
	require.Len(t, cursor.OrderedRelationships, 2)
	require.Len(t, cursor.Resources, 2)

	// Verify the cursor contains the expected data
	require.Contains(t, cursor.Resources, "doc1")
	require.Contains(t, cursor.Resources, "doc2")
}

func TestRelationshipsChunkFromCursor(t *testing.T) {
	t.Run("nil cursor", func(t *testing.T) {
		rm := relationshipsChunkFromCursor(nil)
		require.NotNil(t, rm)
		require.False(t, rm.isPopulated())
	})

	t.Run("empty cursor", func(t *testing.T) {
		cursor := &v1.DatastoreCursor{
			OrderedRelationships: []*core.RelationTuple{},
			Resources:            map[string]*v1.ResourceAndMissingContext{},
		}
		rm := relationshipsChunkFromCursor(cursor)
		require.NotNil(t, rm)
		require.False(t, rm.isPopulated())
	})

	t.Run("populated cursor", func(t *testing.T) {
		rel := tuple.MustParse("document:doc1#viewer@user:alice")
		cursor := &v1.DatastoreCursor{
			OrderedRelationships: []*core.RelationTuple{rel.ToCoreTuple()},
			Resources: map[string]*v1.ResourceAndMissingContext{
				"doc1": {
					ResourceId: "doc1",
					MissingContextBySubjectId: map[string]*v1.SubjectAndMissingContext{
						"alice": {
							SubjectId:            "alice",
							MissingContextParams: []string{"param1"},
						},
					},
				},
			},
		}

		rm := relationshipsChunkFromCursor(cursor)
		require.NotNil(t, rm)
		require.True(t, rm.isPopulated())
		require.Equal(t, []string{"doc1"}, rm.subjectIDsToDispatch())
	})
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
	require.Len(t, rm.underlyingCursor.OrderedRelationships, 3)

	// Verify each relationship was created correctly
	for _, rel := range rm.underlyingCursor.OrderedRelationships {
		require.Equal(t, "document", rel.ResourceAndRelation.Namespace)
		require.Equal(t, "viewer", rel.ResourceAndRelation.Relation)
		require.Equal(t, "user", rel.Subject.Namespace)
		require.Equal(t, "...", rel.Subject.Relation)
		require.Contains(t, subjectIDs, rel.ResourceAndRelation.ObjectId)
		require.Contains(t, subjectIDs, rel.Subject.ObjectId)
		require.Equal(t, rel.ResourceAndRelation.ObjectId, rel.Subject.ObjectId)
	}
}

func TestDatastoreCursorSerialization(t *testing.T) {
	t.Run("nil cursor", func(t *testing.T) {
		str, err := datastoreCursorToString(nil)
		require.NoError(t, err)
		require.Empty(t, str)

		cursor, err := datastoreCursorFromString("")
		require.NoError(t, err)
		require.Nil(t, cursor)
	})

	t.Run("empty cursor", func(t *testing.T) {
		cursor := &v1.DatastoreCursor{
			OrderedRelationships: []*core.RelationTuple{},
			Resources:            map[string]*v1.ResourceAndMissingContext{},
		}

		str, err := datastoreCursorToString(cursor)
		require.NoError(t, err)
		require.Empty(t, str)
	})

	t.Run("populated cursor round trip", func(t *testing.T) {
		rel := tuple.MustParse("document:doc1#viewer@user:alice")
		originalCursor := &v1.DatastoreCursor{
			OrderedRelationships: []*core.RelationTuple{rel.ToCoreTuple()},
			Resources: map[string]*v1.ResourceAndMissingContext{
				"doc1": {
					ResourceId: "doc1",
					MissingContextBySubjectId: map[string]*v1.SubjectAndMissingContext{
						"alice": {
							SubjectId:            "alice",
							MissingContextParams: []string{"param1"},
						},
					},
				},
			},
		}

		// Serialize
		str, err := datastoreCursorToString(originalCursor)
		require.NoError(t, err)
		require.NotEmpty(t, str)

		// Deserialize
		reconstructedCursor, err := datastoreCursorFromString(str)
		require.NoError(t, err)
		require.NotNil(t, reconstructedCursor)

		// Verify data integrity
		require.Len(t, reconstructedCursor.OrderedRelationships, 1)
		require.Len(t, reconstructedCursor.Resources, 1)
		require.Contains(t, reconstructedCursor.Resources, "doc1")

		resource := reconstructedCursor.Resources["doc1"]
		require.Equal(t, "doc1", resource.ResourceId)
		require.Contains(t, resource.MissingContextBySubjectId, "alice")

		subject := resource.MissingContextBySubjectId["alice"]
		require.Equal(t, "alice", subject.SubjectId)
		require.Equal(t, []string{"param1"}, subject.MissingContextParams)
	})

	t.Run("invalid cursor string", func(t *testing.T) {
		_, err := datastoreCursorFromString("invalid-base64")
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid cursor string")
	})
}

func TestNewYieldingStream(t *testing.T) {
	ctx := t.Context()
	yielded := false
	yieldFunc := func(result, error) bool {
		yielded = true
		return true
	}

	rm := newRelationshipsChunk(1)
	rel := tuple.MustParse("document:doc1#viewer@user:alice")
	rm.addRelationship(rel, nil)

	metadata := &v1.ResponseMeta{
		DispatchCount: 1,
	}

	stream := newYieldingStream(ctx, yieldFunc, rm, metadata)

	require.NotNil(t, stream)
	require.NotNil(t, stream.ctx)
	require.NotNil(t, stream.cancel)
	require.NotNil(t, stream.yield)
	require.Equal(t, rm, stream.rm)
	require.Equal(t, metadata, stream.metadata)
	require.True(t, stream.isFirstPublishCall)
	require.False(t, stream.canceled)
	require.False(t, yielded)
}

func TestYieldingStreamContext(t *testing.T) {
	ctx := t.Context()
	yieldFunc := func(result, error) bool { return true }
	rm := newRelationshipsChunk(0)
	metadata := &v1.ResponseMeta{}

	stream := newYieldingStream(ctx, yieldFunc, rm, metadata)

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

	rm := newRelationshipsChunk(1)
	rel := tuple.MustParse("document:doc1#viewer@user:alice")
	rm.addRelationship(rel, nil)

	metadata := &v1.ResponseMeta{
		DispatchCount: 1,
	}

	stream := newYieldingStream(ctx, yieldFunc, rm, metadata)

	resp := &v1.DispatchLookupResources3Response{
		Resource: &v1.PossibleResource{
			ResourceId:    "resource1",
			ForSubjectIds: []string{"doc1"},
		},
		Metadata: &v1.ResponseMeta{
			DispatchCount: 2,
		},
		AfterResponseCursor: &v1.Cursor{
			DispatchVersion: lr3DispatchVersion,
			Sections:        []string{"section1"},
		},
	}

	err := stream.Publish(resp)

	require.NoError(t, err)
	require.True(t, yieldCalled)
	require.NoError(t, yieldedError)
	require.NotNil(t, yieldedResult.Item.possibleResource)
	require.NotNil(t, yieldedResult.Item.metadata)
	require.Equal(t, cter.Cursor{"section1"}, yieldedResult.Cursor)
	require.False(t, stream.isFirstPublishCall)
	require.False(t, stream.canceled)
}

func TestYieldingStreamPublishNilResponse(t *testing.T) {
	ctx := t.Context()
	yieldFunc := func(result, error) bool { return true }
	rm := newRelationshipsChunk(0)
	metadata := &v1.ResponseMeta{}

	stream := newYieldingStream(ctx, yieldFunc, rm, metadata)

	require.Panics(t, func() {
		_ = stream.Publish(nil)
	})
}

func TestYieldingStreamPublishInvalidResponse(t *testing.T) {
	ctx := t.Context()
	yieldFunc := func(result, error) bool { return true }
	rm := newRelationshipsChunk(0)
	metadata := &v1.ResponseMeta{}

	stream := newYieldingStream(ctx, yieldFunc, rm, metadata)

	t.Run("missing metadata", func(t *testing.T) {
		resp := &v1.DispatchLookupResources3Response{
			Resource: &v1.PossibleResource{
				ResourceId:    "resource1",
				ForSubjectIds: []string{"subject1"},
			},
			Metadata: nil,
		}

		require.Panics(t, func() {
			_ = stream.Publish(resp)
		})
	})

	t.Run("missing resource", func(t *testing.T) {
		resp := &v1.DispatchLookupResources3Response{
			Resource: nil,
			Metadata: &v1.ResponseMeta{
				DispatchCount: 1,
			},
		}

		require.Panics(t, func() {
			_ = stream.Publish(resp)
		})
	})
}

func TestYieldingStreamPublishCanceled(t *testing.T) {
	ctx := t.Context()
	yieldFunc := func(result, error) bool { return true }
	rm := newRelationshipsChunk(0)
	metadata := &v1.ResponseMeta{}

	stream := newYieldingStream(ctx, yieldFunc, rm, metadata)
	stream.canceled = true

	resp := &v1.DispatchLookupResources3Response{
		Resource: &v1.PossibleResource{
			ResourceId:    "resource1",
			ForSubjectIds: []string{"subject1"},
		},
		Metadata: &v1.ResponseMeta{
			DispatchCount: 1,
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

	rm := newRelationshipsChunk(1)
	rel := tuple.MustParse("document:doc1#viewer@user:alice")
	rm.addRelationship(rel, nil)

	metadata := &v1.ResponseMeta{}

	stream := newYieldingStream(ctx, yieldFunc, rm, metadata)

	resp := &v1.DispatchLookupResources3Response{
		Resource: &v1.PossibleResource{
			ResourceId:    "resource1",
			ForSubjectIds: []string{"doc1"},
		},
		Metadata: &v1.ResponseMeta{
			DispatchCount: 1,
		},
		AfterResponseCursor: &v1.Cursor{
			DispatchVersion: lr3DispatchVersion,
			Sections:        []string{"section1"},
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
	rm := newRelationshipsChunk(0)
	metadata := &v1.ResponseMeta{}

	stream := newYieldingStream(ctx, yieldFunc, rm, metadata)

	resp := &v1.DispatchLookupResources3Response{
		Resource: &v1.PossibleResource{
			ResourceId:    "resource1",
			ForSubjectIds: []string{"subject1"},
		},
		Metadata: &v1.ResponseMeta{
			DispatchCount: 1,
		},
	}

	err := stream.Publish(resp)

	require.Error(t, err)
	require.Equal(t, context.Canceled, err)
}

func TestYieldingStreamPublishMetadataCombination(t *testing.T) {
	ctx := t.Context()
	var yieldedResult result

	yieldFunc := func(r result, e error) bool {
		yieldedResult = r
		return true
	}

	rm := newRelationshipsChunk(1)
	rel := tuple.MustParse("document:doc1#viewer@user:alice")
	rm.addRelationship(rel, nil)

	initialMetadata := &v1.ResponseMeta{
		DispatchCount: 1,
	}

	stream := newYieldingStream(ctx, yieldFunc, rm, initialMetadata)

	resp := &v1.DispatchLookupResources3Response{
		Resource: &v1.PossibleResource{
			ResourceId:    "resource1",
			ForSubjectIds: []string{"doc1"},
		},
		Metadata: &v1.ResponseMeta{
			DispatchCount: 2,
		},
		AfterResponseCursor: &v1.Cursor{
			DispatchVersion: lr3DispatchVersion,
			Sections:        []string{"section1"},
		},
	}

	err := stream.Publish(resp)
	require.NoError(t, err)

	// For the first publish call, metadata should be combined
	require.NotNil(t, yieldedResult.Item.metadata)
	require.Equal(t, uint32(3), yieldedResult.Item.metadata.DispatchCount) // 1 + 2

	// Reset for second publish
	yieldedResult = result{}

	// Second publish should not combine metadata
	err = stream.Publish(resp)
	require.NoError(t, err)

	require.NotNil(t, yieldedResult.Item.metadata)
	require.Equal(t, uint32(2), yieldedResult.Item.metadata.DispatchCount) // Only response metadata
}
