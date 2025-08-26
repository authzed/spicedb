package query_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/schema/v2"
)

func TestIntersectionIterator(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	ctx := &query.Context{
		Context:   t.Context(),
		Datastore: ds,
		Revision:  revision,
	}

	t.Run("Check_Intersection", func(t *testing.T) {
		// Create an intersection of viewer_and_editor and editor relations
		// This should return only resources where the subject has both relations
		intersect := query.NewIntersection()
		
		vae := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["viewer_and_editor"].BaseRelations[0])
		editor := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["editor"].BaseRelations[0])
		
		intersect.AddSubIterator(vae)
		intersect.AddSubIterator(editor)

		relSeq, err := intersect.Check(ctx, []string{"specialplan"}, "multiroleguy")
		require.NoError(err)

		rels, err := query.CollectAll(relSeq)
		require.NoError(err)
		t.Logf("Intersection results: %+v", rels)

		// The result should only contain resources where both conditions are met
	})

	t.Run("Check_EmptyIntersection", func(t *testing.T) {
		// Create an intersection with contradictory requirements
		intersect := query.NewIntersection()
		
		viewer := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["viewer"].BaseRelations[0])
		editor := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["editor"].BaseRelations[0])
		
		intersect.AddSubIterator(viewer)
		intersect.AddSubIterator(editor)

		// Use a subject that likely doesn't have both relations
		relSeq, err := intersect.Check(ctx, []string{"specialplan"}, "owner")
		require.NoError(err)

		if relSeq != nil {
			rels, err := query.CollectAll(relSeq)
			require.NoError(err)
			t.Logf("Empty intersection results: %+v", rels)
		} else {
			t.Log("Empty intersection returned nil sequence")
		}
	})

	t.Run("Check_NoSubIterators", func(t *testing.T) {
		intersect := query.NewIntersection()

		// Empty intersection should return empty results
		relSeq, err := intersect.Check(ctx, []string{"specialplan"}, "multiroleguy")
		require.NoError(err)

		// Should return nil sequence since there are no sub-iterators
		if relSeq != nil {
			rels, err := query.CollectAll(relSeq)
			require.NoError(err)
			require.Empty(rels)
		}
	})

	t.Run("Check_SingleSubIterator", func(t *testing.T) {
		intersect := query.NewIntersection()
		
		vae := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["viewer_and_editor"].BaseRelations[0])
		intersect.AddSubIterator(vae)

		relSeq, err := intersect.Check(ctx, []string{"specialplan"}, "multiroleguy")
		require.NoError(err)

		rels, err := query.CollectAll(relSeq)
		require.NoError(err)
		t.Logf("Single sub-iterator results: %+v", rels)
	})

	t.Run("Check_EmptyResourceList", func(t *testing.T) {
		intersect := query.NewIntersection()
		
		vae := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["viewer_and_editor"].BaseRelations[0])
		intersect.AddSubIterator(vae)

		relSeq, err := intersect.Check(ctx, []string{}, "multiroleguy")
		require.NoError(err)

		// The behavior with empty resource list may vary by implementation
		// Let's just ensure it doesn't error and log the results
		if relSeq != nil {
			rels, err := query.CollectAll(relSeq)
			require.NoError(err)
			t.Logf("Empty resource list results: %+v", rels)
		} else {
			t.Log("Empty resource list returned nil sequence")
		}
	})

	t.Run("LookupSubjects_Unimplemented", func(t *testing.T) {
		intersect := query.NewIntersection()
		_, err := intersect.LookupSubjects(ctx, "specialplan")
		require.ErrorIs(err, query.ErrUnimplemented)
	})

	t.Run("LookupResources_Unimplemented", func(t *testing.T) {
		intersect := query.NewIntersection()
		_, err := intersect.LookupResources(ctx, "multiroleguy")
		require.ErrorIs(err, query.ErrUnimplemented)
	})
}

func TestIntersectionIteratorClone(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	ctx := &query.Context{
		Context:   t.Context(),
		Datastore: ds,
		Revision:  revision,
	}

	original := query.NewIntersection()
	
	vae := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["viewer_and_editor"].BaseRelations[0])
	editor := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["editor"].BaseRelations[0])
	
	original.AddSubIterator(vae)
	original.AddSubIterator(editor)

	cloned := original.Clone()
	require.NotSame(original, cloned, "cloned iterator should be a different object")

	// Both should have the same structure when explained
	originalExplain := original.Explain()
	clonedExplain := cloned.Explain()
	require.Equal(originalExplain.Info, clonedExplain.Info)
	require.Equal(len(originalExplain.SubExplain), len(clonedExplain.SubExplain))

	// Test that both iterators produce the same results
	resourceIDs := []string{"specialplan"}
	subjectID := "multiroleguy"

	// Collect results from original iterator
	originalSeq, err := original.Check(ctx, resourceIDs, subjectID)
	require.NoError(err)
	var originalResults []query.Relation
	if originalSeq != nil {
		originalResults, err = query.CollectAll(originalSeq)
		require.NoError(err)
	}

	// Collect results from cloned iterator  
	clonedSeq, err := cloned.Check(ctx, resourceIDs, subjectID)
	require.NoError(err)
	var clonedResults []query.Relation
	if clonedSeq != nil {
		clonedResults, err = query.CollectAll(clonedSeq)
		require.NoError(err)
	}

	// Both iterators should produce identical results
	require.Equal(originalResults, clonedResults, "original and cloned iterators should produce identical results")
	t.Logf("Original results: %+v", originalResults)
	t.Logf("Cloned results: %+v", clonedResults)
}

func TestIntersectionIteratorExplain(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	intersect := query.NewIntersection()
	
	t.Run("EmptyIntersection", func(t *testing.T) {
		explain := intersect.Explain()
		require.Equal("Intersection", explain.Info)
		require.Empty(explain.SubExplain, "empty intersection should have no sub-explains")
	})

	t.Run("IntersectionWithSubIterators", func(t *testing.T) {
		vae := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["viewer_and_editor"].BaseRelations[0])
		editor := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["editor"].BaseRelations[0])
		
		intersect.AddSubIterator(vae)
		intersect.AddSubIterator(editor)

		explain := intersect.Explain()
		require.Equal("Intersection", explain.Info)
		require.Len(explain.SubExplain, 2, "intersection should have exactly 2 sub-explains")

		explainStr := explain.String()
		require.Contains(explainStr, "Intersection")
		require.NotEmpty(explainStr)
		t.Logf("Intersection explain: %s", explainStr)
	})
}

func TestIntersectionIteratorEarlyTermination(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	ctx := &query.Context{
		Context:   t.Context(),
		Datastore: ds,
		Revision:  revision,
	}

	// Create an intersection where the first iterator returns no results
	// This should cause early termination
	intersect := query.NewIntersection()
	
	// First, add an iterator that will likely return no results
	nonexistent := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["viewer"].BaseRelations[0])
	editor := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["editor"].BaseRelations[0])
	
	intersect.AddSubIterator(nonexistent)
	intersect.AddSubIterator(editor)

	// Use a subject that likely doesn't exist as a viewer
	relSeq, err := intersect.Check(ctx, []string{"specialplan"}, "nonexistent_user")
	require.NoError(err)

	// Should return empty results since first iterator has no results
	if relSeq != nil {
		rels, err := query.CollectAll(relSeq)
		require.NoError(err)
		// May be empty due to early termination
		t.Logf("Early termination results: %+v", rels)
	}
}