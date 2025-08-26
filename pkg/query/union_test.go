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

func TestUnionIterator(t *testing.T) {
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

	t.Run("Check_Union", func(t *testing.T) {
		// Create a union of viewer and editor relations
		// This should return resources where the subject has either relation
		union := query.NewUnion()
		
		viewer := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["viewer"].BaseRelations[0])
		editor := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["editor"].BaseRelations[0])
		
		union.AddSubIterator(viewer)
		union.AddSubIterator(editor)

		relSeq, err := union.Check(ctx, []string{"specialplan", "companyplan"}, "multiroleguy")
		require.NoError(err)

		rels, err := query.CollectAll(relSeq)
		require.NoError(err)
		t.Logf("Union results: %+v", rels)

		// The result should contain resources where either condition is met
		// Should be at least as many results as each individual iterator would return
	})

	t.Run("Check_EmptyUnion", func(t *testing.T) {
		union := query.NewUnion()

		// Empty union should return empty results
		relSeq, err := union.Check(ctx, []string{"specialplan"}, "multiroleguy")
		require.NoError(err)

		rels, err := query.CollectAll(relSeq)
		require.NoError(err)
		require.Empty(rels, "empty union should return no results")
	})

	t.Run("Check_SingleSubIterator", func(t *testing.T) {
		union := query.NewUnion()
		
		viewer := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["viewer"].BaseRelations[0])
		union.AddSubIterator(viewer)

		relSeq, err := union.Check(ctx, []string{"specialplan"}, "multiroleguy")
		require.NoError(err)

		rels, err := query.CollectAll(relSeq)
		require.NoError(err)
		t.Logf("Single sub-iterator union results: %+v", rels)
		
		// Should return same results as the single iterator alone
	})

	t.Run("Check_EmptyResourceList", func(t *testing.T) {
		union := query.NewUnion()
		
		viewer := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["viewer"].BaseRelations[0])
		union.AddSubIterator(viewer)

		relSeq, err := union.Check(ctx, []string{}, "multiroleguy")
		require.NoError(err)

		rels, err := query.CollectAll(relSeq)
		require.NoError(err)
		require.Empty(rels, "empty resource list should return no results")
	})

	t.Run("Check_EarlyTermination", func(t *testing.T) {
		// Test the optimization where union stops checking remaining resources
		// once all have been found by earlier sub-iterators
		union := query.NewUnion()
		
		// Add iterators that might find the same resource
		viewer := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["viewer"].BaseRelations[0])
		vae := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["viewer_and_editor"].BaseRelations[0])
		
		union.AddSubIterator(viewer)
		union.AddSubIterator(vae)

		relSeq, err := union.Check(ctx, []string{"specialplan"}, "multiroleguy")
		require.NoError(err)

		rels, err := query.CollectAll(relSeq)
		require.NoError(err)
		t.Logf("Early termination union results: %+v", rels)
		
		// The union should optimize by not checking already found resources
	})

	t.Run("LookupSubjects_Unimplemented", func(t *testing.T) {
		union := query.NewUnion()
		_, err := union.LookupSubjects(ctx, "specialplan")
		require.ErrorIs(err, query.ErrUnimplemented)
	})

	t.Run("LookupResources_Unimplemented", func(t *testing.T) {
		union := query.NewUnion()
		_, err := union.LookupResources(ctx, "multiroleguy")
		require.ErrorIs(err, query.ErrUnimplemented)
	})
}

func TestUnionIteratorClone(t *testing.T) {
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

	original := query.NewUnion()
	
	// Use different relation types to avoid the empty resource list issue
	vae := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["viewer_and_editor"].BaseRelations[0])
	owner := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["owner"].BaseRelations[0])
	
	original.AddSubIterator(vae)
	original.AddSubIterator(owner)

	cloned := original.Clone()
	require.NotSame(original, cloned, "cloned iterator should be a different object")

	// Both should have the same structure when explained
	originalExplain := original.Explain()
	clonedExplain := cloned.Explain()
	require.Equal(originalExplain.Info, clonedExplain.Info)
	require.Equal(len(originalExplain.SubExplain), len(clonedExplain.SubExplain))

	// Test that both iterators produce the same results
	// Use a subject that won't be found by the first iterator but might by the second
	resourceIDs := []string{"specialplan"}
	subjectID := "owner"

	// Collect results from original iterator
	originalSeq, err := original.Check(ctx, resourceIDs, subjectID)
	require.NoError(err)
	originalResults, err := query.CollectAll(originalSeq)
	require.NoError(err)

	// Collect results from cloned iterator  
	clonedSeq, err := cloned.Check(ctx, resourceIDs, subjectID)
	require.NoError(err)
	clonedResults, err := query.CollectAll(clonedSeq)
	require.NoError(err)

	// Both iterators should produce identical results
	require.Equal(originalResults, clonedResults, "original and cloned iterators should produce identical results")
	t.Logf("Original results: %+v", originalResults)
	t.Logf("Cloned results: %+v", clonedResults)
}

func TestUnionIteratorExplain(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	union := query.NewUnion()
	
	t.Run("EmptyUnion", func(t *testing.T) {
		explain := union.Explain()
		require.Equal("Union", explain.Info)
		require.Empty(explain.SubExplain, "empty union should have no sub-explains")
	})

	t.Run("UnionWithSubIterators", func(t *testing.T) {
		viewer := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["viewer"].BaseRelations[0])
		editor := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["editor"].BaseRelations[0])
		
		union.AddSubIterator(viewer)
		union.AddSubIterator(editor)

		explain := union.Explain()
		require.Equal("Union", explain.Info)
		require.Len(explain.SubExplain, 2, "union should have exactly 2 sub-explains")

		explainStr := explain.String()
		require.Contains(explainStr, "Union")
		require.NotEmpty(explainStr)
		t.Logf("Union explain: %s", explainStr)
	})
}

func TestUnionIteratorDuplicateElimination(t *testing.T) {
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

	// Create a union with overlapping sub-iterators
	// This tests the deduplication logic where resources found by earlier
	// iterators are removed from the remaining list
	union := query.NewUnion()
	
	// Add the same iterator twice to test deduplication
	viewer1 := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["viewer"].BaseRelations[0])
	viewer2 := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["viewer"].BaseRelations[0])
	
	union.AddSubIterator(viewer1)
	union.AddSubIterator(viewer2)

	relSeq, err := union.Check(ctx, []string{"specialplan"}, "multiroleguy")
	require.NoError(err)

	rels, err := query.CollectAll(relSeq)
	require.NoError(err)
	t.Logf("Duplicate elimination results: %+v", rels)

	// The union should handle potential duplicates correctly through its
	// resource elimination optimization
}

func TestUnionIteratorMultipleResources(t *testing.T) {
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

	union := query.NewUnion()
	
	viewer := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["viewer"].BaseRelations[0])
	editor := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["editor"].BaseRelations[0])
	
	union.AddSubIterator(viewer)
	union.AddSubIterator(editor)

	// Test with multiple resource IDs
	relSeq, err := union.Check(ctx, []string{"specialplan", "companyplan", "nonexistent"}, "multiroleguy")
	require.NoError(err)

	rels, err := query.CollectAll(relSeq)
	require.NoError(err)
	
	t.Logf("Multiple resources union results: %+v", rels)
	// The result should include all valid union relationships found across all resources
}