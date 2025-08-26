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

func TestArrowIterator(t *testing.T) {
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

	// Create an arrow iterator: document.parent -> folder.viewer
	// This simulates permission parent_folder_viewer = parent->viewer
	left := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["parent"].BaseRelations[0])
	right := query.NewRelationIterator(dsSchema.Definitions["folder"].Relations["viewer"].BaseRelations[0])
	arrow := query.NewArrow(left, right)

	t.Run("Check", func(t *testing.T) {
		relSeq, err := arrow.Check(ctx, []string{"companyplan"}, "legal")
		require.NoError(err)

		rels, err := query.CollectAll(relSeq)
		require.NoError(err)

		// Should find relations where:
		// 1. companyplan has parent relationship to some folder
		// 2. That folder has legal as viewer
		// Based on test fixtures, this should return results
		t.Logf("Arrow Check results: %+v", rels)
	})

	t.Run("Check_EmptyResources", func(t *testing.T) {
		relSeq, err := arrow.Check(ctx, []string{}, "legal")
		require.NoError(err)

		rels, err := query.CollectAll(relSeq)
		require.NoError(err)
		require.Empty(rels, "empty resource list should return no results")
	})

	t.Run("Check_NonexistentResource", func(t *testing.T) {
		relSeq, err := arrow.Check(ctx, []string{"nonexistent"}, "legal")
		require.NoError(err)

		rels, err := query.CollectAll(relSeq)
		require.NoError(err)
		// May be empty or not depending on test data, but should not error
		t.Logf("Nonexistent resource results: %+v", rels)
	})

	t.Run("LookupSubjects_Unimplemented", func(t *testing.T) {
		_, err := arrow.LookupSubjects(ctx, "companyplan")
		require.ErrorIs(err, query.ErrUnimplemented)
	})

	t.Run("LookupResources_Unimplemented", func(t *testing.T) {
		_, err := arrow.LookupResources(ctx, "legal")
		require.ErrorIs(err, query.ErrUnimplemented)
	})
}

func TestArrowIteratorClone(t *testing.T) {
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

	left := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["parent"].BaseRelations[0])
	right := query.NewRelationIterator(dsSchema.Definitions["folder"].Relations["viewer"].BaseRelations[0])
	original := query.NewArrow(left, right)

	cloned := original.Clone()
	require.NotSame(original, cloned, "cloned iterator should be a different object")

	// Both should have the same structure when explained
	originalExplain := original.Explain()
	clonedExplain := cloned.Explain()
	require.Equal(originalExplain.Info, clonedExplain.Info)
	require.Equal(len(originalExplain.SubExplain), len(clonedExplain.SubExplain))

	// Test that both iterators produce the same results
	resourceIDs := []string{"companyplan"}
	subjectID := "legal"

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

func TestArrowIteratorExplain(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	left := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["parent"].BaseRelations[0])
	right := query.NewRelationIterator(dsSchema.Definitions["folder"].Relations["viewer"].BaseRelations[0])
	arrow := query.NewArrow(left, right)

	explain := arrow.Explain()
	require.Equal("Arrow", explain.Info)
	require.Len(explain.SubExplain, 2, "arrow should have exactly 2 sub-explains (left and right)")

	explainStr := explain.String()
	require.Contains(explainStr, "Arrow")
	require.NotEmpty(explainStr)
	t.Logf("Arrow explain: %s", explainStr)
}

func TestArrowIteratorMultipleResources(t *testing.T) {
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

	left := query.NewRelationIterator(dsSchema.Definitions["document"].Relations["parent"].BaseRelations[0])
	right := query.NewRelationIterator(dsSchema.Definitions["folder"].Relations["viewer"].BaseRelations[0])
	arrow := query.NewArrow(left, right)

	// Test with multiple resource IDs
	relSeq, err := arrow.Check(ctx, []string{"companyplan", "specialplan", "nonexistent"}, "legal")
	require.NoError(err)

	rels, err := query.CollectAll(relSeq)
	require.NoError(err)
	
	t.Logf("Multiple resources results: %+v", rels)
	// The result should include all valid arrow relationships found across all resources
}