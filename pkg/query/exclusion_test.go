package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestExclusionIterator(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
		Reader:   ds.SnapshotReader(revision),
	}

	// Create test paths
	path1 := MustPathFromString("document:doc1#viewer@user:alice")
	path2 := MustPathFromString("document:doc2#viewer@user:bob")
	path3 := MustPathFromString("document:doc3#viewer@user:charlie")

	t.Run("Basic Exclusion", func(t *testing.T) {
		t.Parallel()
		// Main set: path1, path2, path3
		// Excluded set: path2
		// Expected result: path1, path3
		mainSet := NewFixedIterator(path1, path2, path3)
		excludedSet := NewFixedIterator(path2)

		exclusion := NewExclusion(mainSet, excludedSet)

		pathSeq, err := ctx.Check(exclusion, NewObjects("document", "doc1", "doc2", "doc3"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Len(rels, 1, "Should exclude doc2#viewer@user:bob but keep doc1#viewer@user:alice")
		require.Equal(path1, rels[0])
	})

	t.Run("Empty Main Set", func(t *testing.T) {
		t.Parallel()
		// Main set: empty
		// Excluded set: path1
		// Expected result: empty
		mainSet := NewFixedIterator()
		excludedSet := NewFixedIterator(path1)

		exclusion := NewExclusion(mainSet, excludedSet)

		pathSeq, err := ctx.Check(exclusion, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Empty(rels, "Empty main set should result in empty exclusion")
	})

	t.Run("Empty Excluded Set", func(t *testing.T) {
		t.Parallel()
		// Main set: path1, path2
		// Excluded set: empty
		// Expected result: path1, path2 (nothing to exclude)
		mainSet := NewFixedIterator(path1, path2)
		excludedSet := NewFixedIterator()

		exclusion := NewExclusion(mainSet, excludedSet)

		pathSeq, err := ctx.Check(exclusion, NewObjects("document", "doc1", "doc2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Len(rels, 1, "Should return main set when nothing to exclude")
		require.Equal(path1, rels[0])
	})

	t.Run("No Overlap", func(t *testing.T) {
		t.Parallel()
		// Create relations with truly different endpoints
		mainPath1 := MustPathFromString("document:doc1#viewer@user:alice")
		mainPath2 := MustPathFromString("document:doc2#viewer@user:bob")
		excludePath1 := MustPathFromString("document:doc3#viewer@user:charlie")
		excludePath2 := MustPathFromString("document:doc4#viewer@user:dave")

		// Main set: doc1:alice, doc2:bob
		// Excluded set: doc3:charlie, doc4:dave
		// Expected result: both main relations (no endpoint overlap)
		mainSet := NewFixedIterator(mainPath1, mainPath2)
		excludedSet := NewFixedIterator(excludePath1, excludePath2)

		exclusion := NewExclusion(mainSet, excludedSet)

		pathSeq, err := ctx.Check(exclusion, NewObjects("document", "doc1", "doc2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Len(rels, 1, "Should return main relations matching subject when no endpoint overlap")
		require.Equal(mainPath1, rels[0]) // Only alice matches the subject filter
	})

	t.Run("Complete Exclusion", func(t *testing.T) {
		t.Parallel()
		// Main set: path1, path2
		// Excluded set: path1, path2, path3
		// Expected result: empty (all main set relations are excluded)
		mainSet := NewFixedIterator(path1, path2)
		excludedSet := NewFixedIterator(path1, path2, path3)

		exclusion := NewExclusion(mainSet, excludedSet)

		pathSeq, err := ctx.Check(exclusion, NewObjects("document", "doc1", "doc2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Empty(rels, "Should return empty when all main relations are excluded")
	})

	t.Run("Partial Exclusion", func(t *testing.T) {
		t.Parallel()
		// Create relations with different relations but same endpoints to test exclusion logic
		pathA := MustPathFromString("document:doc1#viewer@user:alice")
		pathB := MustPathFromString("document:doc2#viewer@user:alice")
		pathC := MustPathFromString("document:doc3#viewer@user:alice")
		pathD := MustPathFromString("document:doc4#viewer@user:alice")

		// Create excluded relations with different relations but same endpoints as pathB and pathD
		excludePathB := MustPathFromString("document:doc2#editor@user:alice") // Same endpoint as pathB
		excludePathD := MustPathFromString("document:doc4#owner@user:alice")  // Same endpoint as pathD

		// Main set: pathA, pathB, pathC, pathD
		// Excluded set: excludePathB (matches doc2:alice), excludePathD (matches doc4:alice)
		// Expected result: pathA, pathC (pathB and pathD excluded due to matching endpoints)
		mainSet := NewFixedIterator(pathA, pathB, pathC, pathD)
		excludedSet := NewFixedIterator(excludePathB, excludePathD)

		exclusion := NewExclusion(mainSet, excludedSet)

		pathSeq, err := ctx.Check(exclusion, NewObjects("document", "doc1", "doc2", "doc3", "doc4"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Len(rels, 2, "Should exclude relations with endpoints matching excluded set")

		// Check that we got the expected relations
		foundRelA := false
		foundRelC := false
		for _, rel := range rels {
			if rel.Resource.ObjectID == "doc1" {
				foundRelA = true
			}
			if rel.Resource.ObjectID == "doc3" {
				foundRelC = true
			}
		}
		require.True(foundRelA, "Should contain pathA (doc1)")
		require.True(foundRelC, "Should contain pathC (doc3)")
	})

	t.Run("Clone", func(t *testing.T) {
		t.Parallel()
		mainSet := NewFixedIterator(path1, path2)
		excludedSet := NewFixedIterator(path2)

		original := NewExclusion(mainSet, excludedSet)
		cloned := original.Clone()

		require.NotNil(cloned)
		require.IsType(&Exclusion{}, cloned)

		// Test that cloned exclusion works the same as original
		pathSeq, err := ctx.Check(cloned, NewObjects("document", "doc1", "doc2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Len(rels, 1, "Cloned exclusion should work the same as original")
		require.Equal(path1, rels[0])
	})

	t.Run("Explain", func(t *testing.T) {
		t.Parallel()
		mainSet := NewFixedIterator(path1)
		excludedSet := NewFixedIterator(path2)

		exclusion := NewExclusion(mainSet, excludedSet)

		explain := exclusion.Explain()
		require.Equal("Exclusion", explain.Info)
		require.Len(explain.SubExplain, 2, "Should have two sub-explanations")

		explainStr := explain.String()
		require.Contains(explainStr, "Exclusion")
		require.Contains(explainStr, "Fixed")
	})
}

func TestExclusionWithEmptyIterator(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
		Reader:   ds.SnapshotReader(revision),
	}

	path1 := MustPathFromString("document:doc1#viewer@user:alice")

	t.Run("Empty as Main Set", func(t *testing.T) {
		t.Parallel()
		mainSet := NewEmptyFixedIterator()
		excludedSet := NewFixedIterator(path1)

		exclusion := NewExclusion(mainSet, excludedSet)

		pathSeq, err := ctx.Check(exclusion, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Empty(rels, "Empty main set should result in empty exclusion")
	})

	t.Run("Empty as Excluded Set", func(t *testing.T) {
		t.Parallel()
		mainSet := NewFixedIterator(path1)
		excludedSet := NewEmptyFixedIterator()

		exclusion := NewExclusion(mainSet, excludedSet)

		pathSeq, err := ctx.Check(exclusion, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Len(rels, 1, "Should return main set when excluded set is empty")
		require.Equal(path1, rels[0])
	})
}

func TestExclusionUnimplementedMethods(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
		Reader:   ds.SnapshotReader(revision),
	}

	path1 := MustPathFromString("document:doc1#viewer@user:alice")
	mainSet := NewFixedIterator(path1)
	excludedSet := NewFixedIterator()

	exclusion := NewExclusion(mainSet, excludedSet)

	t.Run("IterSubjectsImpl Unimplemented", func(t *testing.T) {
		t.Parallel()
		require.Panics(func() {
			_, _ = ctx.IterSubjects(exclusion, NewObject("document", "doc1"))
		}, "Should panic since method is unimplemented")
	})

	t.Run("IterResourcesImpl Unimplemented", func(t *testing.T) {
		t.Parallel()
		require.Panics(func() {
			_, _ = ctx.IterResources(exclusion, NewObject("user", "alice").WithEllipses())
		}, "Should panic since method is unimplemented")
	})
}

func TestExclusionErrorHandling(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
		Reader:   ds.SnapshotReader(revision),
	}

	path1 := MustPathFromString("document:doc1#viewer@user:alice")

	t.Run("Main Set Error Propagation", func(t *testing.T) {
		t.Parallel()
		// Create a faulty iterator for the main set
		mainSet := NewFaultyIterator(true, false)
		excludedSet := NewFixedIterator(path1)

		exclusion := NewExclusion(mainSet, excludedSet)

		pathSeq, err := ctx.Check(exclusion, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.Error(err)
		require.Contains(err.Error(), "faulty iterator error")
		require.Nil(pathSeq)
	})

	t.Run("Excluded Set Error Propagation", func(t *testing.T) {
		t.Parallel()
		// Create a normal main set and faulty excluded set
		mainSet := NewFixedIterator(path1)
		excludedSet := NewFaultyIterator(true, false)

		exclusion := NewExclusion(mainSet, excludedSet)

		pathSeq, err := ctx.Check(exclusion, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.Error(err)
		require.Contains(err.Error(), "faulty iterator error")
		require.Nil(pathSeq)
	})

	t.Run("Main Set Collection Error", func(t *testing.T) {
		t.Parallel()
		// Create an iterator that fails during collection
		mainSet := NewFaultyIterator(false, true)
		excludedSet := NewFixedIterator(path1)

		exclusion := NewExclusion(mainSet, excludedSet)

		pathSeq, err := ctx.Check(exclusion, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.Error(err)
		require.Contains(err.Error(), "faulty iterator collection error")
		require.Nil(pathSeq)
	})

	t.Run("Excluded Set Collection Error", func(t *testing.T) {
		t.Parallel()
		// Create an iterator that fails during collection
		mainSet := NewFixedIterator(path1)
		excludedSet := NewFaultyIterator(false, true)

		exclusion := NewExclusion(mainSet, excludedSet)

		pathSeq, err := ctx.Check(exclusion, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.Error(err)
		require.Contains(err.Error(), "faulty iterator collection error")
		require.Nil(pathSeq)
	})
}

func TestExclusionWithComplexIteratorTypes(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
		Reader:   ds.SnapshotReader(revision),
	}

	// Create test relations
	path1 := MustPathFromString("document:doc1#viewer@user:alice")
	path2 := MustPathFromString("document:doc2#viewer@user:alice")
	path3 := MustPathFromString("document:doc3#viewer@user:alice")
	path4 := MustPathFromString("document:doc4#viewer@user:alice")

	t.Run("Exclusion with Union as Main Set", func(t *testing.T) {
		t.Parallel()
		// Create union iterator as main set
		union := NewUnion()
		union.addSubIterator(NewFixedIterator(path1, path2))
		union.addSubIterator(NewFixedIterator(path3))

		excludedSet := NewFixedIterator(path2) // Exclude path2

		exclusion := NewExclusion(union, excludedSet)

		pathSeq, err := ctx.Check(exclusion, NewObjects("document", "doc1", "doc2", "doc3"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Len(rels, 2, "Should return path1 and path3, excluding path2")

		// Check which relations we got
		foundRel1, foundRel3 := false, false
		for _, rel := range rels {
			if rel.Resource.ObjectID == "doc1" {
				foundRel1 = true
			}
			if rel.Resource.ObjectID == "doc3" {
				foundRel3 = true
			}
		}
		require.True(foundRel1, "Should contain path1")
		require.True(foundRel3, "Should contain path3")
	})

	t.Run("Exclusion with Union as Excluded Set", func(t *testing.T) {
		t.Parallel()
		mainSet := NewFixedIterator(path1, path2, path3, path4)

		// Create union iterator as excluded set
		union := NewUnion()
		union.addSubIterator(NewFixedIterator(path2))
		union.addSubIterator(NewFixedIterator(path4))

		exclusion := NewExclusion(mainSet, union)

		pathSeq, err := ctx.Check(exclusion, NewObjects("document", "doc1", "doc2", "doc3", "doc4"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Len(rels, 2, "Should return path1 and path3, excluding path2 and path4")

		// Check which relations we got
		foundRel1, foundRel3 := false, false
		for _, rel := range rels {
			if rel.Resource.ObjectID == "doc1" {
				foundRel1 = true
			}
			if rel.Resource.ObjectID == "doc3" {
				foundRel3 = true
			}
		}
		require.True(foundRel1, "Should contain path1")
		require.True(foundRel3, "Should contain path3")
	})

	t.Run("Nested Exclusion", func(t *testing.T) {
		t.Parallel()
		// Create a nested exclusion: (path1 + path2 + path3) - path2 - path3
		innerMainSet := NewFixedIterator(path1, path2, path3)
		innerExcludedSet := NewFixedIterator(path2)
		innerExclusion := NewExclusion(innerMainSet, innerExcludedSet)

		outerExcludedSet := NewFixedIterator(path3)
		outerExclusion := NewExclusion(innerExclusion, outerExcludedSet)

		pathSeq, err := ctx.Check(outerExclusion, NewObjects("document", "doc1", "doc2", "doc3"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Len(rels, 1, "Should return only path1 after nested exclusions")
		require.Equal("doc1", rels[0].Resource.ObjectID)
	})
}

// Additional comprehensive tests for uncovered functions in exclusion.go

func TestCombineExclusionCaveats(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	// Helper function to create paths with caveats
	createPathWithCaveat := func(relation, caveatName string) Path {
		path := MustPathFromString(relation)
		if caveatName != "" {
			path.Caveat = &core.CaveatExpression{
				OperationOrCaveat: &core.CaveatExpression_Caveat{
					Caveat: &core.ContextualizedCaveat{CaveatName: caveatName},
				},
			}
		}
		return path
	}

	// Test cases for different caveat combinations
	t.Run("main_with_caveat_excluded_without_caveat", func(t *testing.T) {
		t.Parallel()
		mainPath := createPathWithCaveat("document:doc1#view@user:alice", "main_caveat")
		excludedPath := createPathWithCaveat("document:doc1#view@user:alice", "")

		result, shouldInclude := combineExclusionCaveats(mainPath, excludedPath)

		// Main has caveat, excluded has no caveat -> completely excluded
		require.False(shouldInclude, "Should be completely excluded when main has caveat but excluded has none")
		require.Equal(Path{}, result, "Result should be empty path when excluded")
	})

	t.Run("main_without_caveat_excluded_without_caveat", func(t *testing.T) {
		t.Parallel()
		mainPath := createPathWithCaveat("document:doc1#view@user:alice", "")
		excludedPath := createPathWithCaveat("document:doc1#view@user:alice", "")

		result, shouldInclude := combineExclusionCaveats(mainPath, excludedPath)

		// Neither has caveat -> completely excluded
		require.False(shouldInclude, "Should be completely excluded when neither has caveat")
		require.Equal(Path{}, result, "Result should be empty path when excluded")
	})

	t.Run("main_without_caveat_excluded_with_caveat", func(t *testing.T) {
		t.Parallel()
		mainPath := createPathWithCaveat("document:doc1#view@user:alice", "")
		excludedPath := createPathWithCaveat("document:doc1#view@user:alice", "excluded_caveat")

		result, shouldInclude := combineExclusionCaveats(mainPath, excludedPath)

		// Main has no caveat, excluded has caveat -> return main with negated excluded caveat
		require.True(shouldInclude, "Should be included when main has no caveat but excluded has one")
		require.NotNil(result.Caveat, "Result should have a caveat (negated excluded caveat)")

		// Verify the result has the same endpoints as main
		require.Equal(mainPath.Resource, result.Resource)
		require.Equal(mainPath.Relation, result.Relation)
		require.Equal(mainPath.Subject, result.Subject)
	})

	t.Run("both_have_caveats", func(t *testing.T) {
		t.Parallel()
		mainPath := createPathWithCaveat("document:doc1#view@user:alice", "main_caveat")
		excludedPath := createPathWithCaveat("document:doc1#view@user:alice", "excluded_caveat")

		result, shouldInclude := combineExclusionCaveats(mainPath, excludedPath)

		// Both have caveats -> return main with combined caveat (main AND NOT excluded)
		require.True(shouldInclude, "Should be included when both have caveats")
		resultCaveat := result.Caveat
		require.NotNil(resultCaveat, "Result should have a combined caveat")
		require.NotNil(resultCaveat.GetOperation(), "Caveat should be an operation")
		require.Equal(resultCaveat.GetOperation().Op, core.CaveatOperation_AND, "Caveat should be an AND")
		require.Len(resultCaveat.GetOperation().GetChildren(), 2, "Caveat should be an AND of two children (main_caveat AND NOT excluded_caveat)")

		// Verify the result has the same endpoints as main
		require.Equal(mainPath.Resource, result.Resource)
		require.Equal(mainPath.Relation, result.Relation)
		require.Equal(mainPath.Subject, result.Subject)
	})
}

func TestExclusion_CombinedCaveatLogic(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	// Create test datastore and context
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
		Reader:   ds.SnapshotReader(revision),
	}

	// Helper to create paths with caveats
	createPathWithCaveat := func(relation, caveatName string) Path {
		path := MustPathFromString(relation)
		if caveatName != "" {
			path.Caveat = &core.CaveatExpression{
				OperationOrCaveat: &core.CaveatExpression_Caveat{
					Caveat: &core.ContextualizedCaveat{CaveatName: caveatName},
				},
			}
		}
		return path
	}

	t.Run("exclusion_with_caveats", func(t *testing.T) {
		t.Parallel()
		// Main set has paths with various caveats
		mainPath1 := createPathWithCaveat("document:doc1#view@user:alice", "caveat1")
		mainPath2 := createPathWithCaveat("document:doc2#view@user:alice", "") // No caveat
		mainSet := NewFixedIterator(mainPath1, mainPath2)

		// Excluded set has paths that should modify caveats
		excludedPath1 := createPathWithCaveat("document:doc1#view@user:alice", "caveat2")
		excludedSet := NewFixedIterator(excludedPath1)

		exclusion := NewExclusion(mainSet, excludedSet)

		pathSeq, err := ctx.Check(exclusion, NewObjects("document", "doc1", "doc2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		results, err := CollectAll(pathSeq)
		require.NoError(err)

		// We expect:
		// - doc1: main has caveat1, excluded has caveat2 -> combined caveat (caveat1 AND NOT caveat2)
		// - doc2: main has no caveat, excluded doesn't match -> keep as is
		require.Len(results, 2, "Should return both paths with appropriate caveat modifications")

		// Find the doc1 result
		var doc1Result Path
		var doc2Result Path
		var foundDoc1, foundDoc2 bool
		for _, result := range results {
			switch result.Resource.ObjectID {
			case "doc1":
				doc1Result = result
				foundDoc1 = true
			case "doc2":
				doc2Result = result
				foundDoc2 = true
			}
		}

		require.True(foundDoc1, "Should have result for doc1")
		require.True(foundDoc2, "Should have result for doc2")

		// doc1 should have combined caveat
		doc1Caveat := doc1Result.Caveat
		require.NotNil(doc1Caveat, "doc1 result should have combined caveat")
		require.NotNil(doc1Caveat.GetOperation(), "Caveat should be an operation")
		require.Equal(doc1Caveat.GetOperation().Op, core.CaveatOperation_AND, "Caveat should be an AND")
		require.Len(doc1Caveat.GetOperation().GetChildren(), 2, "Caveat should be an AND of two children (caveat1 AND NOT caveat2)")

		// doc2 should have no caveat (original state preserved)
		require.Nil(doc2Result.Caveat, "doc2 result should have no caveat")
	})
}

func TestExclusion_EdgeCases(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	// Create minimal context for basic testing
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	t.Run("empty_main_set", func(t *testing.T) {
		t.Parallel()
		mainSet := NewFixedIterator() // Empty
		excludedSet := NewFixedIterator(MustPathFromString("document:doc1#view@user:alice"))

		exclusion := NewExclusion(mainSet, excludedSet)

		pathSeq, err := ctx.Check(exclusion, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		results, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Empty(results, "Empty main set should result in empty output")
	})

	t.Run("empty_excluded_set", func(t *testing.T) {
		t.Parallel()
		mainPath := MustPathFromString("document:doc1#view@user:alice")
		mainSet := NewFixedIterator(mainPath)
		excludedSet := NewFixedIterator() // Empty

		exclusion := NewExclusion(mainSet, excludedSet)

		pathSeq, err := ctx.Check(exclusion, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		results, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Len(results, 1, "Should return main set when excluded set is empty")
		require.Equal(mainPath, results[0])
	})

	t.Run("no_matching_exclusions", func(t *testing.T) {
		t.Parallel()
		mainPath := MustPathFromString("document:doc1#view@user:alice")
		mainSet := NewFixedIterator(mainPath)

		// Excluded set has different resource/subject, so no matches
		excludedPath := MustPathFromString("document:doc2#view@user:bob")
		excludedSet := NewFixedIterator(excludedPath)

		exclusion := NewExclusion(mainSet, excludedSet)

		pathSeq, err := ctx.Check(exclusion, NewObjects("document", "doc1", "doc2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		results, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Len(results, 1, "Should return main path when no exclusions match")
		require.Equal(mainPath.Resource, results[0].Resource)
		require.Equal(mainPath.Subject, results[0].Subject)
	})
}
