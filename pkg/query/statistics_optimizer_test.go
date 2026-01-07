package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestStatisticsOptimizer_ReorderUnion(t *testing.T) {
	t.Parallel()

	stats := DefaultStaticStatistics()
	optimizer := StatisticsOptimizer{Source: stats}

	t.Run("no reordering needed", func(t *testing.T) {
		t.Parallel()
		// Already in optimal order (higher selectivity first)
		// RelationIterator has 0.9, FixedIterator has 0.9
		baseRel := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
		sub1 := NewRelationIterator(baseRel)
		sub2 := NewFixedIterator(
			MustPathFromString("document:doc1#viewer@user:alice"),
		)

		union := NewUnion(sub1, sub2)
		result, changed, err := optimizer.Optimize(union)
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, union, result)
	})

	t.Run("reorder by selectivity - no change when equal", func(t *testing.T) {
		t.Parallel()
		// Create a union with 3 items
		// All FixedIterators have same selectivity (0.9)
		sub1 := NewFixedIterator(MustPathFromString("document:doc1#viewer@user:alice"))
		sub2 := NewFixedIterator(
			MustPathFromString("document:doc2#viewer@user:bob"),
			MustPathFromString("document:doc3#viewer@user:charlie"),
		)
		sub3 := NewFixedIterator(
			MustPathFromString("document:doc4#viewer@user:dave"),
			MustPathFromString("document:doc5#viewer@user:eve"),
			MustPathFromString("document:doc6#viewer@user:frank"),
		)

		union := NewUnion(sub1, sub2, sub3)
		result, changed, err := optimizer.Optimize(union)
		require.NoError(t, err)

		// Since all have same selectivity, order shouldn't change
		require.False(t, changed)
		require.Equal(t, union, result)
	})

	t.Run("reorder by selectivity - reorders by different selectivities", func(t *testing.T) {
		t.Parallel()
		// Create a union where subiterators have different selectivities
		// Unions prefer higher selectivity first (more likely to match)
		// Intersections combine selectivity: product for intersection
		// So create nested structures with different effective selectivities

		// Create sub-queries with different selectivity through intersection
		// Intersection multiplies selectivity, so more intersections = lower selectivity
		base1 := NewFixedIterator(MustPathFromString("document:doc1#viewer@user:alice"))
		base2 := NewFixedIterator(MustPathFromString("document:doc2#viewer@user:bob"))
		base3 := NewFixedIterator(MustPathFromString("document:doc3#viewer@user:charlie"))

		// Lower selectivity: intersection of 2 items (0.9 * 0.9 = 0.81)
		lowSelect := NewIntersection(base1, base2)

		// Higher selectivity: single item (0.9)
		highSelect := base3

		// Verify our selectivity assumptions
		lowEst, err := stats.Cost(lowSelect)
		require.NoError(t, err)
		highEst, err := stats.Cost(highSelect)
		require.NoError(t, err)
		require.Less(t, lowEst.CheckSelectivity, highEst.CheckSelectivity,
			"Low selectivity item should have lower selectivity than high selectivity item")

		// Create union with low selectivity first (should be reordered)
		union := NewUnion(lowSelect, highSelect)
		result, changed, err := optimizer.Optimize(union)
		require.NoError(t, err)

		// Should have reordered to put higher selectivity first
		require.True(t, changed, "Should reorder union to put higher selectivity first")

		// Verify the order changed
		require.IsType(t, &Union{}, result)
		resultUnion := result.(*Union)
		require.Len(t, resultUnion.subIts, 2)

		// First should be the higher selectivity item
		require.Equal(t, highSelect, resultUnion.subIts[0])
		require.Equal(t, lowSelect, resultUnion.subIts[1])

		// Verify selectivity order in result
		firstEst, err := stats.Cost(resultUnion.subIts[0])
		require.NoError(t, err)
		secondEst, err := stats.Cost(resultUnion.subIts[1])
		require.NoError(t, err)
		require.Greater(t, firstEst.CheckSelectivity, secondEst.CheckSelectivity,
			"First item should have higher selectivity than second")
	})

	t.Run("single subiterator", func(t *testing.T) {
		t.Parallel()
		sub := NewFixedIterator(
			MustPathFromString("document:doc1#viewer@user:alice"),
		)
		union := NewUnion(sub)

		result, changed, err := optimizer.Optimize(union)
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, union, result)
	})
}

func TestStatisticsOptimizer_ReorderIntersection(t *testing.T) {
	t.Parallel()

	stats := DefaultStaticStatistics()
	optimizer := StatisticsOptimizer{Source: stats}

	t.Run("no reordering needed", func(t *testing.T) {
		t.Parallel()
		// Already in optimal order (lower selectivity first)
		sub1 := NewFixedIterator(
			MustPathFromString("document:doc1#viewer@user:alice"),
		)
		sub2 := NewFixedIterator(
			MustPathFromString("document:doc2#viewer@user:bob"),
		)

		intersection := NewIntersection(sub1, sub2)
		result, changed, err := optimizer.Optimize(intersection)
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, intersection, result)
	})

	t.Run("single subiterator", func(t *testing.T) {
		t.Parallel()
		sub := NewFixedIterator(
			MustPathFromString("document:doc1#viewer@user:alice"),
		)
		intersection := NewIntersection(sub)

		result, changed, err := optimizer.Optimize(intersection)
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, intersection, result)
	})
}

func TestStatisticsOptimizer_RebalanceArrow(t *testing.T) {
	t.Parallel()

	stats := DefaultStaticStatistics()
	optimizer := StatisticsOptimizer{Source: stats}

	t.Run("no nested arrows", func(t *testing.T) {
		t.Parallel()
		left := NewFixedIterator(
			MustPathFromString("document:doc1#parent@folder:folder1"),
		)
		right := NewFixedIterator(
			MustPathFromString("folder:folder1#viewer@user:alice"),
		)
		arrow := NewArrow(left, right)

		result, changed, err := optimizer.Optimize(arrow)
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, arrow, result)
	})

	t.Run("left nested arrow - no rebalancing when costs equal", func(t *testing.T) {
		t.Parallel()
		// Create (A->B)->C with equal cardinalities
		// All single-path iterators result in equal costs for both arrangements
		a := NewFixedIterator(MustPathFromString("document:doc1#parent@folder:folder1"))
		b := NewFixedIterator(MustPathFromString("folder:folder1#parent@folder:folder2"))
		c := NewFixedIterator(MustPathFromString("folder:folder2#viewer@user:alice"))

		leftArrow := NewArrow(a, b)
		originalArrow := NewArrow(leftArrow, c)

		result, changed, err := optimizer.Optimize(originalArrow)
		require.NoError(t, err)
		require.False(t, changed, "Should not rebalance when costs are equal")
		require.Equal(t, originalArrow, result, "Result should be unchanged when no optimization occurs")
	})

	t.Run("left nested arrow - rebalancing with deeper nesting", func(t *testing.T) {
		t.Parallel()
		// Create ((A->B)->C)->D
		// This should rebalance to a cheaper structure
		a := NewFixedIterator(MustPathFromString("document:doc1#parent@folder:folder1"))
		b := NewFixedIterator(MustPathFromString("folder:folder1#parent@folder:folder2"))
		c := NewFixedIterator(MustPathFromString("folder:folder2#parent@folder:folder3"))
		d := NewFixedIterator(MustPathFromString("folder:folder3#viewer@user:alice"))

		ab := NewArrow(a, b)
		abc := NewArrow(ab, c)
		originalArrow := NewArrow(abc, d)

		// Calculate original cost
		originalCost, err := stats.Cost(originalArrow)
		require.NoError(t, err)

		result, changed, err := optimizer.Optimize(originalArrow)
		require.NoError(t, err)

		if changed {
			// Verify the optimized result is actually cheaper
			resultCost, err := stats.Cost(result)
			require.NoError(t, err)
			require.Less(t, resultCost.CheckCost, originalCost.CheckCost,
				"Optimized structure should be cheaper than original")
		}
	})

	t.Run("right nested arrow - no rebalancing when costs equal", func(t *testing.T) {
		t.Parallel()
		// Create A->(B->C) with equal cardinalities
		// All single-path iterators result in equal costs for both arrangements
		a := NewFixedIterator(MustPathFromString("document:doc1#parent@folder:folder1"))
		b := NewFixedIterator(MustPathFromString("folder:folder1#parent@folder:folder2"))
		c := NewFixedIterator(MustPathFromString("folder:folder2#viewer@user:alice"))

		rightArrow := NewArrow(b, c)
		originalArrow := NewArrow(a, rightArrow)

		result, changed, err := optimizer.Optimize(originalArrow)
		require.NoError(t, err)
		require.False(t, changed, "Should not rebalance when costs are equal")
		require.Equal(t, originalArrow, result, "Result should be unchanged when no optimization occurs")
	})

	t.Run("complex nested arrows", func(t *testing.T) {
		t.Parallel()
		// Create a more complex structure with varying cardinalities
		a := NewFixedIterator(
			MustPathFromString("document:doc1#parent@folder:folder1"),
			MustPathFromString("document:doc2#parent@folder:folder1"),
			MustPathFromString("document:doc3#parent@folder:folder1"),
		)
		b := NewFixedIterator(MustPathFromString("folder:folder1#parent@folder:folder2"))
		c := NewFixedIterator(MustPathFromString("folder:folder2#viewer@user:alice"))

		// Try (A->B)->C structure
		leftArrow := NewArrow(a, b)
		arrow := NewArrow(leftArrow, c)

		result, changed, err := optimizer.Optimize(arrow)
		require.NoError(t, err)

		// Verify result is valid regardless of whether it changed
		require.NotNil(t, result)
		if changed {
			// If it changed, verify the new structure is actually cheaper
			originalCost, err := stats.Cost(arrow)
			require.NoError(t, err)
			newCost, err := stats.Cost(result)
			require.NoError(t, err)
			require.Less(t, newCost.CheckCost, originalCost.CheckCost,
				"Optimized structure should be cheaper")
		}
	})
}

func TestStatisticsOptimizer_IntersectionArrowNotRebalanced(t *testing.T) {
	t.Parallel()

	stats := DefaultStaticStatistics()
	optimizer := StatisticsOptimizer{Source: stats}

	t.Run("intersection arrow should not be rebalanced", func(t *testing.T) {
		t.Parallel()
		// IntersectionArrow should not be rebalanced even if nested
		left := NewFixedIterator(
			MustPathFromString("document:doc1#team@team:eng"),
		)
		right := NewFixedIterator(
			MustPathFromString("team:eng#member@user:alice"),
		)
		intersectionArrow := NewIntersectionArrow(left, right)

		result, changed, err := optimizer.Optimize(intersectionArrow)
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, intersectionArrow, result)
	})
}

func TestStatisticsOptimizer_ComplexTree(t *testing.T) {
	t.Parallel()

	stats := DefaultStaticStatistics()
	optimizer := StatisticsOptimizer{Source: stats}

	t.Run("optimize complex nested structure", func(t *testing.T) {
		t.Parallel()
		// Create a complex tree with unions, intersections, and arrows
		sub1 := NewFixedIterator(MustPathFromString("document:doc1#viewer@user:alice"))
		sub2 := NewFixedIterator(MustPathFromString("document:doc2#viewer@user:bob"))
		sub3 := NewFixedIterator(MustPathFromString("document:doc3#viewer@user:charlie"))

		union := NewUnion(sub1, sub2, sub3)

		arrow1 := NewArrow(
			NewFixedIterator(MustPathFromString("document:doc1#parent@folder:folder1")),
			NewFixedIterator(MustPathFromString("folder:folder1#viewer@user:alice")),
		)

		arrow2 := NewArrow(
			NewFixedIterator(MustPathFromString("document:doc1#editor@user:alice")),
			NewFixedIterator(MustPathFromString("user:alice#viewer@user:alice")),
		)

		intersection := NewIntersection(union, arrow1, arrow2)

		result, changed, err := optimizer.Optimize(intersection)
		require.NoError(t, err)

		// Verify result is valid
		require.NotNil(t, result)

		// The optimization should work without errors
		// Whether it changes depends on the cost calculations
		if changed {
			// If changed, the new structure should still be a valid iterator
			require.NotNil(t, result.Subiterators())
		}
	})

	t.Run("nested unions and intersections", func(t *testing.T) {
		t.Parallel()
		// Create deeply nested structure
		innerUnion := NewUnion(
			NewFixedIterator(MustPathFromString("document:doc1#viewer@user:alice")),
			NewFixedIterator(MustPathFromString("document:doc2#viewer@user:bob")),
		)

		innerIntersection := NewIntersection(
			NewFixedIterator(MustPathFromString("document:doc1#editor@user:alice")),
			NewFixedIterator(MustPathFromString("document:doc1#owner@user:alice")),
		)

		outerUnion := NewUnion(innerUnion, innerIntersection)

		result, changed, err := optimizer.Optimize(outerUnion)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify the optimizer handles nested structures
		_ = changed // May or may not change depending on selectivities
	})
}

func TestStatisticsOptimizer_EdgeCases(t *testing.T) {
	t.Parallel()

	stats := DefaultStaticStatistics()
	optimizer := StatisticsOptimizer{Source: stats}

	t.Run("empty iterator", func(t *testing.T) {
		t.Parallel()
		empty := NewFixedIterator()
		result, changed, err := optimizer.Optimize(empty)
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, empty, result)
	})

	t.Run("single fixed iterator", func(t *testing.T) {
		t.Parallel()
		single := NewFixedIterator(
			MustPathFromString("document:doc1#viewer@user:alice"),
		)
		result, changed, err := optimizer.Optimize(single)
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, single, result)
	})

	t.Run("relation iterator", func(t *testing.T) {
		t.Parallel()
		baseRel := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
		rel := NewRelationIterator(baseRel)
		result, changed, err := optimizer.Optimize(rel)
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, rel, result)
	})
}

func TestStatisticsOptimizer_Idempotence(t *testing.T) {
	t.Parallel()

	stats := DefaultStaticStatistics()
	optimizer := StatisticsOptimizer{Source: stats}

	t.Run("repeated optimization should be idempotent", func(t *testing.T) {
		t.Parallel()
		// Create a structure
		sub1 := NewFixedIterator(MustPathFromString("document:doc1#viewer@user:alice"))
		sub2 := NewFixedIterator(MustPathFromString("document:doc2#viewer@user:bob"))
		sub3 := NewFixedIterator(MustPathFromString("document:doc3#viewer@user:charlie"))

		union := NewUnion(sub1, sub2, sub3)

		// First optimization
		result1, changed1, err := optimizer.Optimize(union)
		require.NoError(t, err)

		// Second optimization on the result
		result2, changed2, err := optimizer.Optimize(result1)
		require.NoError(t, err)

		// Second optimization should not change anything
		if changed1 {
			require.False(t, changed2, "Second optimization should be idempotent")
			require.Equal(t, result1, result2)
		}
	})
}
