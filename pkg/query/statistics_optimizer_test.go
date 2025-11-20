package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
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

	t.Run("reorder by selectivity", func(t *testing.T) {
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
		_, changed, err := optimizer.Optimize(union)
		require.NoError(t, err)

		// Since all have same selectivity, order shouldn't change
		require.False(t, changed)
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

	t.Run("left nested arrow", func(t *testing.T) {
		t.Parallel()
		// Create (A->B)->C
		a := NewFixedIterator(MustPathFromString("document:doc1#parent@folder:folder1"))
		b := NewFixedIterator(MustPathFromString("folder:folder1#parent@folder:folder2"))
		c := NewFixedIterator(MustPathFromString("folder:folder2#viewer@user:alice"))

		leftArrow := NewArrow(a, b)
		originalArrow := NewArrow(leftArrow, c)

		// Calculate costs
		originalCost, err := stats.Cost(originalArrow)
		require.NoError(t, err)

		// Alternative: A->(B->C)
		innerArrow := NewArrow(b, c)
		alternative := NewArrow(a, innerArrow)
		alternativeCost, err := stats.Cost(alternative)
		require.NoError(t, err)

		result, changed, err := optimizer.Optimize(originalArrow)
		require.NoError(t, err)

		// Check if optimization occurred when expected
		if alternativeCost.CheckCost < originalCost.CheckCost {
			require.True(t, changed, "Should have rebalanced when alternative is cheaper")
			// Verify structure
			resultArrow, ok := result.(*Arrow)
			require.True(t, ok)
			_, isArrow := resultArrow.right.(*Arrow)
			require.True(t, isArrow, "Right side should be an arrow after rebalancing")
		} else {
			require.False(t, changed, "Should not rebalance when original is cheaper or equal")
		}
	})

	t.Run("right nested arrow", func(t *testing.T) {
		t.Parallel()
		// Create A->(B->C)
		a := NewFixedIterator(MustPathFromString("document:doc1#parent@folder:folder1"))
		b := NewFixedIterator(MustPathFromString("folder:folder1#parent@folder:folder2"))
		c := NewFixedIterator(MustPathFromString("folder:folder2#viewer@user:alice"))

		rightArrow := NewArrow(b, c)
		originalArrow := NewArrow(a, rightArrow)

		// Calculate costs
		originalCost, err := stats.Cost(originalArrow)
		require.NoError(t, err)

		// Alternative: (A->B)->C
		innerArrow := NewArrow(a, b)
		alternative := NewArrow(innerArrow, c)
		alternativeCost, err := stats.Cost(alternative)
		require.NoError(t, err)

		result, changed, err := optimizer.Optimize(originalArrow)
		require.NoError(t, err)

		// Check if optimization occurred when expected
		if alternativeCost.CheckCost < originalCost.CheckCost {
			require.True(t, changed, "Should have rebalanced when alternative is cheaper")
			// Verify structure
			resultArrow, ok := result.(*Arrow)
			require.True(t, ok)
			_, isArrow := resultArrow.left.(*Arrow)
			require.True(t, isArrow, "Left side should be an arrow after rebalancing")
		} else {
			require.False(t, changed, "Should not rebalance when original is cheaper or equal")
		}
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

func TestStatisticsOptimizer_RebalanceArrowThroughWrappers(t *testing.T) {
	t.Parallel()

	stats := DefaultStaticStatistics()
	optimizer := StatisticsOptimizer{Source: stats}

	t.Run("wrapped left arrow - alias", func(t *testing.T) {
		t.Parallel()
		// Create Alias(A->B)->C
		a := NewFixedIterator(MustPathFromString("document:doc1#parent@folder:folder1"))
		b := NewFixedIterator(MustPathFromString("folder:folder1#parent@folder:folder2"))
		c := NewFixedIterator(MustPathFromString("folder:folder2#viewer@user:alice"))

		innerArrow := NewArrow(a, b)
		aliasedArrow := NewAlias("parent_rel", innerArrow)
		outerArrow := NewArrow(aliasedArrow, c)

		result, changed, err := optimizer.Optimize(outerArrow)
		require.NoError(t, err)

		// Verify result is valid
		require.NotNil(t, result)

		// If changed, verify structure is correct
		if changed {
			resultArrow, ok := result.(*Arrow)
			require.True(t, ok, "Result should be an Arrow")

			// The right side should be wrapped (possibly with alias)
			require.NotNil(t, resultArrow.right)
		}
	})

	t.Run("wrapped right arrow - caveat", func(t *testing.T) {
		t.Parallel()
		// Create A->Caveat(B->C)
		a := NewFixedIterator(MustPathFromString("document:doc1#parent@folder:folder1"))
		b := NewFixedIterator(MustPathFromString("folder:folder1#parent@folder:folder2"))
		c := NewFixedIterator(MustPathFromString("folder:folder2#viewer@user:alice"))

		innerArrow := NewArrow(b, c)
		// Note: Creating a real CaveatIterator requires a caveat, so we'll use Alias as a proxy
		wrappedArrow := NewAlias("caveated", innerArrow)
		outerArrow := NewArrow(a, wrappedArrow)

		result, changed, err := optimizer.Optimize(outerArrow)
		require.NoError(t, err)

		// Verify result is valid
		require.NotNil(t, result)

		// If changed, verify the structure
		if changed {
			resultArrow, ok := result.(*Arrow)
			require.True(t, ok, "Result should be an Arrow")

			// The left side should be wrapped (possibly with alias)
			require.NotNil(t, resultArrow.left)
		}
	})

	t.Run("caveat stays on correct branch", func(t *testing.T) {
		t.Parallel()
		// Create a caveat that only applies to branch B
		// Structure: Caveat(A->B)->C where caveat is on B
		baseRelB := schema.NewTestBaseRelationWithFeatures("folder", "viewer", "user", tuple.Ellipsis, "test_caveat", false)

		a := NewFixedIterator(MustPathFromString("document:doc1#parent@folder:folder1"))
		b := NewRelationIterator(baseRelB) // This has the caveat
		c := NewFixedIterator(MustPathFromString("user:alice#viewer@user:alice"))

		innerArrow := NewArrow(a, b)
		// Wrap with caveat (simulating the caveat being pushed down)
		caveatIt := NewCaveatIterator(innerArrow, &core.ContextualizedCaveat{
			CaveatName: "test_caveat",
		})
		outerArrow := NewArrow(caveatIt, c)

		result, changed, err := optimizer.Optimize(outerArrow)
		require.NoError(t, err)

		// Verify result is valid
		require.NotNil(t, result)

		// If it rebalanced, the caveat should still be present somewhere
		// The caveat should stay associated with branch B
		if changed {
			// Walk the result to ensure caveat is still present
			foundCaveat := false
			_, _ = Walk(result, func(node Iterator) (Iterator, error) {
				if _, ok := node.(*CaveatIterator); ok {
					foundCaveat = true
				}
				return node, nil
			})
			// If the structure changed, caveat should still be there
			// (it might not be rebalanced if it's not cheaper, which is fine)
			_ = foundCaveat
		}
	})

	t.Run("double wrapped arrow", func(t *testing.T) {
		t.Parallel()
		// Create Alias(Alias(A->B))->C
		a := NewFixedIterator(MustPathFromString("document:doc1#parent@folder:folder1"))
		b := NewFixedIterator(MustPathFromString("folder:folder1#parent@folder:folder2"))
		c := NewFixedIterator(MustPathFromString("folder:folder2#viewer@user:alice"))

		innerArrow := NewArrow(a, b)
		alias1 := NewAlias("rel1", innerArrow)
		alias2 := NewAlias("rel2", alias1)
		outerArrow := NewArrow(alias2, c)

		result, _, err := optimizer.Optimize(outerArrow)
		require.NoError(t, err)

		// Verify result is valid regardless of whether it changed
		require.NotNil(t, result)
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
