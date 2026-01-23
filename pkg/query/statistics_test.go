package query

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestStaticStatistics_Cost(t *testing.T) {
	t.Parallel()

	stats := DefaultStaticStatistics()

	t.Run("FixedIterator", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			it := NewFixedIterator()
			est, err := stats.Cost(it)
			require.NoError(t, err)
			require.Equal(t, 0, est.Cardinality)
			require.Equal(t, 1, est.CheckCost)
			require.Equal(t, 0, est.IterResourcesCost)
			require.Equal(t, 0, est.IterSubjectsCost)
		})

		t.Run("with paths", func(t *testing.T) {
			it := NewFixedIterator(
				MustPathFromString("document:doc1#viewer@user:alice"),
				MustPathFromString("document:doc2#viewer@user:bob"),
				MustPathFromString("document:doc3#viewer@user:charlie"),
			)
			est, err := stats.Cost(it)
			require.NoError(t, err)
			require.Equal(t, 3, est.Cardinality)
			require.Equal(t, 1, est.CheckCost)
			require.Equal(t, stats.CheckSelectivity, est.CheckSelectivity) // nolint:testifylint // these values aren't being operated on
			require.Equal(t, 3, est.IterResourcesCost)
			require.Equal(t, 3, est.IterSubjectsCost)
		})
	})

	t.Run("RelationIterator", func(t *testing.T) {
		t.Parallel()

		baseRel := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
		it := NewRelationIterator(baseRel)
		est, err := stats.Cost(it)
		require.NoError(t, err)
		require.Equal(t, stats.NumberOfTuplesInRelation, est.Cardinality)
		require.Equal(t, 1, est.CheckCost)
		require.Equal(t, stats.CheckSelectivity, est.CheckSelectivity) // nolint:testifylint // these values aren't being operated on
		require.Equal(t, stats.Fanout, est.IterResourcesCost)
		require.Equal(t, stats.Fanout, est.IterSubjectsCost)
	})

	t.Run("Arrow", func(t *testing.T) {
		t.Parallel()

		t.Run("simple", func(t *testing.T) {
			left := NewFixedIterator(
				MustPathFromString("document:doc1#parent@folder:folder1"),
				MustPathFromString("document:doc2#parent@folder:folder2"),
			)
			right := NewFixedIterator(
				MustPathFromString("folder:folder1#viewer@user:alice"),
			)
			arrow := NewArrow(left, right)
			est, err := stats.Cost(arrow)
			require.NoError(t, err)

			// Cardinality: left.Cardinality * right.Cardinality = 2 * 1 = 2
			require.Equal(t, 2, est.Cardinality)

			// CheckCost: left.IterSubjectsCost + (left.Cardinality * right.CheckCost)
			// = 2 + (2 * 1) = 4
			require.Equal(t, 4, est.CheckCost)

			// CheckSelectivity: left * right = 0.9 * 0.9 = 0.81
			require.InDelta(t, 0.81, est.CheckSelectivity, 0.001)

			// IterResourcesCost: left.IterResourcesCost + (left.Cardinality + right.IterResourcesCost)
			// = 2 + 2 * 1 = 4
			require.Equal(t, 4, est.IterResourcesCost)
		})

		t.Run("reversed arrow", func(t *testing.T) {
			left := NewFixedIterator(
				MustPathFromString("document:doc1#parent@folder:folder1"),
				MustPathFromString("document:doc2#parent@folder:folder2"),
			)
			right := NewFixedIterator(
				MustPathFromString("folder:folder1#viewer@user:alice"),
			)
			arrow := NewArrow(left, right)

			// Set arrow direction to the other way
			arrow.direction = rightToLeft

			est, err := stats.Cost(arrow)
			require.NoError(t, err)

			// Cardinality: left.Cardinality * right.Cardinality = 2 * 1 = 2
			require.Equal(t, 2, est.Cardinality)

			// CheckCost: right.IterResourcesCost + (right.Cardinality * left.CheckCost)
			// = 1 + (1 * 2) = 2
			require.Equal(t, 2, est.CheckCost)

			// CheckSelectivity: left * right = 0.9 * 0.9 = 0.81
			require.InDelta(t, 0.81, est.CheckSelectivity, 0.001)

			// IterResourcesCost: right.IterResourcesCost + (right.Cardinality + left.IterResourcesCost)
			// = 1 + 1 * 2 = 3
			require.Equal(t, 3, est.IterResourcesCost)
		})

		t.Run("with relation iterator", func(t *testing.T) {
			baseRel := schema.NewTestBaseRelation("document", "parent", "folder", tuple.Ellipsis)
			left := NewRelationIterator(baseRel)
			right := NewFixedIterator(
				MustPathFromString("folder:folder1#viewer@user:alice"),
			)
			arrow := NewArrow(left, right)
			est, err := stats.Cost(arrow)
			require.NoError(t, err)

			// Left has cardinality of DatastoreSize (10)
			// Cardinality: 10 * 1 = 10
			require.Equal(t, 10, est.Cardinality)

			// CheckCost: 5 + (10 * 1) = 5 + 10 = 15
			require.Equal(t, 15, est.CheckCost)
		})
	})

	t.Run("IntersectionArrow", func(t *testing.T) {
		t.Parallel()

		left := NewFixedIterator(
			MustPathFromString("document:doc1#team@team:eng"),
			MustPathFromString("document:doc1#team@team:design"),
			MustPathFromString("document:doc1#team@team:sales"),
		)
		right := NewFixedIterator(
			MustPathFromString("team:eng#member@user:alice"),
		)
		intersectionArrow := NewIntersectionArrow(left, right)
		est, err := stats.Cost(intersectionArrow)
		require.NoError(t, err)

		// IntersectionArrow has reduced selectivity due to "all" semantics
		// selectivity = (left.CheckSelectivity ^ fanout) * right.CheckSelectivity
		// = (0.9 ^ 5) * 0.9 = 0.5314
		expectedSelectivity := math.Pow(0.9, 5.0) * 0.9
		require.InDelta(t, expectedSelectivity, est.CheckSelectivity, 0.001)

		// Cardinality: int(left.Cardinality * right.Cardinality * selectivity)
		// = int(3 * 1 * 0.405) = 1
		require.Equal(t, 1, est.Cardinality)

		// CheckCost: left.IterSubjectsCost + (left.Cardinality * right.CheckCost)
		// = 3 + (3 * 1) = 6
		require.Equal(t, 6, est.CheckCost)

		// IterResourcesCost: left.IterResourcesCost + (left.Cardinality * right.IterResourcesCost) + checkCost
		// = 3 + (3 * 1) + 6 = 12
		require.Equal(t, 12, est.IterResourcesCost)
	})

	t.Run("Union", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			union := NewUnion()
			_, err := stats.Cost(union)
			require.Error(t, err)
			require.Contains(t, err.Error(), "union with no subiterators")
		})

		t.Run("single subiterator", func(t *testing.T) {
			sub := NewFixedIterator(
				MustPathFromString("document:doc1#viewer@user:alice"),
			)
			union := NewUnion(sub)
			est, err := stats.Cost(union)
			require.NoError(t, err)
			require.Equal(t, 1, est.Cardinality)
			require.Equal(t, 1, est.CheckCost)
		})

		t.Run("multiple subiterators", func(t *testing.T) {
			sub1 := NewFixedIterator(
				MustPathFromString("document:doc1#viewer@user:alice"),
				MustPathFromString("document:doc2#viewer@user:alice"),
			)
			sub2 := NewFixedIterator(
				MustPathFromString("document:doc3#editor@user:alice"),
			)
			sub3 := NewFixedIterator(
				MustPathFromString("document:doc4#owner@user:alice"),
			)
			union := NewUnion(sub1, sub2, sub3)
			est, err := stats.Cost(union)
			require.NoError(t, err)

			// Cardinality: sum of all = 2 + 1 + 1 = 4
			require.Equal(t, 4, est.Cardinality)

			// CheckCost: sum of all = 1 + 1 + 1 = 3
			require.Equal(t, 3, est.CheckCost)

			// CheckSelectivity: max of all branches (all are 0.9)
			require.Equal(t, 0.9, est.CheckSelectivity) // nolint:testifylint // these values aren't being operated on

			// IterResourcesCost and IterSubjectsCost: sum of all
			require.Equal(t, 4, est.IterResourcesCost)
			require.Equal(t, 4, est.IterSubjectsCost)
		})

		t.Run("mixed costs", func(t *testing.T) {
			// One expensive relation iterator and one cheap fixed iterator
			baseRel := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
			sub1 := NewRelationIterator(baseRel)
			sub2 := NewFixedIterator(
				MustPathFromString("document:doc1#viewer@user:alice"),
			)
			union := NewUnion(sub1, sub2)
			est, err := stats.Cost(union)
			require.NoError(t, err)

			// Cardinality: 10 + 1 = 11
			require.Equal(t, 11, est.Cardinality)

			// CheckCost: 1 + 1 = 2
			require.Equal(t, 2, est.CheckCost)

			// IterResourcesCost: 5 + 1 = 6
			require.Equal(t, 6, est.IterResourcesCost)
		})
	})

	t.Run("Intersection", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			intersection := NewIntersection()
			_, err := stats.Cost(intersection)
			require.Error(t, err)
			require.Contains(t, err.Error(), "intersection with no subiterators")
		})

		t.Run("single subiterator", func(t *testing.T) {
			sub := NewFixedIterator(
				MustPathFromString("document:doc1#viewer@user:alice"),
				MustPathFromString("document:doc2#viewer@user:alice"),
			)
			intersection := NewIntersection(sub)
			est, err := stats.Cost(intersection)
			require.NoError(t, err)

			// With single iterator, cardinality is just that iterator's cardinality
			require.Equal(t, 2, est.Cardinality)
			require.Equal(t, 1, est.CheckCost)
			require.Equal(t, 0.9, est.CheckSelectivity) // nolint:testifylint // these values aren't being operated on
		})

		t.Run("two subiterators", func(t *testing.T) {
			sub1 := NewFixedIterator(
				MustPathFromString("document:doc1#viewer@user:alice"),
				MustPathFromString("document:doc2#viewer@user:alice"),
				MustPathFromString("document:doc3#viewer@user:alice"),
			)
			sub2 := NewFixedIterator(
				MustPathFromString("document:doc1#editor@user:alice"),
				MustPathFromString("document:doc2#editor@user:alice"),
			)
			intersection := NewIntersection(sub1, sub2)
			est, err := stats.Cost(intersection)
			require.NoError(t, err)

			// First iterator: cardinality = 3
			// Second iterator: cardinality = 2
			// Intersection sums: 3 + 2 = 5
			require.Equal(t, 5, est.Cardinality)

			// CheckCost: sum of both = 1 + 1 = 2
			require.Equal(t, 2, est.CheckCost)

			// CheckSelectivity: product = 0.9 * 0.9 = 0.81
			require.InDelta(t, 0.81, est.CheckSelectivity, 0.001)

			// IterResourcesCost and IterSubjectsCost: sum of both
			require.Equal(t, 5, est.IterResourcesCost)
			require.Equal(t, 5, est.IterSubjectsCost)
		})

		t.Run("three subiterators", func(t *testing.T) {
			sub1 := NewFixedIterator(
				MustPathFromString("document:doc1#viewer@user:alice"),
				MustPathFromString("document:doc2#viewer@user:alice"),
				MustPathFromString("document:doc3#viewer@user:alice"),
				MustPathFromString("document:doc4#viewer@user:alice"),
				MustPathFromString("document:doc5#viewer@user:alice"),
			)
			sub2 := NewFixedIterator(
				MustPathFromString("document:doc1#editor@user:alice"),
			)
			sub3 := NewFixedIterator(
				MustPathFromString("document:doc1#owner@user:alice"),
			)
			intersection := NewIntersection(sub1, sub2, sub3)
			est, err := stats.Cost(intersection)
			require.NoError(t, err)

			// First: cardinality = 5
			// Second: cardinality = 1
			// Third: cardinality = 1
			// Intersection sums: 5 + 1 + 1 = 7
			require.Equal(t, 7, est.Cardinality)

			// CheckCost: 1 + 1 + 1 = 3
			require.Equal(t, 3, est.CheckCost)

			// CheckSelectivity: 0.9 * 0.9 * 0.9 = 0.729
			require.InDelta(t, 0.729, est.CheckSelectivity, 0.001)
		})

		t.Run("with expensive iterator first", func(t *testing.T) {
			baseRel := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
			sub1 := NewRelationIterator(baseRel)
			sub2 := NewFixedIterator(
				MustPathFromString("document:doc1#editor@user:alice"),
			)
			intersection := NewIntersection(sub1, sub2)
			est, err := stats.Cost(intersection)
			require.NoError(t, err)

			// First: cardinality = 10
			// Second: cardinality = 1
			// Intersection sums: 10 + 1 = 11
			require.Equal(t, 11, est.Cardinality)

			// CheckCost: 1 + 1 = 2
			require.Equal(t, 2, est.CheckCost)
		})
	})

	t.Run("Complex nested iterators", func(t *testing.T) {
		t.Parallel()

		t.Run("union of arrows", func(t *testing.T) {
			// Create two arrows and union them
			arrow1 := NewArrow(
				NewFixedIterator(MustPathFromString("document:doc1#parent@folder:folder1")),
				NewFixedIterator(MustPathFromString("folder:folder1#viewer@user:alice")),
			)
			arrow2 := NewArrow(
				NewFixedIterator(MustPathFromString("document:doc2#parent@folder:folder2")),
				NewFixedIterator(MustPathFromString("folder:folder2#viewer@user:alice")),
			)
			union := NewUnion(arrow1, arrow2)
			est, err := stats.Cost(union)
			require.NoError(t, err)

			// Each arrow has cardinality 1*1 = 1, union sums: 1 + 1 = 2
			require.Equal(t, 2, est.Cardinality)

			// Each arrow has CheckCost 1 + (1*1) = 2, union sums: 2 + 2 = 4
			require.Equal(t, 4, est.CheckCost)
		})

		t.Run("intersection of arrows", func(t *testing.T) {
			arrow1 := NewArrow(
				NewFixedIterator(
					MustPathFromString("document:doc1#parent@folder:folder1"),
					MustPathFromString("document:doc2#parent@folder:folder2"),
				),
				NewFixedIterator(MustPathFromString("folder:folder1#viewer@user:alice")),
			)
			arrow2 := NewArrow(
				NewFixedIterator(MustPathFromString("document:doc1#editor@user:alice")),
				NewFixedIterator(MustPathFromString("user:alice#viewer@user:alice")),
			)
			intersection := NewIntersection(arrow1, arrow2)
			est, err := stats.Cost(intersection)
			require.NoError(t, err)

			// First arrow: cardinality = 2*1 = 2
			// Second arrow: cardinality = 1*1 = 1
			// Intersection sums: 2 + 1 = 3
			require.Equal(t, 3, est.Cardinality)

			// Each arrow has CheckCost, they sum
			require.Positive(t, est.CheckCost)
		})

		t.Run("arrow of unions", func(t *testing.T) {
			leftUnion := NewUnion(
				NewFixedIterator(MustPathFromString("document:doc1#parent@folder:folder1")),
				NewFixedIterator(MustPathFromString("document:doc2#parent@folder:folder2")),
			)
			rightUnion := NewUnion(
				NewFixedIterator(MustPathFromString("folder:folder1#viewer@user:alice")),
				NewFixedIterator(MustPathFromString("folder:folder2#viewer@user:bob")),
			)
			arrow := NewArrow(leftUnion, rightUnion)
			est, err := stats.Cost(arrow)
			require.NoError(t, err)

			// Left union: cardinality = 1 + 1 = 2
			// Right union: cardinality = 1 + 1 = 2
			// Arrow: 2 * 2 = 4
			require.Equal(t, 4, est.Cardinality)
		})
	})

	t.Run("Exclusion", func(t *testing.T) {
		t.Parallel()

		t.Run("simple exclusion", func(t *testing.T) {
			mainSet := NewFixedIterator(
				MustPathFromString("document:doc1#viewer@user:alice"),
				MustPathFromString("document:doc2#viewer@user:bob"),
				MustPathFromString("document:doc3#viewer@user:charlie"),
			)
			excluded := NewFixedIterator(
				MustPathFromString("document:doc2#viewer@user:bob"),
			)

			exclusion := NewExclusion(mainSet, excluded)
			est, err := stats.Cost(exclusion)
			require.NoError(t, err)

			// Main has cardinality 3, excluded has selectivity 0.9
			// Exclusion factor: 1 - 0.9 = 0.1
			// Cardinality: 3 * 0.1 = 0.3 -> 0
			require.Equal(t, 0, est.Cardinality)

			// CheckCost: sum of both = 1 + 1 = 2
			require.Equal(t, 2, est.CheckCost)

			// CheckSelectivity: main * exclusionFactor = 0.9 * 0.1 = 0.09
			require.InDelta(t, 0.09, est.CheckSelectivity, 0.001)

			// IterResourcesCost: sum of both = 3 + 1 = 4
			require.Equal(t, 4, est.IterResourcesCost)
		})

		t.Run("exclusion with relation iterator", func(t *testing.T) {
			baseRel := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
			mainSet := NewRelationIterator(baseRel)
			excluded := NewFixedIterator(
				MustPathFromString("document:doc1#viewer@user:alice"),
				MustPathFromString("document:doc2#viewer@user:bob"),
			)

			exclusion := NewExclusion(mainSet, excluded)
			est, err := stats.Cost(exclusion)
			require.NoError(t, err)

			// Main has cardinality 10, excluded has selectivity 0.9
			// Exclusion factor: 1 - 0.9 = 0.1
			// Cardinality: int(10.0 * 0.1) = int(1.0) should be 1
			// But due to floating point, this might be slightly less than 1.0
			// which truncates to 0
			require.GreaterOrEqual(t, est.Cardinality, 0)
			require.LessOrEqual(t, est.Cardinality, 1)

			// CheckCost: 1 + 1 = 2
			require.Equal(t, 2, est.CheckCost)
		})
	})

	t.Run("Passthrough iterators", func(t *testing.T) {
		t.Parallel()

		// Create a mock iterator that has one subiterator (should pass through)
		fixed := NewFixedIterator(
			MustPathFromString("document:doc1#viewer@user:alice"),
		)
		// Alias wraps another iterator and should pass through
		alias := NewAlias("test_alias", fixed)
		est, err := stats.Cost(alias)
		require.NoError(t, err)

		// Should have same cost as the wrapped iterator
		require.Equal(t, 1, est.Cardinality)
		require.Equal(t, 1, est.CheckCost)
	})
}

func TestStaticStatistics_CustomConfig(t *testing.T) {
	t.Parallel()

	t.Run("custom datastore size", func(t *testing.T) {
		t.Parallel()
		stats := StaticStatistics{
			NumberOfTuplesInRelation: 1000,
			Fanout:                   10,
			CheckSelectivity:         0.8,
		}

		baseRel := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
		it := NewRelationIterator(baseRel)
		est, err := stats.Cost(it)
		require.NoError(t, err)
		require.Equal(t, 1000, est.Cardinality)
		require.Equal(t, 10, est.IterResourcesCost) // Fanout
	})

	t.Run("custom selectivity", func(t *testing.T) {
		t.Parallel()
		stats := StaticStatistics{
			NumberOfTuplesInRelation: 10,
			Fanout:                   5,
			CheckSelectivity:         0.5,
		}

		it := NewFixedIterator(
			MustPathFromString("document:doc1#viewer@user:alice"),
		)
		est, err := stats.Cost(it)
		require.NoError(t, err)
		require.Equal(t, 0.5, est.CheckSelectivity) // nolint:testifylint // these values aren't being operated on
	})

	t.Run("custom intersection arrow reduction", func(t *testing.T) {
		t.Parallel()
		stats := StaticStatistics{
			NumberOfTuplesInRelation: 10,
			Fanout:                   5,
			CheckSelectivity:         0.9,
		}

		left := NewFixedIterator(
			MustPathFromString("document:doc1#team@team:eng"),
		)
		right := NewFixedIterator(
			MustPathFromString("team:eng#member@user:alice"),
		)
		intersectionArrow := NewIntersectionArrow(left, right)
		est, err := stats.Cost(intersectionArrow)
		require.NoError(t, err)

		// selectivity = (0.9 ^ 5) * 0.9 = 0.531
		require.InDelta(t, 0.531, est.CheckSelectivity, 0.001)
	})
}
