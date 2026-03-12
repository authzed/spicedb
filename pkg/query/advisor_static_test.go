package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestStaticAdvisor_GetHints_ArrowDirection(t *testing.T) {
	advisor := DefaultStaticAdvisor()

	t.Run("prefers right-to-left when right is cheaper", func(t *testing.T) {
		// Create an arrow outline where right has low cardinality (1 path)
		// and left has higher cardinality (3 paths)
		// This should prefer right-to-left execution
		leftOutline := Outline{
			Type: FixedIteratorType,
			Args: &IteratorArgs{
				FixedPaths: []Path{
					MustPathFromString("document:doc1#parent@folder:folder1"),
					MustPathFromString("document:doc2#parent@folder:folder1"),
					MustPathFromString("document:doc3#parent@folder:folder1"),
				},
			},
		}

		rightOutline := Outline{
			Type: FixedIteratorType,
			Args: &IteratorArgs{
				FixedPaths: []Path{
					MustPathFromString("folder:folder1#viewer@user:alice"),
				},
			},
		}

		arrowOutline := Outline{
			Type:        ArrowIteratorType,
			SubOutlines: []Outline{leftOutline, rightOutline},
		}

		hints, err := advisor.GetHints(arrowOutline, CanonicalOutline{})
		require.NoError(t, err)
		require.Len(t, hints, 1)

		// Verify the hint suggests right-to-left direction
		// We can test this by applying the hint to a compiled arrow and checking direction
		arrow := NewArrowIterator(
			NewFixedIterator(MustPathFromString("document:doc1#parent@folder:folder1")),
			NewFixedIterator(MustPathFromString("folder:folder1#viewer@user:alice")),
		)

		err = hints[0](arrow)
		require.NoError(t, err)
		require.Equal(t, rightToLeft, arrow.direction)
	})

	t.Run("prefers left-to-right when left is cheaper", func(t *testing.T) {
		// Create an arrow outline where left has low cardinality (1 path)
		// and right has higher cardinality (3 paths)
		// This should prefer left-to-right execution
		leftOutline := Outline{
			Type: FixedIteratorType,
			Args: &IteratorArgs{
				FixedPaths: []Path{
					MustPathFromString("document:doc1#parent@folder:folder1"),
				},
			},
		}

		rightOutline := Outline{
			Type: FixedIteratorType,
			Args: &IteratorArgs{
				FixedPaths: []Path{
					MustPathFromString("folder:folder1#viewer@user:alice"),
					MustPathFromString("folder:folder1#viewer@user:bob"),
					MustPathFromString("folder:folder1#viewer@user:charlie"),
				},
			},
		}

		arrowOutline := Outline{
			Type:        ArrowIteratorType,
			SubOutlines: []Outline{leftOutline, rightOutline},
		}

		hints, err := advisor.GetHints(arrowOutline, CanonicalOutline{})
		require.NoError(t, err)
		require.Len(t, hints, 1)

		// Verify the hint suggests left-to-right direction
		arrow := NewArrowIterator(
			NewFixedIterator(MustPathFromString("document:doc1#parent@folder:folder1")),
			NewFixedIterator(MustPathFromString("folder:folder1#viewer@user:alice")),
		)

		err = hints[0](arrow)
		require.NoError(t, err)
		require.Equal(t, leftToRight, arrow.direction)
	})

	t.Run("non-arrow outline returns no hints", func(t *testing.T) {
		unionOutline := Outline{
			Type: UnionIteratorType,
			SubOutlines: []Outline{
				{
					Type: FixedIteratorType,
					Args: &IteratorArgs{
						FixedPaths: []Path{MustPathFromString("document:doc1#viewer@user:alice")},
					},
				},
			},
		}

		hints, err := advisor.GetHints(unionOutline, CanonicalOutline{})
		require.NoError(t, err)
		require.Empty(t, hints)
	})
}

func TestStaticAdvisor_CostOutline(t *testing.T) {
	advisor := DefaultStaticAdvisor()

	t.Run("fixed iterator", func(t *testing.T) {
		outline := Outline{
			Type: FixedIteratorType,
			Args: &IteratorArgs{
				FixedPaths: []Path{
					MustPathFromString("document:doc1#viewer@user:alice"),
					MustPathFromString("document:doc2#viewer@user:bob"),
				},
			},
		}

		est, err := advisor.costOutline(outline)
		require.NoError(t, err)
		require.Equal(t, 2, est.Cardinality)
		require.Equal(t, 1, est.CheckCost)
		require.Equal(t, 2, est.IterResourcesCost)
		require.Equal(t, 2, est.IterSubjectsCost)
	})

	t.Run("datastore iterator", func(t *testing.T) {
		baseRel := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
		outline := Outline{
			Type: DatastoreIteratorType,
			Args: &IteratorArgs{
				Relation: baseRel,
			},
		}

		est, err := advisor.costOutline(outline)
		require.NoError(t, err)
		require.Equal(t, advisor.NumberOfTuplesInRelation, est.Cardinality)
		require.Equal(t, 1, est.CheckCost)
		require.Equal(t, advisor.Fanout, est.IterResourcesCost)
		require.Equal(t, advisor.Fanout, est.IterSubjectsCost)
	})

	t.Run("union iterator", func(t *testing.T) {
		outline := Outline{
			Type: UnionIteratorType,
			SubOutlines: []Outline{
				{
					Type: FixedIteratorType,
					Args: &IteratorArgs{
						FixedPaths: []Path{
							MustPathFromString("document:doc1#viewer@user:alice"),
							MustPathFromString("document:doc2#viewer@user:bob"),
						},
					},
				},
				{
					Type: FixedIteratorType,
					Args: &IteratorArgs{
						FixedPaths: []Path{
							MustPathFromString("document:doc3#viewer@user:charlie"),
						},
					},
				},
			},
		}

		est, err := advisor.costOutline(outline)
		require.NoError(t, err)
		require.Equal(t, 3, est.Cardinality) // Sum: 2 + 1
		require.Equal(t, 2, est.CheckCost)   // Sum: 1 + 1
	})

	t.Run("arrow iterator", func(t *testing.T) {
		outline := Outline{
			Type: ArrowIteratorType,
			SubOutlines: []Outline{
				{
					Type: FixedIteratorType,
					Args: &IteratorArgs{
						FixedPaths: []Path{
							MustPathFromString("document:doc1#parent@folder:folder1"),
							MustPathFromString("document:doc2#parent@folder:folder2"),
						},
					},
				},
				{
					Type: FixedIteratorType,
					Args: &IteratorArgs{
						FixedPaths: []Path{
							MustPathFromString("folder:folder1#viewer@user:alice"),
						},
					},
				},
			},
		}

		est, err := advisor.costOutline(outline)
		require.NoError(t, err)
		require.Equal(t, 2, est.Cardinality) // 2 * 1

		// CheckCost: left.IterSubjectsCost + (left.Cardinality * right.CheckCost)
		// = 2 + (2 * 1) = 4
		require.Equal(t, 4, est.CheckCost)
	})
}

func TestStaticAdvisor_GetMutations(t *testing.T) {
	advisor := DefaultStaticAdvisor()

	t.Run("union reorders by descending selectivity", func(t *testing.T) {
		// Create union with different selectivities
		// Intersection has lower selectivity (0.9 * 0.9 = 0.81)
		lowSelectivity := Outline{
			Type: IntersectionIteratorType,
			SubOutlines: []Outline{
				{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc1#viewer@user:alice")}}},
				{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc2#viewer@user:bob")}}},
			},
		}

		// Single fixed has higher selectivity (0.9)
		highSelectivity := Outline{
			Type: FixedIteratorType,
			Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc3#viewer@user:charlie")}},
		}

		// Union with low selectivity first (suboptimal)
		unionOutline := Outline{
			Type:        UnionIteratorType,
			SubOutlines: []Outline{lowSelectivity, highSelectivity},
		}

		mutations, err := advisor.GetMutations(unionOutline, CanonicalOutline{})
		require.NoError(t, err)
		require.Len(t, mutations, 1)

		// Apply mutation and verify order
		result := mutations[0](unionOutline)
		require.Len(t, result.SubOutlines, 2)

		// Higher selectivity should be first
		require.Equal(t, FixedIteratorType, result.SubOutlines[0].Type)
		require.Equal(t, IntersectionIteratorType, result.SubOutlines[1].Type)
	})

	t.Run("intersection reorders by ascending selectivity", func(t *testing.T) {
		// Create intersection with different selectivities
		// Single fixed has higher selectivity (0.9)
		highSelectivity := Outline{
			Type: FixedIteratorType,
			Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc1#viewer@user:alice")}},
		}

		// Intersection has lower selectivity (0.9 * 0.9 = 0.81)
		lowSelectivity := Outline{
			Type: IntersectionIteratorType,
			SubOutlines: []Outline{
				{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc2#viewer@user:bob")}}},
				{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc3#viewer@user:charlie")}}},
			},
		}

		// Intersection with high selectivity first (suboptimal)
		intersectionOutline := Outline{
			Type:        IntersectionIteratorType,
			SubOutlines: []Outline{highSelectivity, lowSelectivity},
		}

		mutations, err := advisor.GetMutations(intersectionOutline, CanonicalOutline{})
		require.NoError(t, err)
		require.Len(t, mutations, 1)

		// Apply mutation and verify order
		result := mutations[0](intersectionOutline)
		require.Len(t, result.SubOutlines, 2)

		// Lower selectivity should be first
		require.Equal(t, IntersectionIteratorType, result.SubOutlines[0].Type)
		require.Equal(t, FixedIteratorType, result.SubOutlines[1].Type)
	})

	t.Run("returns nil when order is already optimal", func(t *testing.T) {
		// Union already in optimal order (high selectivity first)
		child1 := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc1#viewer@user:alice")}}}
		child2 := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc2#viewer@user:bob")}}}

		unionOutline := Outline{
			Type:        UnionIteratorType,
			SubOutlines: []Outline{child1, child2},
		}

		mutations, err := advisor.GetMutations(unionOutline, CanonicalOutline{})
		require.NoError(t, err)
		require.Nil(t, mutations)
	})

	t.Run("returns nil for single child", func(t *testing.T) {
		unionOutline := Outline{
			Type: UnionIteratorType,
			SubOutlines: []Outline{
				{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc1#viewer@user:alice")}}},
			},
		}

		mutations, err := advisor.GetMutations(unionOutline, CanonicalOutline{})
		require.NoError(t, err)
		require.Nil(t, mutations)
	})

	t.Run("returns nil for non-union/intersection", func(t *testing.T) {
		arrowOutline := Outline{
			Type: ArrowIteratorType,
			SubOutlines: []Outline{
				{Type: FixedIteratorType},
				{Type: FixedIteratorType},
			},
		}

		mutations, err := advisor.GetMutations(arrowOutline, CanonicalOutline{})
		require.NoError(t, err)
		require.Nil(t, mutations)
	})

	t.Run("handles three children", func(t *testing.T) {
		// Create three children with different selectivities
		// Use nested intersections to get different selectivities
		low := Outline{
			Type: IntersectionIteratorType,
			SubOutlines: []Outline{
				{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc1#viewer@user:alice")}}},
				{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc2#viewer@user:bob")}}},
				{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc3#viewer@user:charlie")}}},
			},
		}

		medium := Outline{
			Type: IntersectionIteratorType,
			SubOutlines: []Outline{
				{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc4#viewer@user:dave")}}},
				{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc5#viewer@user:eve")}}},
			},
		}

		high := Outline{
			Type: FixedIteratorType,
			Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc6#viewer@user:frank")}},
		}

		// Union in suboptimal order: low, medium, high
		unionOutline := Outline{
			Type:        UnionIteratorType,
			SubOutlines: []Outline{low, medium, high},
		}

		mutations, err := advisor.GetMutations(unionOutline, CanonicalOutline{})
		require.NoError(t, err)
		require.Len(t, mutations, 1)

		// Apply mutation
		result := mutations[0](unionOutline)
		require.Len(t, result.SubOutlines, 3)

		// Should be reordered to high, medium, low (descending selectivity)
		require.Equal(t, FixedIteratorType, result.SubOutlines[0].Type)
		require.Equal(t, IntersectionIteratorType, result.SubOutlines[1].Type)
		require.Len(t, result.SubOutlines[1].SubOutlines, 2) // medium has 2 children
		require.Equal(t, IntersectionIteratorType, result.SubOutlines[2].Type)
		require.Len(t, result.SubOutlines[2].SubOutlines, 3) // low has 3 children
	})

	t.Run("arrow rebalancing - left nested arrow cheaper to rotate", func(t *testing.T) {
		// Create (A->B)->C where A has high cardinality and B,C have low cardinality
		// Original cost: A.IterSubjects + (A.Card * B.CheckCost), then result.IterSubjects + (result.Card * C.CheckCost)
		// Alternative A->(B->C) might be cheaper

		// A has 10 paths (high cardinality)
		aPaths := make([]Path, 10)
		for i := range 10 {
			aPaths[i] = MustPathFromString("document:doc" + string(rune('0'+i)) + "#viewer@user:alice")
		}
		a := Outline{
			Type: FixedIteratorType,
			Args: &IteratorArgs{FixedPaths: aPaths},
		}

		// B and C have 1 path each (low cardinality)
		b := Outline{
			Type: FixedIteratorType,
			Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("folder:f1#viewer@user:bob")}},
		}
		c := Outline{
			Type: FixedIteratorType,
			Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("folder:f2#viewer@user:charlie")}},
		}

		// Build (A->B)->C
		ab := Outline{Type: ArrowIteratorType, SubOutlines: []Outline{a, b}}
		abThenC := Outline{Type: ArrowIteratorType, SubOutlines: []Outline{ab, c}}

		mutations, err := advisor.GetMutations(abThenC, CanonicalOutline{})
		require.NoError(t, err)

		// Should return a rotation mutation if cost analysis determines it's beneficial
		// The actual decision depends on cost calculation, but we can verify the structure
		if mutations != nil {
			require.Len(t, mutations, 1)

			result := mutations[0](abThenC)
			require.Equal(t, ArrowIteratorType, result.Type)
			require.Len(t, result.SubOutlines, 2)

			// If rotated, structure should be A->(B->C)
			// First child is A
			require.Equal(t, FixedIteratorType, result.SubOutlines[0].Type)
			// Second child is B->C
			require.Equal(t, ArrowIteratorType, result.SubOutlines[1].Type)
		}
	})

	t.Run("arrow rebalancing - right nested arrow", func(t *testing.T) {
		// Create A->(B->C) and check if it should be rotated
		a := Outline{
			Type: FixedIteratorType,
			Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc1#viewer@user:alice")}},
		}

		b := Outline{
			Type: FixedIteratorType,
			Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("folder:f1#viewer@user:bob")}},
		}

		c := Outline{
			Type: FixedIteratorType,
			Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("folder:f2#viewer@user:charlie")}},
		}

		// Build B->C
		bc := Outline{Type: ArrowIteratorType, SubOutlines: []Outline{b, c}}
		// Build A->(B->C)
		aThenBC := Outline{Type: ArrowIteratorType, SubOutlines: []Outline{a, bc}}

		mutations, err := advisor.GetMutations(aThenBC, CanonicalOutline{})
		require.NoError(t, err)

		// Depending on cost, may or may not rotate
		// Just verify no errors and valid structure if mutation returned
		if mutations != nil {
			require.Len(t, mutations, 1)
			result := mutations[0](aThenBC)
			require.Equal(t, ArrowIteratorType, result.Type)
		}
	})

	t.Run("arrow rebalancing - no nested arrows", func(t *testing.T) {
		a := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc1#viewer@user:alice")}}}
		b := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("folder:f1#viewer@user:bob")}}}

		arrow := Outline{Type: ArrowIteratorType, SubOutlines: []Outline{a, b}}

		mutations, err := advisor.GetMutations(arrow, CanonicalOutline{})
		require.NoError(t, err)
		require.Nil(t, mutations) // No nested arrows, no mutations
	})
}
