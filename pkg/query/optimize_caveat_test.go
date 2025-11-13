package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema/v2"
)

// createTestCaveatForPushdown creates a test ContextualizedCaveat
func createTestCaveatForPushdown(name string) *core.ContextualizedCaveat {
	return &core.ContextualizedCaveat{
		CaveatName: name,
		Context:    nil,
	}
}

// createTestRelationIterator creates a RelationIterator with a caveat
func createTestRelationIterator(caveatName string) *RelationIterator {
	// Create a BaseRelation with the caveat
	baseRelation := schema.NewTestBaseRelationWithFeatures("document", "viewer", "user", "", caveatName, false)
	return NewRelationIterator(baseRelation)
}

// createTestRelationIteratorNoCaveat creates a RelationIterator without a caveat
func createTestRelationIteratorNoCaveat() *RelationIterator {
	baseRelation := schema.NewTestBaseRelationWithFeatures("document", "viewer", "user", "", "", false)
	return NewRelationIterator(baseRelation)
}

func TestPushdownCaveatEvaluation(t *testing.T) {
	t.Parallel()

	t.Run("pushes caveat through union when both sides have caveat", func(t *testing.T) {
		t.Parallel()

		caveat := createTestCaveatForPushdown("test_caveat")

		// Create Union[Relation(with caveat), Relation(with caveat)]
		rel1 := createTestRelationIterator("test_caveat")
		rel2 := createTestRelationIterator("test_caveat")
		union := NewUnion(rel1, rel2)

		// Wrap in caveat: Caveat(Union[Rel1, Rel2])
		caveatIterator := NewCaveatIterator(union, caveat)

		// Apply optimization
		result, changed, err := ApplyOptimizations(caveatIterator, []OptimizerFunc{
			WrapOptimizer[*CaveatIterator](PushdownCaveatEvaluation),
		})
		require.NoError(t, err)
		require.True(t, changed)

		// Should become Union[Caveat(Rel1), Caveat(Rel2)]
		resultUnion, ok := result.(*Union)
		require.True(t, ok, "Expected result to be a Union")
		require.Len(t, resultUnion.subIts, 2)

		// Both should be wrapped in caveats
		_, ok1 := resultUnion.subIts[0].(*CaveatIterator)
		_, ok2 := resultUnion.subIts[1].(*CaveatIterator)
		require.True(t, ok1, "First subiterator should be a CaveatIterator")
		require.True(t, ok2, "Second subiterator should be a CaveatIterator")
	})

	t.Run("pushes caveat through union only on side with caveat", func(t *testing.T) {
		t.Parallel()

		caveat := createTestCaveatForPushdown("test_caveat")

		// Create Union[Relation(with caveat), Relation(no caveat)]
		rel1 := createTestRelationIterator("test_caveat")
		rel2 := createTestRelationIteratorNoCaveat()
		union := NewUnion(rel1, rel2)

		// Wrap in caveat: Caveat(Union[Rel1, Rel2])
		caveatIterator := NewCaveatIterator(union, caveat)

		// Apply optimization
		result, changed, err := ApplyOptimizations(caveatIterator, []OptimizerFunc{
			WrapOptimizer[*CaveatIterator](PushdownCaveatEvaluation),
		})
		require.NoError(t, err)
		require.True(t, changed)

		// Should become Union[Caveat(Rel1), Rel2]
		resultUnion, ok := result.(*Union)
		require.True(t, ok, "Expected result to be a Union")
		require.Len(t, resultUnion.subIts, 2)

		// First should be wrapped, second should not
		caveat1, ok1 := resultUnion.subIts[0].(*CaveatIterator)
		rel2Result, ok2 := resultUnion.subIts[1].(*RelationIterator)
		require.True(t, ok1, "First subiterator should be a CaveatIterator")
		require.True(t, ok2, "Second subiterator should be a RelationIterator (not wrapped)")

		// Verify the caveat wraps the correct relation
		caveat1Sub, ok := caveat1.subiterator.(*RelationIterator)
		require.True(t, ok)
		require.Equal(t, rel1, caveat1Sub)
		require.Equal(t, rel2, rel2Result)
	})

	t.Run("does not push caveat through intersection arrow", func(t *testing.T) {
		t.Parallel()

		caveat := createTestCaveatForPushdown("test_caveat")

		// Create an IntersectionArrow with a relation that has the caveat
		rel := createTestRelationIterator("test_caveat")
		relNoCaveat := createTestRelationIteratorNoCaveat()
		intersectionArrow := NewIntersectionArrow(rel, relNoCaveat)

		// Wrap in caveat
		caveatIterator := NewCaveatIterator(intersectionArrow, caveat)

		// Apply optimization
		result, changed, err := ApplyOptimizations(caveatIterator, []OptimizerFunc{
			WrapOptimizer[*CaveatIterator](PushdownCaveatEvaluation),
		})
		require.NoError(t, err)
		require.False(t, changed, "Should not optimize through IntersectionArrow")

		// Should remain as Caveat(IntersectionArrow)
		resultCaveat, ok := result.(*CaveatIterator)
		require.True(t, ok, "Expected result to still be a CaveatIterator")
		_, ok = resultCaveat.subiterator.(*IntersectionArrow)
		require.True(t, ok, "Subiterator should still be IntersectionArrow")
	})

	t.Run("does not push when no subiterators have caveat", func(t *testing.T) {
		t.Parallel()

		caveat := createTestCaveatForPushdown("test_caveat")

		// Create Union[Relation(no caveat), Relation(no caveat)]
		rel1 := createTestRelationIteratorNoCaveat()
		rel2 := createTestRelationIteratorNoCaveat()
		union := NewUnion(rel1, rel2)

		// Wrap in caveat: Caveat(Union[Rel1, Rel2])
		caveatIterator := NewCaveatIterator(union, caveat)

		// Apply optimization
		result, changed, err := ApplyOptimizations(caveatIterator, []OptimizerFunc{
			WrapOptimizer[*CaveatIterator](PushdownCaveatEvaluation),
		})
		require.NoError(t, err)
		require.False(t, changed)

		// Should remain unchanged
		resultCaveat, ok := result.(*CaveatIterator)
		require.True(t, ok)
		require.Equal(t, caveatIterator, resultCaveat)
	})

	t.Run("does not push through leaf iterator", func(t *testing.T) {
		t.Parallel()

		caveat := createTestCaveatForPushdown("test_caveat")

		// Create Caveat(Relation) - leaf has no subiterators
		rel := createTestRelationIterator("test_caveat")
		caveatIterator := NewCaveatIterator(rel, caveat)

		// Apply optimization
		result, changed, err := ApplyOptimizations(caveatIterator, []OptimizerFunc{
			WrapOptimizer[*CaveatIterator](PushdownCaveatEvaluation),
		})
		require.NoError(t, err)
		require.False(t, changed)

		// Should remain unchanged
		resultCaveat, ok := result.(*CaveatIterator)
		require.True(t, ok)
		require.Equal(t, caveatIterator, resultCaveat)
	})

	t.Run("pushes through nested union", func(t *testing.T) {
		t.Parallel()

		caveat := createTestCaveatForPushdown("test_caveat")

		// Create Caveat(Union[Union[Rel1, Rel2], Rel3])
		rel1 := createTestRelationIterator("test_caveat")
		rel2 := createTestRelationIteratorNoCaveat()
		innerUnion := NewUnion(rel1, rel2)

		rel3 := createTestRelationIterator("test_caveat")
		outerUnion := NewUnion(innerUnion, rel3)

		caveatIterator := NewCaveatIterator(outerUnion, caveat)

		// Apply optimization
		result, changed, err := ApplyOptimizations(caveatIterator, []OptimizerFunc{
			WrapOptimizer[*CaveatIterator](PushdownCaveatEvaluation),
		})
		require.NoError(t, err)
		require.True(t, changed)

		// Due to recursive optimization, this will become:
		// Union[Union[Caveat(Rel1), Rel2], Caveat(Rel3)]
		// The outer caveat pushes down to wrap innerUnion and rel3
		// Then the caveat on innerUnion recursively pushes down to only wrap rel1
		resultUnion, ok := result.(*Union)
		require.True(t, ok)
		require.Len(t, resultUnion.subIts, 2)

		// First should be Union[Caveat(Rel1), Rel2] (caveat pushed down further)
		innerResultUnion, ok1 := resultUnion.subIts[0].(*Union)
		require.True(t, ok1, "First subiterator should be a Union (caveat pushed down)")
		require.Len(t, innerResultUnion.subIts, 2)
		_, ok = innerResultUnion.subIts[0].(*CaveatIterator)
		require.True(t, ok, "First element of inner union should be Caveat(Rel1)")
		_, ok = innerResultUnion.subIts[1].(*RelationIterator)
		require.True(t, ok, "Second element of inner union should be Rel2 (no caveat)")

		// Second should be Caveat(Rel3)
		caveat2, ok2 := resultUnion.subIts[1].(*CaveatIterator)
		require.True(t, ok2)
		_, ok = caveat2.subiterator.(*RelationIterator)
		require.True(t, ok, "Second subiterator should be Caveat(Relation)")
	})

	t.Run("works with intersection of relations", func(t *testing.T) {
		t.Parallel()

		caveat := createTestCaveatForPushdown("test_caveat")

		// Create Caveat(Intersection[Rel1(with caveat), Rel2(no caveat)])
		rel1 := createTestRelationIterator("test_caveat")
		rel2 := createTestRelationIteratorNoCaveat()
		intersection := NewIntersection(rel1, rel2)

		caveatIterator := NewCaveatIterator(intersection, caveat)

		// Apply optimization
		result, changed, err := ApplyOptimizations(caveatIterator, []OptimizerFunc{
			WrapOptimizer[*CaveatIterator](PushdownCaveatEvaluation),
		})
		require.NoError(t, err)
		require.True(t, changed)

		// Should become Intersection[Caveat(Rel1), Rel2]
		resultIntersection, ok := result.(*Intersection)
		require.True(t, ok)
		require.Len(t, resultIntersection.subIts, 2)

		// First should be wrapped, second should not
		_, ok1 := resultIntersection.subIts[0].(*CaveatIterator)
		_, ok2 := resultIntersection.subIts[1].(*RelationIterator)
		require.True(t, ok1, "First subiterator should be a CaveatIterator")
		require.True(t, ok2, "Second subiterator should be a RelationIterator")
	})
}

func TestContainsCaveat(t *testing.T) {
	t.Parallel()

	caveat := createTestCaveatForPushdown("test_caveat")

	t.Run("detects caveat in relation iterator", func(t *testing.T) {
		t.Parallel()

		rel := createTestRelationIterator("test_caveat")
		require.True(t, containsCaveat(rel, caveat))
	})

	t.Run("does not detect when caveat name differs", func(t *testing.T) {
		t.Parallel()

		rel := createTestRelationIterator("other_caveat")
		require.False(t, containsCaveat(rel, caveat))
	})

	t.Run("does not detect when no caveat", func(t *testing.T) {
		t.Parallel()

		rel := createTestRelationIteratorNoCaveat()
		require.False(t, containsCaveat(rel, caveat))
	})

	t.Run("detects caveat in nested structure", func(t *testing.T) {
		t.Parallel()

		rel1 := createTestRelationIteratorNoCaveat()
		rel2 := createTestRelationIterator("test_caveat")
		union := NewUnion(rel1, rel2)

		require.True(t, containsCaveat(union, caveat))
	})

	t.Run("does not detect caveat in structure without it", func(t *testing.T) {
		t.Parallel()

		rel1 := createTestRelationIteratorNoCaveat()
		rel2 := createTestRelationIteratorNoCaveat()
		union := NewUnion(rel1, rel2)

		require.False(t, containsCaveat(union, caveat))
	})

	t.Run("handles nil caveat in relationContainsCaveat", func(t *testing.T) {
		t.Parallel()

		rel := createTestRelationIterator("test_caveat")
		require.False(t, relationContainsCaveat(rel, nil))
	})

	t.Run("handles relation with nil base in relationContainsCaveat", func(t *testing.T) {
		t.Parallel()

		caveat := createTestCaveatForPushdown("test_caveat")
		// Create a RelationIterator with nil base
		rel := &RelationIterator{base: nil}
		require.False(t, relationContainsCaveat(rel, caveat))
	})
}

func TestPushdownCaveatEvaluationEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("does not push through nested CaveatIterator", func(t *testing.T) {
		t.Parallel()

		caveat := createTestCaveatForPushdown("test_caveat")

		// Create Caveat(Caveat(Relation))
		rel := createTestRelationIterator("test_caveat")
		innerCaveat := NewCaveatIterator(rel, caveat)
		outerCaveat := NewCaveatIterator(innerCaveat, caveat)

		// Apply optimization
		result, changed, err := ApplyOptimizations(outerCaveat, []OptimizerFunc{
			WrapOptimizer[*CaveatIterator](PushdownCaveatEvaluation),
		})
		require.NoError(t, err)
		require.False(t, changed, "Should not push through nested CaveatIterator to prevent infinite recursion")

		// Should remain unchanged
		resultCaveat, ok := result.(*CaveatIterator)
		require.True(t, ok)
		_, ok = resultCaveat.subiterator.(*CaveatIterator)
		require.True(t, ok, "Subiterator should still be a CaveatIterator")
	})
}
