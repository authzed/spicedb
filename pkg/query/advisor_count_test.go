package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// keySourceFromMap is a minimal CanonicalKeySource for tests, backed by an
// explicit map from OutlineNodeID to CanonicalKey.
type keySourceFromMap map[OutlineNodeID]CanonicalKey

func (m keySourceFromMap) GetCanonicalKey(id OutlineNodeID) CanonicalKey {
	return m[id]
}

// applyDirectionHint applies the first hint in hints to a freshly-created
// ArrowIterator and returns the resulting direction.
func applyDirectionHint(t *testing.T, hints []Hint) arrowDirection {
	t.Helper()
	require.Len(t, hints, 1)
	arrow := NewArrowIterator(
		NewFixedIterator(MustPathFromString("document:doc1#parent@folder:folder1")),
		NewFixedIterator(MustPathFromString("folder:folder1#viewer@user:alice")),
	)
	require.NoError(t, hints[0](arrow))
	return arrow.direction
}

func TestCountAdvisor_GetHints_NonArrow(t *testing.T) {
	advisor := NewCountAdvisor(nil)
	outline := Outline{Type: UnionIteratorType}

	hints, err := advisor.GetHints(outline, CanonicalOutline{})
	require.NoError(t, err)
	require.Nil(t, hints)
}

func TestCountAdvisor_GetHints_NoData(t *testing.T) {
	// With no stats at all, both sides default to defaultArrowFanout.
	// rightFanout == leftFanout, so the condition rightFanout < leftFanout is false
	// and we expect leftToRight (keep default).
	advisor := NewCountAdvisor(map[CanonicalKey]CountStats{})

	left := Outline{Type: FixedIteratorType, ID: 1}
	right := Outline{Type: FixedIteratorType, ID: 2}
	arrow := Outline{Type: ArrowIteratorType, SubOutlines: []Outline{left, right}, ID: 3}

	ks := keySourceFromMap{1: "left", 2: "right", 3: "arrow"}

	hints, err := advisor.GetHints(arrow, ks)
	require.NoError(t, err)
	require.Equal(t, leftToRight, applyDirectionHint(t, hints))
}

func TestCountAdvisor_GetHints_LeftHighFanout(t *testing.T) {
	// Left fanout = 10/1 = 10, right fanout = 2/1 = 2.
	// rightFanout (2) < leftFanout (10) → reverse.
	advisor := NewCountAdvisor(map[CanonicalKey]CountStats{
		"left":  {IterSubjectsCalls: 1, IterSubjectsResults: 10},
		"right": {IterResourcesCalls: 1, IterResourcesResults: 2},
	})

	left := Outline{Type: FixedIteratorType, ID: 1}
	right := Outline{Type: FixedIteratorType, ID: 2}
	arrow := Outline{Type: ArrowIteratorType, SubOutlines: []Outline{left, right}, ID: 3}

	ks := keySourceFromMap{1: "left", 2: "right", 3: "arrow"}

	hints, err := advisor.GetHints(arrow, ks)
	require.NoError(t, err)
	require.Equal(t, rightToLeft, applyDirectionHint(t, hints))
}

func TestCountAdvisor_GetHints_RightHighFanout(t *testing.T) {
	// Left fanout = 2/1 = 2, right fanout = 10/1 = 10.
	// rightFanout (10) > leftFanout (2) → keep leftToRight.
	advisor := NewCountAdvisor(map[CanonicalKey]CountStats{
		"left":  {IterSubjectsCalls: 1, IterSubjectsResults: 2},
		"right": {IterResourcesCalls: 1, IterResourcesResults: 10},
	})

	left := Outline{Type: FixedIteratorType, ID: 1}
	right := Outline{Type: FixedIteratorType, ID: 2}
	arrow := Outline{Type: ArrowIteratorType, SubOutlines: []Outline{left, right}, ID: 3}

	ks := keySourceFromMap{1: "left", 2: "right", 3: "arrow"}

	hints, err := advisor.GetHints(arrow, ks)
	require.NoError(t, err)
	require.Equal(t, leftToRight, applyDirectionHint(t, hints))
}

func TestCountAdvisor_GetHints_EqualFanout(t *testing.T) {
	// Both fanouts equal (5/1 = 5 each) → not strictly less → leftToRight.
	advisor := NewCountAdvisor(map[CanonicalKey]CountStats{
		"left":  {IterSubjectsCalls: 1, IterSubjectsResults: 5},
		"right": {IterResourcesCalls: 1, IterResourcesResults: 5},
	})

	left := Outline{Type: FixedIteratorType, ID: 1}
	right := Outline{Type: FixedIteratorType, ID: 2}
	arrow := Outline{Type: ArrowIteratorType, SubOutlines: []Outline{left, right}, ID: 3}

	ks := keySourceFromMap{1: "left", 2: "right", 3: "arrow"}

	hints, err := advisor.GetHints(arrow, ks)
	require.NoError(t, err)
	require.Equal(t, leftToRight, applyDirectionHint(t, hints))
}

func TestCountAdvisor_GetHints_OnlyLeftData(t *testing.T) {
	// Left fanout = 10/1 = 10; right has no data → defaults to defaultArrowFanout (3).
	// rightFanout (3) < leftFanout (10) → reverse.
	advisor := NewCountAdvisor(map[CanonicalKey]CountStats{
		"left": {IterSubjectsCalls: 1, IterSubjectsResults: 10},
	})

	left := Outline{Type: FixedIteratorType, ID: 1}
	right := Outline{Type: FixedIteratorType, ID: 2}
	arrow := Outline{Type: ArrowIteratorType, SubOutlines: []Outline{left, right}, ID: 3}

	ks := keySourceFromMap{1: "left", 2: "right", 3: "arrow"}

	hints, err := advisor.GetHints(arrow, ks)
	require.NoError(t, err)
	require.Equal(t, rightToLeft, applyDirectionHint(t, hints))
}

func TestCountAdvisor_GetHints_OnlyRightData(t *testing.T) {
	// Right fanout = 10/1 = 10; left has no data → defaults to defaultArrowFanout (3).
	// rightFanout (10) > leftFanout (3) → leftToRight.
	advisor := NewCountAdvisor(map[CanonicalKey]CountStats{
		"right": {IterResourcesCalls: 1, IterResourcesResults: 10},
	})

	left := Outline{Type: FixedIteratorType, ID: 1}
	right := Outline{Type: FixedIteratorType, ID: 2}
	arrow := Outline{Type: ArrowIteratorType, SubOutlines: []Outline{left, right}, ID: 3}

	ks := keySourceFromMap{1: "left", 2: "right", 3: "arrow"}

	hints, err := advisor.GetHints(arrow, ks)
	require.NoError(t, err)
	require.Equal(t, leftToRight, applyDirectionHint(t, hints))
}

func TestCountAdvisor_GetMutations_AlwaysNil(t *testing.T) {
	advisor := NewCountAdvisor(nil)

	for _, typ := range []IteratorType{
		ArrowIteratorType, UnionIteratorType, IntersectionIteratorType,
		DatastoreIteratorType, FixedIteratorType,
	} {
		mutations, err := advisor.GetMutations(Outline{Type: typ}, CanonicalOutline{})
		require.NoError(t, err)
		require.Nil(t, mutations)
	}
}
