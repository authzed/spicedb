package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestIsNull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		outline  Outline
		expected bool
	}{
		{
			name:     "NullIteratorType is null",
			outline:  Outline{Type: NullIteratorType},
			expected: true,
		},
		{
			name: "FixedIteratorType with no paths is null",
			outline: Outline{
				Type: FixedIteratorType,
				Args: &IteratorArgs{FixedPaths: []Path{}},
			},
			expected: true,
		},
		{
			name: "FixedIteratorType with nil Args is null",
			outline: Outline{
				Type: FixedIteratorType,
				Args: nil,
			},
			expected: true,
		},
		{
			name: "FixedIteratorType with paths is not null",
			outline: Outline{
				Type: FixedIteratorType,
				Args: &IteratorArgs{FixedPaths: []Path{{}}},
			},
			expected: false,
		},
		{
			name:     "DatastoreIteratorType is not null",
			outline:  Outline{Type: DatastoreIteratorType},
			expected: false,
		},
		{
			name:     "UnionIteratorType is not null",
			outline:  Outline{Type: UnionIteratorType},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := isNullOutline(tt.outline)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestReplaceEmptyComposites(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		outline  Outline
		expected Outline
	}{
		{
			name: "empty Union becomes Null",
			outline: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{},
			},
			expected: Outline{Type: NullIteratorType},
		},
		{
			name: "empty Intersection becomes Null",
			outline: Outline{
				Type:        IntersectionIteratorType,
				SubOutlines: []Outline{},
			},
			expected: Outline{Type: NullIteratorType},
		},
		{
			name: "Union with children unchanged",
			outline: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					{Type: DatastoreIteratorType},
				},
			},
			expected: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					{Type: DatastoreIteratorType},
				},
			},
		},
		{
			name:     "non-composite unchanged",
			outline:  Outline{Type: DatastoreIteratorType},
			expected: Outline{Type: DatastoreIteratorType},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := replaceEmptyComposites(tt.outline)
			require.True(t, result.Equals(tt.expected), "expected %+v, got %+v", tt.expected, result)
		})
	}
}

func TestCollapseSingleChild(t *testing.T) {
	t.Parallel()

	childOutline := Outline{Type: DatastoreIteratorType}

	tests := []struct {
		name     string
		outline  Outline
		expected Outline
	}{
		{
			name: "Union with single child collapses",
			outline: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{childOutline},
			},
			expected: childOutline,
		},
		{
			name: "Intersection with single child collapses",
			outline: Outline{
				Type:        IntersectionIteratorType,
				SubOutlines: []Outline{childOutline},
			},
			expected: childOutline,
		},
		{
			name: "Union with multiple children unchanged",
			outline: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					{Type: DatastoreIteratorType},
					{Type: DatastoreIteratorType},
				},
			},
			expected: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					{Type: DatastoreIteratorType},
					{Type: DatastoreIteratorType},
				},
			},
		},
		{
			name:     "non-composite unchanged",
			outline:  Outline{Type: DatastoreIteratorType},
			expected: Outline{Type: DatastoreIteratorType},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := collapseSingleChild(tt.outline)
			require.True(t, result.Equals(tt.expected))
		})
	}
}

func TestPropagateNull(t *testing.T) {
	t.Parallel()

	nullOutline := Outline{Type: NullIteratorType}
	dataOutline := Outline{Type: DatastoreIteratorType}

	tests := []struct {
		name     string
		outline  Outline
		expected Outline
	}{
		{
			name: "Intersection with null becomes null",
			outline: Outline{
				Type: IntersectionIteratorType,
				SubOutlines: []Outline{
					dataOutline,
					nullOutline,
				},
			},
			expected: nullOutline,
		},
		{
			name: "Intersection with all nulls becomes null",
			outline: Outline{
				Type: IntersectionIteratorType,
				SubOutlines: []Outline{
					nullOutline,
					nullOutline,
				},
			},
			expected: nullOutline,
		},
		{
			name: "Intersection with no nulls unchanged",
			outline: Outline{
				Type: IntersectionIteratorType,
				SubOutlines: []Outline{
					dataOutline,
					dataOutline,
				},
			},
			expected: Outline{
				Type: IntersectionIteratorType,
				SubOutlines: []Outline{
					dataOutline,
					dataOutline,
				},
			},
		},
		{
			name: "Union with all nulls becomes null",
			outline: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					nullOutline,
					nullOutline,
				},
			},
			expected: nullOutline,
		},
		{
			name: "Union with mixed nulls filters out nulls",
			outline: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					nullOutline,
					dataOutline,
					nullOutline,
				},
			},
			expected: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					dataOutline,
				},
			},
		},
		{
			name: "Union with no nulls unchanged",
			outline: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					dataOutline,
					dataOutline,
				},
			},
			expected: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					dataOutline,
					dataOutline,
				},
			},
		},
		{
			name:     "non-composite unchanged",
			outline:  dataOutline,
			expected: dataOutline,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := propagateNull(tt.outline)
			require.True(t, result.Equals(tt.expected))
		})
	}
}

func TestFlattenComposites(t *testing.T) {
	t.Parallel()

	a := Outline{Type: DatastoreIteratorType, Args: &IteratorArgs{RelationName: "a"}}
	b := Outline{Type: DatastoreIteratorType, Args: &IteratorArgs{RelationName: "b"}}
	c := Outline{Type: DatastoreIteratorType, Args: &IteratorArgs{RelationName: "c"}}
	d := Outline{Type: DatastoreIteratorType, Args: &IteratorArgs{RelationName: "d"}}

	tests := []struct {
		name     string
		outline  Outline
		expected Outline
	}{
		{
			name: "Union[Union[A,B],C] flattens to Union[A,B,C]",
			outline: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					{
						Type:        UnionIteratorType,
						SubOutlines: []Outline{a, b},
					},
					c,
				},
			},
			expected: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{a, b, c},
			},
		},
		{
			name: "Union[Union[A,B],Union[C,D]] flattens to Union[A,B,C,D]",
			outline: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					{
						Type:        UnionIteratorType,
						SubOutlines: []Outline{a, b},
					},
					{
						Type:        UnionIteratorType,
						SubOutlines: []Outline{c, d},
					},
				},
			},
			expected: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{a, b, c, d},
			},
		},
		{
			name: "Intersection[Intersection[A,B],C] flattens",
			outline: Outline{
				Type: IntersectionIteratorType,
				SubOutlines: []Outline{
					{
						Type:        IntersectionIteratorType,
						SubOutlines: []Outline{a, b},
					},
					c,
				},
			},
			expected: Outline{
				Type:        IntersectionIteratorType,
				SubOutlines: []Outline{a, b, c},
			},
		},
		{
			name: "Union[Intersection[A,B],C] does not flatten",
			outline: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					{
						Type:        IntersectionIteratorType,
						SubOutlines: []Outline{a, b},
					},
					c,
				},
			},
			expected: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					{
						Type:        IntersectionIteratorType,
						SubOutlines: []Outline{a, b},
					},
					c,
				},
			},
		},
		{
			name: "Union[A,B] with no nesting unchanged",
			outline: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{a, b},
			},
			expected: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{a, b},
			},
		},
		{
			name:     "non-composite unchanged",
			outline:  a,
			expected: a,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := flattenComposites(tt.outline)
			require.True(t, result.Equals(tt.expected))
		})
	}
}

func TestSortCompositeChildren(t *testing.T) {
	t.Parallel()

	// Create outlines with different relation names for predictable sorting
	a := Outline{Type: DatastoreIteratorType, Args: &IteratorArgs{RelationName: "a"}}
	b := Outline{Type: DatastoreIteratorType, Args: &IteratorArgs{RelationName: "b"}}
	c := Outline{Type: DatastoreIteratorType, Args: &IteratorArgs{RelationName: "c"}}

	tests := []struct {
		name     string
		outline  Outline
		expected Outline
	}{
		{
			name: "Union children sorted",
			outline: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{c, a, b},
			},
			expected: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{a, b, c},
			},
		},
		{
			name: "Intersection children sorted",
			outline: Outline{
				Type:        IntersectionIteratorType,
				SubOutlines: []Outline{b, c, a},
			},
			expected: Outline{
				Type:        IntersectionIteratorType,
				SubOutlines: []Outline{a, b, c},
			},
		},
		{
			name: "single child unchanged",
			outline: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{a},
			},
			expected: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{a},
			},
		},
		{
			name:     "non-composite unchanged",
			outline:  a,
			expected: a,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := sortCompositeChildren(tt.outline)
			require.True(t, result.Equals(tt.expected))
		})
	}
}

func TestExtractCaveats(t *testing.T) {
	t.Parallel()

	caveat1 := &core.ContextualizedCaveat{CaveatName: "caveat1"}
	caveat2 := &core.ContextualizedCaveat{CaveatName: "caveat2"}
	dataOutline := Outline{Type: DatastoreIteratorType}

	tests := []struct {
		name            string
		outline         Outline
		expectedTree    Outline
		expectedCaveats []*core.ContextualizedCaveat
	}{
		{
			name: "single caveat extracted",
			outline: Outline{
				Type:        CaveatIteratorType,
				Args:        &IteratorArgs{Caveat: caveat1},
				SubOutlines: []Outline{dataOutline},
			},
			expectedTree:    dataOutline,
			expectedCaveats: []*core.ContextualizedCaveat{caveat1},
		},
		{
			name: "nested caveats extracted",
			outline: Outline{
				Type: CaveatIteratorType,
				Args: &IteratorArgs{Caveat: caveat1},
				SubOutlines: []Outline{
					{
						Type:        CaveatIteratorType,
						Args:        &IteratorArgs{Caveat: caveat2},
						SubOutlines: []Outline{dataOutline},
					},
				},
			},
			expectedTree:    dataOutline,
			expectedCaveats: []*core.ContextualizedCaveat{caveat2, caveat1},
		},
		{
			name: "caveats in union children extracted",
			outline: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					{
						Type:        CaveatIteratorType,
						Args:        &IteratorArgs{Caveat: caveat1},
						SubOutlines: []Outline{dataOutline},
					},
					{
						Type:        CaveatIteratorType,
						Args:        &IteratorArgs{Caveat: caveat2},
						SubOutlines: []Outline{dataOutline},
					},
				},
			},
			expectedTree: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{dataOutline, dataOutline},
			},
			expectedCaveats: []*core.ContextualizedCaveat{caveat1, caveat2},
		},
		{
			name:            "no caveats",
			outline:         dataOutline,
			expectedTree:    dataOutline,
			expectedCaveats: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tree, caveats, err := extractCaveats(tt.outline)
			require.NoError(t, err)
			require.True(t, tree.Equals(tt.expectedTree))
			require.Len(t, caveats, len(tt.expectedCaveats))
			for i, expectedCaveat := range tt.expectedCaveats {
				require.True(t, expectedCaveat.EqualVT(caveats[i]))
			}
		})
	}
}

func TestNestCaveats(t *testing.T) {
	t.Parallel()

	caveat1 := &core.ContextualizedCaveat{CaveatName: "caveat1"}
	caveat2 := &core.ContextualizedCaveat{CaveatName: "caveat2"}
	dataOutline := Outline{Type: DatastoreIteratorType}

	tests := []struct {
		name     string
		outline  Outline
		caveats  []*core.ContextualizedCaveat
		expected Outline
	}{
		{
			name:     "no caveats",
			outline:  dataOutline,
			caveats:  nil,
			expected: dataOutline,
		},
		{
			name:    "single caveat",
			outline: dataOutline,
			caveats: []*core.ContextualizedCaveat{caveat1},
			expected: Outline{
				Type:        CaveatIteratorType,
				Args:        &IteratorArgs{Caveat: caveat1},
				SubOutlines: []Outline{dataOutline},
			},
		},
		{
			name:    "two caveats nested correctly",
			outline: dataOutline,
			caveats: []*core.ContextualizedCaveat{caveat1, caveat2},
			expected: Outline{
				Type: CaveatIteratorType,
				Args: &IteratorArgs{Caveat: caveat1},
				SubOutlines: []Outline{
					{
						Type:        CaveatIteratorType,
						Args:        &IteratorArgs{Caveat: caveat2},
						SubOutlines: []Outline{dataOutline},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := nestCaveats(tt.outline, tt.caveats)
			require.True(t, result.Equals(tt.expected))
		})
	}
}

func TestCanonicalizeOutline(t *testing.T) {
	t.Parallel()

	caveat1 := &core.ContextualizedCaveat{CaveatName: "caveat1"}
	caveat2 := &core.ContextualizedCaveat{CaveatName: "caveat2"}

	// Create test relations for consistent ordering
	rel1 := schema.NewTestBaseRelation("doc", "rel1", "user", tuple.Ellipsis)
	rel2 := schema.NewTestBaseRelation("doc", "rel2", "user", tuple.Ellipsis)

	a := Outline{Type: DatastoreIteratorType, Args: &IteratorArgs{Relation: rel1}}
	b := Outline{Type: DatastoreIteratorType, Args: &IteratorArgs{Relation: rel2}}

	tests := []struct {
		name     string
		outline  Outline
		expected Outline
	}{
		{
			name: "empty union becomes null",
			outline: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{},
			},
			expected: Outline{Type: NullIteratorType},
		},
		{
			name: "single child union collapses",
			outline: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{a},
			},
			expected: a,
		},
		{
			name: "union with null removed",
			outline: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					{Type: NullIteratorType},
					a,
				},
			},
			expected: a,
		},
		{
			name: "intersection with null becomes null",
			outline: Outline{
				Type: IntersectionIteratorType,
				SubOutlines: []Outline{
					{Type: NullIteratorType},
					a,
				},
			},
			expected: Outline{Type: NullIteratorType},
		},
		{
			name: "nested unions flattened and sorted",
			outline: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					{
						Type:        UnionIteratorType,
						SubOutlines: []Outline{b, a},
					},
					a,
				},
			},
			expected: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{a, a, b},
			},
		},
		{
			name: "caveats lifted to top and sorted",
			outline: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					{
						Type:        CaveatIteratorType,
						Args:        &IteratorArgs{Caveat: caveat2},
						SubOutlines: []Outline{a},
					},
					{
						Type:        CaveatIteratorType,
						Args:        &IteratorArgs{Caveat: caveat1},
						SubOutlines: []Outline{b},
					},
				},
			},
			expected: Outline{
				Type: CaveatIteratorType,
				Args: &IteratorArgs{Caveat: caveat1},
				SubOutlines: []Outline{
					{
						Type: CaveatIteratorType,
						Args: &IteratorArgs{Caveat: caveat2},
						SubOutlines: []Outline{
							{
								Type:        UnionIteratorType,
								SubOutlines: []Outline{a, b},
							},
						},
					},
				},
			},
		},
		{
			name: "complex nested structure",
			outline: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					{
						Type: UnionIteratorType,
						SubOutlines: []Outline{
							{Type: NullIteratorType},
							a,
						},
					},
					{
						Type:        UnionIteratorType,
						SubOutlines: []Outline{b},
					},
				},
			},
			expected: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{a, b},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := CanonicalizeOutline(tt.outline)
			require.NoError(t, err)
			require.True(t, result.Equals(tt.expected), "expected %+v, got %+v", tt.expected, result)
		})
	}
}

func TestCanonicalizeIdempotency(t *testing.T) {
	t.Parallel()

	caveat := &core.ContextualizedCaveat{CaveatName: "test"}
	rel := schema.NewTestBaseRelation("doc", "rel", "user", tuple.Ellipsis)

	a := Outline{Type: DatastoreIteratorType, Args: &IteratorArgs{Relation: rel}}
	b := Outline{Type: DatastoreIteratorType, Args: &IteratorArgs{RelationName: "other"}}

	tests := []struct {
		name    string
		outline Outline
	}{
		{
			name:    "simple outline",
			outline: a,
		},
		{
			name: "union",
			outline: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{b, a},
			},
		},
		{
			name: "nested with caveat",
			outline: Outline{
				Type: CaveatIteratorType,
				Args: &IteratorArgs{Caveat: caveat},
				SubOutlines: []Outline{
					{
						Type: UnionIteratorType,
						SubOutlines: []Outline{
							a,
							{Type: NullIteratorType},
							b,
						},
					},
				},
			},
		},
		{
			name: "complex nested structure",
			outline: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					{
						Type: UnionIteratorType,
						SubOutlines: []Outline{
							{
								Type:        CaveatIteratorType,
								Args:        &IteratorArgs{Caveat: caveat},
								SubOutlines: []Outline{a},
							},
							b,
						},
					},
					{Type: NullIteratorType},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// First canonicalization
			canonical1, err := CanonicalizeOutline(tt.outline)
			require.NoError(t, err)

			// Second canonicalization (should be idempotent)
			canonical2, err := CanonicalizeOutline(canonical1)
			require.NoError(t, err)

			// They should be equal
			require.True(t, canonical1.Equals(canonical2),
				"Canonicalization is not idempotent.\nFirst: %+v\nSecond: %+v",
				canonical1, canonical2)
		})
	}
}

func TestCanonicalizeEquivalence(t *testing.T) {
	t.Parallel()

	rel := schema.NewTestBaseRelation("doc", "rel", "user", tuple.Ellipsis)
	a := Outline{Type: DatastoreIteratorType, Args: &IteratorArgs{Relation: rel}}
	b := Outline{Type: DatastoreIteratorType, Args: &IteratorArgs{RelationName: "other"}}
	c := Outline{Type: DatastoreIteratorType, Args: &IteratorArgs{DefinitionName: "def"}}

	tests := []struct {
		name        string
		outline1    Outline
		outline2    Outline
		shouldMatch bool
	}{
		{
			name: "Union[A,B] equals Union[B,A]",
			outline1: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{a, b},
			},
			outline2: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{b, a},
			},
			shouldMatch: true,
		},
		{
			name: "Union[Union[A,B],C] equals Union[A,B,C]",
			outline1: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					{
						Type:        UnionIteratorType,
						SubOutlines: []Outline{a, b},
					},
					c,
				},
			},
			outline2: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{a, b, c},
			},
			shouldMatch: true,
		},
		{
			name: "Union[A,Null] equals A",
			outline1: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					a,
					{Type: NullIteratorType},
				},
			},
			outline2:    a,
			shouldMatch: true,
		},
		{
			name: "Union[A] equals A",
			outline1: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{a},
			},
			outline2:    a,
			shouldMatch: true,
		},
		{
			name: "Intersection[A,Null] equals Null",
			outline1: Outline{
				Type: IntersectionIteratorType,
				SubOutlines: []Outline{
					a,
					{Type: NullIteratorType},
				},
			},
			outline2:    Outline{Type: NullIteratorType},
			shouldMatch: true,
		},
		{
			name: "Union[A,B] does not equal Union[A,C]",
			outline1: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{a, b},
			},
			outline2: Outline{
				Type:        UnionIteratorType,
				SubOutlines: []Outline{a, c},
			},
			shouldMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			canonical1, err := CanonicalizeOutline(tt.outline1)
			require.NoError(t, err)

			canonical2, err := CanonicalizeOutline(tt.outline2)
			require.NoError(t, err)

			if tt.shouldMatch {
				require.True(t, canonical1.Equals(canonical2),
					"Expected outlines to canonicalize to same form.\nCanonical1: %+v\nCanonical2: %+v",
					canonical1, canonical2)
			} else {
				require.False(t, canonical1.Equals(canonical2),
					"Expected outlines to canonicalize to different forms.\nCanonical1: %+v\nCanonical2: %+v",
					canonical1, canonical2)
			}
		})
	}
}

func TestCanonicalizeOutline_PopulatesCanonicalKey(t *testing.T) {
	t.Parallel()

	rel := schema.NewTestBaseRelation("doc", "rel", "user", tuple.Ellipsis)

	tests := []struct {
		name    string
		outline Outline
	}{
		{
			name:    "simple outline",
			outline: Outline{Type: NullIteratorType},
		},
		{
			name: "outline with args",
			outline: Outline{
				Type: DatastoreIteratorType,
				Args: &IteratorArgs{Relation: rel},
			},
		},
		{
			name: "union with subiterators",
			outline: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					{Type: NullIteratorType},
					{Type: NullIteratorType},
				},
			},
		},
		{
			name: "complex nested structure",
			outline: Outline{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					{
						Type: IntersectionIteratorType,
						SubOutlines: []Outline{
							{
								Type: DatastoreIteratorType,
								Args: &IteratorArgs{Relation: rel},
							},
							{Type: NullIteratorType},
						},
					},
					{Type: NullIteratorType},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)

			canonical, err := CanonicalizeOutline(tt.outline)
			require.NoError(err)

			// CanonicalKey should be populated
			require.False(canonical.CanonicalKey.IsEmpty(),
				"CanonicalKey should be populated after canonicalization")
		})
	}
}

func TestCanonicalKey_MatchesSerialization(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	rel := schema.NewTestBaseRelation("doc", "rel", "user", tuple.Ellipsis)
	outline := Outline{
		Type: UnionIteratorType,
		SubOutlines: []Outline{
			{Type: NullIteratorType},
			{
				Type: DatastoreIteratorType,
				Args: &IteratorArgs{Relation: rel},
			},
		},
	}

	canonical, err := CanonicalizeOutline(outline)
	require.NoError(err)

	// CanonicalKey should match the result of calling SerializeOutline
	directSerialization := canonical.Serialize()
	require.Equal(directSerialization.String(), canonical.CanonicalKey.String(),
		"CanonicalKey should match direct serialization result")
}

func TestCanonicalKey_AllNodesPopulated(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	rel := schema.NewTestBaseRelation("doc", "rel", "user", tuple.Ellipsis)
	outline := Outline{
		Type: UnionIteratorType,
		SubOutlines: []Outline{
			{
				Type: IntersectionIteratorType,
				SubOutlines: []Outline{
					{
						Type: DatastoreIteratorType,
						Args: &IteratorArgs{Relation: rel},
					},
					{Type: NullIteratorType},
				},
			},
			{Type: NullIteratorType},
		},
	}

	canonical, err := CanonicalizeOutline(outline)
	require.NoError(err)

	// Helper function to check all nodes recursively
	var checkAllNodes func(o Outline)
	checkAllNodes = func(o Outline) {
		require.False(o.CanonicalKey.IsEmpty(),
			"All nodes should have non-empty CanonicalKey")

		for _, sub := range o.SubOutlines {
			checkAllNodes(sub)
		}
	}

	checkAllNodes(canonical)
}

func TestCanonicalKey_Uniqueness(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	rel1 := schema.NewTestBaseRelation("doc", "rel1", "user", tuple.Ellipsis)
	rel2 := schema.NewTestBaseRelation("doc", "rel2", "user", tuple.Ellipsis)

	// Create two different outlines
	outline1 := Outline{
		Type: UnionIteratorType,
		SubOutlines: []Outline{
			{
				Type: DatastoreIteratorType,
				Args: &IteratorArgs{Relation: rel1},
			},
			{Type: NullIteratorType},
		},
	}

	outline2 := Outline{
		Type: UnionIteratorType,
		SubOutlines: []Outline{
			{
				Type: DatastoreIteratorType,
				Args: &IteratorArgs{Relation: rel2},
			},
			{Type: NullIteratorType},
		},
	}

	canonical1, err := CanonicalizeOutline(outline1)
	require.NoError(err)

	canonical2, err := CanonicalizeOutline(outline2)
	require.NoError(err)

	// Different outlines should have different CanonicalKeys
	require.NotEqual(canonical1.CanonicalKey.String(), canonical2.CanonicalKey.String(),
		"Different outlines should produce different CanonicalKeys")
}

func TestCanonicalKey_Equivalence(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	rel := schema.NewTestBaseRelation("doc", "rel", "user", tuple.Ellipsis)
	a := Outline{Type: DatastoreIteratorType, Args: &IteratorArgs{Relation: rel}}
	b := Outline{Type: DatastoreIteratorType, Args: &IteratorArgs{RelationName: "other"}}

	// Union[A,B] and Union[B,A] should produce the same CanonicalKey after canonicalization
	outline1 := Outline{
		Type:        UnionIteratorType,
		SubOutlines: []Outline{a, b},
	}

	outline2 := Outline{
		Type:        UnionIteratorType,
		SubOutlines: []Outline{b, a},
	}

	canonical1, err := CanonicalizeOutline(outline1)
	require.NoError(err)

	canonical2, err := CanonicalizeOutline(outline2)
	require.NoError(err)

	// Equivalent outlines should have the same CanonicalKey
	require.Equal(canonical1.CanonicalKey.String(), canonical2.CanonicalKey.String(),
		"Equivalent outlines should produce identical CanonicalKeys after canonicalization")
}

func TestCanonicalKey_Idempotency(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	rel := schema.NewTestBaseRelation("doc", "rel", "user", tuple.Ellipsis)
	outline := Outline{
		Type: UnionIteratorType,
		SubOutlines: []Outline{
			{
				Type: UnionIteratorType,
				SubOutlines: []Outline{
					{Type: NullIteratorType},
					{
						Type: DatastoreIteratorType,
						Args: &IteratorArgs{Relation: rel},
					},
				},
			},
			{Type: NullIteratorType},
		},
	}

	// First canonicalization
	canonical1, err := CanonicalizeOutline(outline)
	require.NoError(err)
	key1 := canonical1.CanonicalKey.String()

	// Second canonicalization (should be idempotent)
	canonical2, err := CanonicalizeOutline(canonical1)
	require.NoError(err)
	key2 := canonical2.CanonicalKey.String()

	// CanonicalKeys should be identical
	require.Equal(key1, key2,
		"Re-canonicalizing should produce the same CanonicalKey")
}

func TestCanonicalKey_MethodsWork(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	rel := schema.NewTestBaseRelation("doc", "rel", "user", tuple.Ellipsis)
	outline := Outline{
		Type: DatastoreIteratorType,
		Args: &IteratorArgs{Relation: rel},
	}

	canonical, err := CanonicalizeOutline(outline)
	require.NoError(err)

	// Test String() method
	keyStr := canonical.CanonicalKey.String()
	require.NotEmpty(keyStr)

	// Test IsEmpty() method
	require.False(canonical.CanonicalKey.IsEmpty())

	// Test Hash() method
	hash1 := canonical.CanonicalKey.Hash()
	hash2 := canonical.CanonicalKey.Hash()
	require.Equal(hash1, hash2, "Hash should be consistent")
	require.NotZero(hash1, "Hash should not be zero for non-empty key")
}

func TestCanonicalKey_WithCaveats(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	caveat := &core.ContextualizedCaveat{CaveatName: "age_check"}
	rel := schema.NewTestBaseRelation("doc", "rel", "user", tuple.Ellipsis)

	outline := Outline{
		Type: CaveatIteratorType,
		Args: &IteratorArgs{Caveat: caveat},
		SubOutlines: []Outline{
			{
				Type: DatastoreIteratorType,
				Args: &IteratorArgs{Relation: rel},
			},
		},
	}

	canonical, err := CanonicalizeOutline(outline)
	require.NoError(err)

	// CanonicalKey should be populated
	require.False(canonical.CanonicalKey.IsEmpty())

	// Should contain caveat name in the key
	require.Contains(canonical.CanonicalKey.String(), "cav:age_check")
}
