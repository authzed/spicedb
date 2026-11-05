package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query/planhint"
)

func TestArrowCheckAlternatives(t *testing.T) {
	// Distinct occurrences ensure each strategy points at its own starting operand.
	left := Outline{ID: 1, Type: NullIteratorType}
	right := Outline{ID: 2, Type: FixedIteratorType}
	outline := Outline{Type: ArrowIteratorType, SubOutlines: []Outline{left, right}}
	alternatives, ok := outline.ArrowCheckAlternatives()
	require.True(t, ok)
	require.Equal(t, planhint.LeftToRight, alternatives[0].Direction)
	require.Equal(t, left, alternatives[0].Enumeration)
	require.Equal(t, OperationIterSubjects, alternatives[0].Operation)
	require.Equal(t, planhint.RightToLeft, alternatives[1].Direction)
	require.Equal(t, right, alternatives[1].Enumeration)
	require.Equal(t, OperationIterResources, alternatives[1].Operation)
}

func TestArrowCheckAlternativesRejectUnsupportedShapes(t *testing.T) {
	cases := map[string]Outline{
		"empty arrow":        {Type: ArrowIteratorType},
		"missing right":      {Type: ArrowIteratorType, SubOutlines: []Outline{{Type: NullIteratorType}}},
		"extra operand":      {Type: ArrowIteratorType, SubOutlines: []Outline{{}, {}, {}}},
		"union":              {Type: UnionIteratorType, SubOutlines: []Outline{{}, {}}},
		"intersection arrow": {Type: IntersectionArrowIteratorType, SubOutlines: []Outline{{}, {}}},
	}

	for name, outline := range cases {
		t.Run(name, func(t *testing.T) {
			alternatives, ok := outline.ArrowCheckAlternatives()
			require.False(t, ok)
			require.Empty(t, alternatives)
		})
	}
}

func TestArrowCheckAlternativesRespectRecursiveScope(t *testing.T) {
	member := &IteratorArgs{DefinitionName: "group", RelationName: "member"}
	manager := &IteratorArgs{DefinitionName: "group", RelationName: "manager"}
	sentinel := Outline{Type: RecursiveSentinelIteratorType, Args: member}

	for name, right := range map[string]Outline{
		"free sentinel":         sentinel,
		"wrapped free sentinel": {Type: AliasIteratorType, SubOutlines: []Outline{sentinel}},
		"own controller":        {Type: RecursiveIteratorType, Args: member, SubOutlines: []Outline{sentinel}},
		"different controller":  {Type: RecursiveIteratorType, Args: manager, SubOutlines: []Outline{sentinel}},
	} {
		t.Run(name, func(t *testing.T) {
			arrow := Outline{
				Type:        ArrowIteratorType,
				SubOutlines: []Outline{{Type: NullIteratorType}, right},
			}
			_, ok := arrow.ArrowCheckAlternatives()
			require.Equal(t, name == "own controller", ok)
		})
	}
}

// Compilation must reject unsafe hints even when they come from another producer.
func TestCompileDeclinesReverseAcrossRecursiveBoundary(t *testing.T) {
	co, err := CanonicalizeOutline(Outline{
		Type: ArrowIteratorType,
		SubOutlines: []Outline{
			{Type: FixedIteratorType},
			{Type: RecursiveSentinelIteratorType, Args: &IteratorArgs{DefinitionName: "group", RelationName: "member"}},
		},
	})
	require.NoError(t, err)
	co.Hints = map[OutlineNodeID][]Hint{co.Root.ID: {ArrowDirectionHint(rightToLeft)}}
	co.HintLedger = planhint.NewLedger(nil)
	it, err := co.Compile()

	require.NoError(t, err)
	require.Equal(t, leftToRight, it.(*ArrowIterator).direction)
	decisions := co.HintLedger.Decisions()

	require.Len(t, decisions, 1)
	require.Equal(t, planhint.Declined, decisions[0].Outcome)
	require.Equal(t, planhint.CorrectnessConstraint, decisions[0].Reason)
}
