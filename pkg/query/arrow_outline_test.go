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
