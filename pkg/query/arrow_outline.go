package query

import "github.com/authzed/spicedb/pkg/query/planhint"

// ArrowCheckAlternative describes one starting strategy for an ordinary arrow Check.
// Enumeration produces intermediate objects, which are then checked against the other operand.
type ArrowCheckAlternative struct {
	// Direction identifies the execution direction represented by this alternative.
	Direction planhint.Direction

	// Enumeration is the subtree enumerated to produce intermediate objects.
	Enumeration Outline

	// Operation is the enumeration operation applied to Enumeration.
	Operation Operation
}

// ArrowCheckAlternatives returns the forward and reverse starting strategies for an arrow Check.
// Forward execution enumerates subjects from the left operand, then checks the right.
// Reverse execution enumerates resources from the right operand, then checks the left.
// The forward alternative comes first so callers can preserve it when estimated costs tie.
// Non-arrow nodes, malformed arrows and intersection arrows have no such alternatives.
func (outline Outline) ArrowCheckAlternatives() ([2]ArrowCheckAlternative, bool) {
	if outline.Type != ArrowIteratorType || len(outline.SubOutlines) != 2 {
		return [2]ArrowCheckAlternative{}, false
	}

	return [2]ArrowCheckAlternative{
		{
			Direction:   planhint.LeftToRight,
			Enumeration: outline.SubOutlines[0],
			Operation:   OperationIterSubjects,
		},
		{
			Direction:   planhint.RightToLeft,
			Enumeration: outline.SubOutlines[1],
			Operation:   OperationIterResources,
		},
	}, true
}
