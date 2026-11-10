package query

import (
	"slices"

	"github.com/authzed/spicedb/pkg/query/planhint"
)

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
// Arrows whose right operand requires an enclosing recursion controller also have
// no alternatives: enumerating a free sentinel would discard recursive grants.
func (outline Outline) ArrowCheckAlternatives() ([2]ArrowCheckAlternative, bool) {
	if outline.Type != ArrowIteratorType || len(outline.SubOutlines) != 2 || outline.SubOutlines[1].HasFreeRecursiveReferences() {
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

// HasFreeRecursiveReferences reports whether enumeration depends on a controller
// outside this subtree. A controller binds references to its own definition and
// relation; a sentinel alone exposes only an enumeration frontier, not its Check
// controller. Such a subtree cannot replace targeted Check with IterResources.
func (outline Outline) HasFreeRecursiveReferences() bool {
	return hasFreeRecursiveReferences(outline, nil)
}

func hasFreeRecursiveReferences(outline Outline, bound []string) bool {
	if outline.Type == RecursiveSentinelIteratorType {
		return outline.Args == nil || !slices.Contains(bound, outline.Args.DefinitionName+"#"+outline.Args.RelationName)
	}

	if outline.Type == RecursiveIteratorType && outline.Args != nil {
		bound = append(bound, outline.Args.DefinitionName+"#"+outline.Args.RelationName)
	}

	for _, child := range outline.SubOutlines {
		if hasFreeRecursiveReferences(child, bound) {
			return true
		}
	}

	return false
}
