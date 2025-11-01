package query

import (
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// PushdownCaveatEvaluation pushes caveat evaluation down through certain composite iterators
// to allow earlier filtering and better performance.
//
// This optimization transforms:
//
//	Caveat(Union[A, B]) -> Union[Caveat(A), B] (if only A contains the caveat)
//	Caveat(Union[A, B]) -> Union[Caveat(A), Caveat(B)] (if both contain the caveat)
//
// The pushdown does NOT occur through IntersectionArrow iterators, as they have special
// semantics that require caveat evaluation to happen after the intersection.
func PushdownCaveatEvaluation(c *CaveatIterator) (Iterator, bool, error) {
	// Don't push through IntersectionArrow
	if _, ok := c.subiterator.(*IntersectionArrow); ok {
		return c, false, nil
	}

	// Don't push down if the subiterator is already a CaveatIterator
	// This prevents infinite recursion
	if _, ok := c.subiterator.(*CaveatIterator); ok {
		return c, false, nil
	}

	// Get the subiterators of the child
	subs := c.subiterator.Subiterators()
	if len(subs) == 0 {
		// No subiterators to push down into (e.g., leaf iterator)
		return c, false, nil
	}

	// Find which subiterators contain relations with this caveat
	newSubs := make([]Iterator, len(subs))
	changed := false
	for i, sub := range subs {
		if containsCaveat(sub, c.caveat) {
			// Wrap this subiterator with the caveat
			newSubs[i] = NewCaveatIterator(sub, c.caveat)
			changed = true
		} else {
			// Leave unchanged
			newSubs[i] = sub
		}
	}

	if !changed {
		return c, false, nil
	}

	// Replace the subiterators in the child iterator
	newChild, err := c.subiterator.ReplaceSubiterators(newSubs)
	if err != nil {
		return nil, false, err
	}

	// Return the child without the caveat wrapper
	return newChild, true, nil
}

// containsCaveat checks if an iterator tree contains a RelationIterator
// that references the given caveat.
func containsCaveat(it Iterator, caveat *core.ContextualizedCaveat) bool {
	found := false
	_, err := Walk(it, func(node Iterator) (Iterator, error) {
		if rel, ok := node.(*RelationIterator); ok {
			if relationContainsCaveat(rel, caveat) {
				found = true
			}
		}
		return node, nil
	})
	if err != nil {
		spiceerrors.MustPanicf("should never error -- callback contains no errors, but linters must always check")
	}

	return found
}

// relationContainsCaveat checks if a RelationIterator's base relation
// has a caveat that matches the given caveat name.
func relationContainsCaveat(rel *RelationIterator, caveat *core.ContextualizedCaveat) bool {
	if rel.base == nil || caveat == nil {
		return false
	}

	// Check if the relation has this caveat
	return rel.base.Caveat() == caveat.CaveatName
}
