package query

import (
	"cmp"
	"slices"
)

// StatisticsOptimizer uses cost estimates to optimize iterator trees.
type StatisticsOptimizer struct {
	Source StatisticsSource
}

// Optimize applies statistics-based optimizations to an iterator tree.
// It uses cost estimates to make decisions about reordering and restructuring.
func (s StatisticsOptimizer) Optimize(it Iterator) (Iterator, bool, error) {
	// Apply all statistics-based optimization strategies
	optimizers := []OptimizerFunc{
		s.reorderUnion,
		s.reorderIntersection,
		s.rebalanceArrow,
	}

	return ApplyOptimizations(it, optimizers)
}

// reorderUnion reorders union subiterators by selectivity (higher first).
// Higher selectivity branches are more likely to short-circuit, making unions more efficient.
func (s StatisticsOptimizer) reorderUnion(it Iterator) (Iterator, bool, error) {
	union, ok := it.(*Union)
	if !ok {
		return it, false, nil
	}

	newSubs, changed, err := s.reorderBySelectivity(union.subIts, false)
	if err != nil || !changed {
		return it, changed, err
	}

	return NewUnion(newSubs...), true, nil
}

// reorderIntersection reorders intersection subiterators by selectivity (lower first).
// Lower selectivity (more selective) branches filter out more results early,
// reducing work for subsequent branches.
func (s StatisticsOptimizer) reorderIntersection(it Iterator) (Iterator, bool, error) {
	intersection, ok := it.(*Intersection)
	if !ok {
		return it, false, nil
	}

	newSubs, changed, err := s.reorderBySelectivity(intersection.subIts, true)
	if err != nil || !changed {
		return it, changed, err
	}

	return NewIntersection(newSubs...), true, nil
}

// rebalanceArrow rebalances arrow operators to minimize total cost.
// If an arrow contains nested arrows on either side (possibly through wrapper
// iterators like Alias or CaveatIterator), we can restructure them to reduce
// the overall computation cost.
//
// For example: (A->B)->C can be rebalanced to A->(B->C) if that's cheaper.
// The key insight is that arrow operators are left-associative but we can
// restructure them based on cost estimates.
func (s StatisticsOptimizer) rebalanceArrow(it Iterator) (Iterator, bool, error) {
	arrow, ok := it.(*Arrow)
	if !ok {
		return it, false, nil
	}

	// Check if left side contains an arrow (possibly wrapped): wrapped(A->B)->C
	if leftArrow, leftWrappers := unwrapToArrow(arrow.left); leftArrow != nil {
		if alternative, changed, err := s.tryRebalance(arrow, leftArrow, leftWrappers, true); err != nil || changed {
			return alternative, changed, err
		}
	}

	// Check if right side contains an arrow (possibly wrapped): A->wrapped(B->C)
	if rightArrow, rightWrappers := unwrapToArrow(arrow.right); rightArrow != nil {
		if alternative, changed, err := s.tryRebalance(arrow, rightArrow, rightWrappers, false); err != nil || changed {
			return alternative, changed, err
		}
	}

	return it, false, nil
}

// unwrapToArrow looks through single-subiterator wrapper iterators
// (Alias, CaveatIterator, etc.) to find an underlying Arrow.
// Returns the arrow and the path of wrappers, or nil if no arrow is found.
func unwrapToArrow(it Iterator) (*Arrow, []Iterator) {
	var wrappers []Iterator
	current := it

	// Keep unwrapping single-subiterator wrappers
	for {
		subs := current.Subiterators()
		if len(subs) != 1 {
			// Not a single-subiterator wrapper
			break
		}

		// Check if current is an arrow
		if arrow, ok := current.(*Arrow); ok {
			return arrow, wrappers
		}

		// It's a wrapper, record it and continue
		wrappers = append(wrappers, current)
		current = subs[0]
	}

	// Check the final iterator
	if arrow, ok := current.(*Arrow); ok {
		return arrow, wrappers
	}

	return nil, nil
}

// rewrapIterator wraps an iterator with the given wrapper chain,
// properly handling CaveatIterators by checking which branch they should apply to.
// The wrappers are applied in order (innermost to outermost).
func rewrapIterator(inner Iterator, wrappers []Iterator, leftBranch, rightBranch Iterator) (Iterator, error) {
	result := inner
	for _, wrapper := range wrappers {
		// Special handling for CaveatIterator - only apply if the caveat is relevant to the branches
		if caveatIt, ok := wrapper.(*CaveatIterator); ok {
			// Check if the caveat applies to left or right branch
			leftHasCaveat := containsCaveat(leftBranch, caveatIt.caveat)
			rightHasCaveat := containsCaveat(rightBranch, caveatIt.caveat)

			// Only wrap if the caveat applies to at least one branch
			if !leftHasCaveat && !rightHasCaveat {
				// Caveat doesn't apply to either branch, skip it
				continue
			}

			// If caveat applies to both branches, we need to wrap the whole thing
			// If it only applies to one, we still wrap the whole thing since we're
			// at the level where the arrow combines them
		}

		// Replace the wrapper's subiterator with our current result
		newWrapper, err := wrapper.ReplaceSubiterators([]Iterator{result})
		if err != nil {
			return nil, err
		}
		result = newWrapper
	}
	return result, nil
}

// tryRebalance attempts to rebalance an arrow with a nested arrow on one side.
// Returns the rebalanced iterator and true if the rebalancing was cheaper, nil and false otherwise.
func (s StatisticsOptimizer) tryRebalance(original *Arrow, nestedArrow *Arrow, wrappers []Iterator, isLeftSide bool) (Iterator, bool, error) {
	// Calculate cost of original
	originalCost, err := s.Source.Cost(original)
	if err != nil {
		return nil, false, err
	}

	// Build the alternative based on which side the nested arrow is on
	var innerArrow *Arrow
	var alternative *Arrow
	var leftBranch, rightBranch Iterator

	if isLeftSide {
		// Original: wrapped(A->B)->C
		// Alternative: A->wrapped(B->C)
		leftBranch = nestedArrow.right
		rightBranch = original.right
		innerArrow = NewArrow(leftBranch, rightBranch)

		wrappedInner, err := rewrapIterator(innerArrow, wrappers, leftBranch, rightBranch)
		if err != nil {
			return nil, false, err
		}

		alternative = NewArrow(nestedArrow.left, wrappedInner)
	} else {
		// Original: A->wrapped(B->C)
		// Alternative: wrapped(A->B)->C
		leftBranch = original.left
		rightBranch = nestedArrow.left
		innerArrow = NewArrow(leftBranch, rightBranch)

		wrappedInner, err := rewrapIterator(innerArrow, wrappers, leftBranch, rightBranch)
		if err != nil {
			return nil, false, err
		}

		alternative = NewArrow(wrappedInner, nestedArrow.right)
	}

	// Calculate cost of alternative
	alternativeCost, err := s.Source.Cost(alternative)
	if err != nil {
		return nil, false, err
	}

	// Return alternative if cheaper
	if alternativeCost.CheckCost < originalCost.CheckCost {
		return alternative, true, nil
	}

	return nil, false, nil
}

// iteratorWithSelectivity pairs an iterator with its selectivity for sorting.
type iteratorWithSelectivity struct {
	iterator    Iterator
	selectivity float64
}

// reorderBySelectivity reorders subiterators by selectivity.
// If ascending is true, sorts by lower selectivity first (for intersections).
// If ascending is false, sorts by higher selectivity first (for unions).
func (s StatisticsOptimizer) reorderBySelectivity(subs []Iterator, ascending bool) ([]Iterator, bool, error) {
	if len(subs) <= 1 {
		return subs, false, nil
	}

	// Get cost estimates for each subiterator
	subsWithCost := make([]iteratorWithSelectivity, len(subs))
	for i, sub := range subs {
		est, err := s.Source.Cost(sub)
		if err != nil {
			return nil, false, err
		}
		subsWithCost[i] = iteratorWithSelectivity{
			iterator:    sub,
			selectivity: est.CheckSelectivity,
		}
	}

	// Sort by selectivity
	slices.SortFunc(subsWithCost, func(a, b iteratorWithSelectivity) int {
		result := cmp.Compare(a.selectivity, b.selectivity)
		if !ascending {
			result = -result // Reverse for descending order
		}
		return result
	})

	// Check if order changed
	changed := false
	for i, sub := range subsWithCost {
		if sub.iterator != subs[i] {
			changed = true
			break
		}
	}

	if !changed {
		return subs, false, nil
	}

	// Build new subiterators slice
	newSubs := make([]Iterator, len(subsWithCost))
	for i, sub := range subsWithCost {
		newSubs[i] = sub.iterator
	}

	return newSubs, true, nil
}
