package query

import (
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
	if !ok || len(union.subIts) <= 1 {
		return it, false, nil
	}

	// Get cost estimates for each subiterator
	type subWithCost struct {
		iterator    Iterator
		selectivity float64
	}

	subs := make([]subWithCost, len(union.subIts))
	for i, sub := range union.subIts {
		est, err := s.Source.Cost(sub)
		if err != nil {
			return it, false, err
		}
		subs[i] = subWithCost{
			iterator:    sub,
			selectivity: est.CheckSelectivity,
		}
	}

	// Sort by selectivity (higher first)
	slices.SortFunc(subs, func(a, b subWithCost) int {
		// Higher selectivity first (descending order)
		if a.selectivity > b.selectivity {
			return -1
		}
		if a.selectivity < b.selectivity {
			return 1
		}
		return 0
	})

	// Check if order changed
	changed := false
	for i, sub := range subs {
		if sub.iterator != union.subIts[i] {
			changed = true
			break
		}
	}

	if !changed {
		return it, false, nil
	}

	// Build new subiterators slice
	newSubs := make([]Iterator, len(subs))
	for i, sub := range subs {
		newSubs[i] = sub.iterator
	}

	return NewUnion(newSubs...), true, nil
}

// reorderIntersection reorders intersection subiterators by selectivity (lower first).
// Lower selectivity (more selective) branches filter out more results early,
// reducing work for subsequent branches.
func (s StatisticsOptimizer) reorderIntersection(it Iterator) (Iterator, bool, error) {
	intersection, ok := it.(*Intersection)
	if !ok || len(intersection.subIts) <= 1 {
		return it, false, nil
	}

	// Get cost estimates for each subiterator
	type subWithCost struct {
		iterator    Iterator
		selectivity float64
	}

	subs := make([]subWithCost, len(intersection.subIts))
	for i, sub := range intersection.subIts {
		est, err := s.Source.Cost(sub)
		if err != nil {
			return it, false, err
		}
		subs[i] = subWithCost{
			iterator:    sub,
			selectivity: est.CheckSelectivity,
		}
	}

	// Sort by selectivity (lower first - more selective first)
	slices.SortFunc(subs, func(a, b subWithCost) int {
		// Lower selectivity first (ascending order)
		if a.selectivity < b.selectivity {
			return -1
		}
		if a.selectivity > b.selectivity {
			return 1
		}
		return 0
	})

	// Check if order changed
	changed := false
	for i, sub := range subs {
		if sub.iterator != intersection.subIts[i] {
			changed = true
			break
		}
	}

	if !changed {
		return it, false, nil
	}

	// Build new subiterators slice
	newSubs := make([]Iterator, len(subs))
	for i, sub := range subs {
		newSubs[i] = sub.iterator
	}

	return NewIntersection(newSubs...), true, nil
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
	leftArrow, leftWrappers := unwrapToArrow(arrow.left)
	if leftArrow != nil {
		// We have wrapped(A->B)->C, consider rebalancing to A->wrapped(B->C)
		// Original: wrapped(A->B)->C
		// Alternative: A->wrapped(B->C)

		// Calculate cost of original
		originalCost, err := s.Source.Cost(arrow)
		if err != nil {
			return it, false, err
		}

		// Calculate cost of alternative: A->wrapped(B->C)
		// Create the inner arrow: B->C
		innerArrow := NewArrow(leftArrow.right, arrow.right)

		// Rewrap the inner arrow with the same wrappers
		// Pass branch info so caveats stay on the correct branch
		wrappedInner, err := rewrapIterator(innerArrow, leftWrappers, leftArrow.right, arrow.right)
		if err != nil {
			return it, false, err
		}

		// Create the alternative: A->wrapped(B->C)
		alternative := NewArrow(leftArrow.left, wrappedInner)

		alternativeCost, err := s.Source.Cost(alternative)
		if err != nil {
			return it, false, err
		}

		// If alternative is cheaper, use it
		if alternativeCost.CheckCost < originalCost.CheckCost {
			return alternative, true, nil
		}
	}

	// Check if right side contains an arrow (possibly wrapped): A->wrapped(B->C)
	rightArrow, rightWrappers := unwrapToArrow(arrow.right)
	if rightArrow != nil {
		// We have A->wrapped(B->C), consider rebalancing to wrapped(A->B)->C
		// Original: A->wrapped(B->C)
		// Alternative: wrapped(A->B)->C

		// Calculate cost of original
		originalCost, err := s.Source.Cost(arrow)
		if err != nil {
			return it, false, err
		}

		// Calculate cost of alternative: wrapped(A->B)->C
		// Create the inner arrow: A->B
		innerArrow := NewArrow(arrow.left, rightArrow.left)

		// Rewrap the inner arrow with the same wrappers
		// Pass branch info so caveats stay on the correct branch
		wrappedInner, err := rewrapIterator(innerArrow, rightWrappers, arrow.left, rightArrow.left)
		if err != nil {
			return it, false, err
		}

		// Create the alternative: wrapped(A->B)->C
		alternative := NewArrow(wrappedInner, rightArrow.right)

		alternativeCost, err := s.Source.Cost(alternative)
		if err != nil {
			return it, false, err
		}

		// If alternative is cheaper, use it
		if alternativeCost.CheckCost < originalCost.CheckCost {
			return alternative, true, nil
		}
	}

	return it, false, nil
}
