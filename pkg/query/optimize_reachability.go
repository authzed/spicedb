package query

import "slices"

// ApplyReachabilityPruning applies subject-type reachability pruning to an
// iterator tree, replacing subtrees with empty FixedIterators when they can
// never produce the target subject type.
//
// For arrows (e.g. editor->view), the pruning decision is based on whether the
// right side (computed userset) can reach the target subject type. If not, the
// entire arrow is elided. The left side's subject types are intermediate hops
// and are not considered for pruning.
func ApplyReachabilityPruning(it Iterator, targetSubjectType string) (Iterator, bool, error) {
	if it == nil {
		return nil, false, nil
	}

	// For arrows, only recurse into the right child.
	if arrow, ok := it.(*ArrowIterator); ok {
		return applyReachabilityToArrow(arrow, targetSubjectType)
	}
	if arrow, ok := it.(*IntersectionArrowIterator); ok {
		return applyReachabilityToIntersectionArrow(arrow, targetSubjectType)
	}

	// For all other iterators, recurse into all children first (bottom-up).
	origSubs := it.Subiterators()
	changed := false
	if len(origSubs) > 0 {
		subs := make([]Iterator, len(origSubs))
		copy(subs, origSubs)

		subChanged := false
		for i, sub := range subs {
			newSub, ok, err := ApplyReachabilityPruning(sub, targetSubjectType)
			if err != nil {
				return nil, false, err
			}
			if ok {
				subs[i] = newSub
				subChanged = true
			}
		}
		if subChanged {
			changed = true
			var err error
			it, err = it.ReplaceSubiterators(subs)
			if err != nil {
				return nil, false, err
			}
		}
	}

	// Now check this node's subject types.
	subjectTypes, err := it.SubjectTypes()
	if err != nil || len(subjectTypes) == 0 {
		return it, changed, nil
	}

	if hasMatchingSubjectType(subjectTypes, targetSubjectType) {
		return it, changed, nil
	}

	return newEmptyWithKey(it.CanonicalKey()), true, nil
}

func applyReachabilityToArrow(arrow *ArrowIterator, targetSubjectType string) (Iterator, bool, error) {
	// Only recurse into the right child - left side types are intermediates.
	newRight, rightChanged, err := ApplyReachabilityPruning(arrow.right, targetSubjectType)
	if err != nil {
		return nil, false, err
	}

	if rightChanged {
		newArrow, err := arrow.ReplaceSubiterators([]Iterator{arrow.left, newRight})
		if err != nil {
			return nil, false, err
		}
		arrow = newArrow.(*ArrowIterator)
	}

	// Check if the right side can produce the target subject type.
	if shouldPruneArrowRight(arrow.right, targetSubjectType, rightChanged) {
		return newEmptyWithKey(arrow.CanonicalKey()), true, nil
	}
	return arrow, rightChanged, nil
}

func applyReachabilityToIntersectionArrow(arrow *IntersectionArrowIterator, targetSubjectType string) (Iterator, bool, error) {
	// Only recurse into the right child - left side types are intermediates.
	newRight, rightChanged, err := ApplyReachabilityPruning(arrow.right, targetSubjectType)
	if err != nil {
		return nil, false, err
	}

	if rightChanged {
		newArrow, err := arrow.ReplaceSubiterators([]Iterator{arrow.left, newRight})
		if err != nil {
			return nil, false, err
		}
		arrow = newArrow.(*IntersectionArrowIterator)
	}

	// Check if the right side can produce the target subject type.
	if shouldPruneArrowRight(arrow.right, targetSubjectType, rightChanged) {
		return newEmptyWithKey(arrow.CanonicalKey()), true, nil
	}
	return arrow, rightChanged, nil
}

// shouldPruneArrowRight checks whether the right side of an arrow can produce
// the target subject type. Since applyReachabilityToArrow always recurses into
// the right child first, by the time this runs any non-matching leaves have
// already been pruned. So this only needs to check two cases:
//  1. Right side has no subject types after pruning → prune the arrow.
//  2. Right side still has matching subject types → keep the arrow.
func shouldPruneArrowRight(right Iterator, targetSubjectType string, rightAlreadyPruned bool) bool {
	rightTypes, err := right.SubjectTypes()
	if err != nil {
		return false
	}

	if len(rightTypes) == 0 {
		return rightAlreadyPruned
	}

	return !hasMatchingSubjectType(rightTypes, targetSubjectType)
}

func hasMatchingSubjectType(subjectTypes []ObjectType, targetType string) bool {
	return slices.ContainsFunc(subjectTypes, func(s ObjectType) bool {
		return s.Type == targetType
	})
}

func newEmptyWithKey(key CanonicalKey) *FixedIterator {
	empty := NewFixedIterator()
	empty.canonicalKey = key
	return empty
}
