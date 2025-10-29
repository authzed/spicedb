package query

import "slices"

// ElideSingletonUnionAndIntersection removes unnecessary union and intersection wrappers
// that contain only a single subiterator.
func ElideSingletonUnionAndIntersection(it Iterator) (Iterator, bool, error) {
	switch v := it.(type) {
	case *Union:
		if len(v.subIts) == 1 {
			return v.subIts[0], true, nil
		}
	case *Intersection:
		if len(v.subIts) == 1 {
			return v.subIts[0], true, nil
		}
	}
	return it, false, nil
}

// RemoveNullIterators removes null iterators from union and intersection operations.
// Unions, removes the empty set (A | 0 = A), Intersection, returns a null itself (A & 0 = 0)
func RemoveNullIterators(it Iterator) (Iterator, bool, error) {
	switch v := it.(type) {
	case *Union:
		subs := v.Subiterators()
		hasEmpty := false
		newSubs := make([]Iterator, 0)
		for _, s := range subs {
			if isEmptyFixed(s) {
				hasEmpty = true
			} else {
				newSubs = append(newSubs, s)
			}
		}
		if hasEmpty {
			newit, err := it.ReplaceSubiterators(newSubs)
			return newit, true, err
		}
	case *Intersection:
		if slices.ContainsFunc(v.Subiterators(), isEmptyFixed) {
			return NewEmptyFixedIterator(), true, nil
		}
	}
	return it, false, nil
}

// isEmptyFixed detects an empty, fixed iterator, used as a null.
func isEmptyFixed(it Iterator) bool {
	if v, ok := it.(*FixedIterator); ok {
		if len(v.paths) == 0 {
			return true
		}
	}
	return false
}

// StaticOptimizations is a list of optimization functions that can be safely applied
// to any iterator tree without needing runtime information or context.
var StaticOptimizations = []OptimizerFunc{
	RemoveNullIterators,
	ElideSingletonUnionAndIntersection,
}
