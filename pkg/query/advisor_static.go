package query

import (
	"cmp"
	"errors"
	"math"
	"slices"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// staticEstimate represents the estimated worst-case cost and selectivity metrics for an iterator.
// These estimates are used internally by StaticAdvisor to make optimization decisions.
//
// Costs are a completely made-up unit, relevant only within the StaticAdvisor. They are
// not portable between different advisors, and are only comparable to each other.
type staticEstimate struct {
	Cardinality int // Cardinality is the estimated number of results this iterator will produce.

	// CheckSelectivity is the estimated probability (0.0-1.0) that a Check operation
	// will return true. Higher values mean the check is more likely to pass.
	CheckSelectivity float64

	// CheckCost is the estimated cost to perform a Check operation on this iterator.
	// This represents the computational cost of verifying if a specific resource-subject
	// relationship exists.
	CheckCost int

	// IterResourcesCost is the estimated cost to iterate over all resources
	// accessible through this iterator.
	IterResourcesCost int

	// IterSubjectsCost is the estimated cost to iterate over all subjects
	// that have access through this iterator.
	IterSubjectsCost int
}

// StaticAdvisor provides query plan optimization guidance using static cost estimates.
// It uses configurable parameters to estimate iterator costs and suggest optimizations
// through hints and mutations.
//
// Costs are static for StaticAdvisor -- we take the base cost of a check to be 1 tuple check.
// For iterating subjects and resources, we take it to iterate all tuples for a given relation.
type StaticAdvisor struct {
	// NumberOfTuplesInRelation is the assumed number of tuples in any relation (a complete average).
	NumberOfTuplesInRelation int

	// Fanout is the assumed average number of subjects per resource or
	// resources per subject.
	Fanout int

	// CheckSelectivity is the default probability (0.0-1.0) that a Check
	// operation will return true.
	CheckSelectivity float64
}

// DefaultStaticAdvisor returns a StaticAdvisor instance with default values
func DefaultStaticAdvisor() StaticAdvisor {
	return StaticAdvisor{
		NumberOfTuplesInRelation: 10,
		Fanout:                   5,
		CheckSelectivity:         0.9,
	}
}

// GetHints returns optimization hints for the given outline node.
// For arrow iterators, it suggests the optimal execution direction based on cost estimates.
func (s StaticAdvisor) GetHints(outline Outline, keySource CanonicalKeySource) ([]Hint, error) {
	// Only arrow iterators get hints for now
	if outline.Type != ArrowIteratorType {
		return nil, nil
	}

	// Need exactly 2 subiterators for arrow
	if len(outline.SubOutlines) != 2 {
		return nil, nil
	}

	// Get cost estimates for both suboutlines
	leftEst, err := s.costOutline(outline.SubOutlines[0])
	if err != nil {
		return nil, err
	}

	rightEst, err := s.costOutline(outline.SubOutlines[1])
	if err != nil {
		return nil, err
	}

	// Cost for left-to-right: IterSubjects(left) + Check(right) for each result
	leftToRightCost := leftEst.IterSubjectsCost + (leftEst.Cardinality * rightEst.CheckCost)

	// Cost for right-to-left: IterResources(right) + Check(left) for each result
	rightToLeftCost := rightEst.IterResourcesCost + (rightEst.Cardinality * leftEst.CheckCost)

	// Choose the cheaper direction
	if rightToLeftCost < leftToRightCost {
		return []Hint{ArrowDirectionHint(rightToLeft)}, nil
	}

	// Default to left-to-right (or keep current)
	return []Hint{ArrowDirectionHint(leftToRight)}, nil
}

// GetMutations returns outline mutations for the given outline node.
// This includes reordering union/intersection branches by selectivity and
// rebalancing nested arrow structures.
func (s StaticAdvisor) GetMutations(outline Outline, keySource CanonicalKeySource) ([]OutlineMutation, error) {
	switch outline.Type {
	case UnionIteratorType:
		// Reorder by descending selectivity (higher first = more likely to short-circuit)
		order, changed, err := s.selectivityOrder(outline.SubOutlines, false)
		if err != nil || !changed {
			return nil, err
		}
		return []OutlineMutation{ReorderMutation(order)}, nil

	case IntersectionIteratorType:
		// Reorder by ascending selectivity (lower first = more selective, filters more early)
		order, changed, err := s.selectivityOrder(outline.SubOutlines, true)
		if err != nil || !changed {
			return nil, err
		}
		return []OutlineMutation{ReorderMutation(order)}, nil

	case ArrowIteratorType:
		// Try to rebalance nested arrows
		return s.rebalanceArrow(outline)
	}
	return nil, nil
}

// rebalanceArrow checks if an arrow has nested arrows and returns a rotation
// mutation if rebalancing would reduce cost.
func (s StaticAdvisor) rebalanceArrow(outline Outline) ([]OutlineMutation, error) {
	if len(outline.SubOutlines) != 2 {
		return nil, nil
	}

	left := outline.SubOutlines[0]
	right := outline.SubOutlines[1]

	// Check if left child is an arrow: (A->B)->C
	if left.Type == ArrowIteratorType && len(left.SubOutlines) == 2 {
		mutation, shouldRotate, err := s.tryRotateLeft(outline)
		if err != nil || !shouldRotate {
			return nil, err
		}
		return []OutlineMutation{mutation}, nil
	}

	// Check if right child is an arrow: A->(B->C)
	if right.Type == ArrowIteratorType && len(right.SubOutlines) == 2 {
		mutation, shouldRotate, err := s.tryRotateRight(outline)
		if err != nil || !shouldRotate {
			return nil, err
		}
		return []OutlineMutation{mutation}, nil
	}

	return nil, nil
}

// tryRotateLeft checks if rotating (A->B)->C to A->(B->C) would reduce cost.
func (s StaticAdvisor) tryRotateLeft(outline Outline) (OutlineMutation, bool, error) {
	// Calculate cost of original
	originalCost, err := s.costOutline(outline)
	if err != nil {
		return nil, false, err
	}

	// Apply rotation mutation to get alternative
	rotation := RotateArrowMutation(true)
	alternative := rotation(outline)

	// Calculate cost of alternative
	alternativeCost, err := s.costOutline(alternative)
	if err != nil {
		return nil, false, err
	}

	// Return mutation if alternative is cheaper
	if alternativeCost.CheckCost < originalCost.CheckCost {
		return rotation, true, nil
	}

	return nil, false, nil
}

// tryRotateRight checks if rotating A->(B->C) to (A->B)->C would reduce cost.
func (s StaticAdvisor) tryRotateRight(outline Outline) (OutlineMutation, bool, error) {
	// Calculate cost of original
	originalCost, err := s.costOutline(outline)
	if err != nil {
		return nil, false, err
	}

	// Apply rotation mutation to get alternative
	rotation := RotateArrowMutation(false)
	alternative := rotation(outline)

	// Calculate cost of alternative
	alternativeCost, err := s.costOutline(alternative)
	if err != nil {
		return nil, false, err
	}

	// Return mutation if alternative is cheaper
	if alternativeCost.CheckCost < originalCost.CheckCost {
		return rotation, true, nil
	}

	return nil, false, nil
}

// selectivityOrder returns the permutation (as []int indices) that sorts subs
// by selectivity. ascending=true means lower selectivity first (intersection),
// ascending=false means higher selectivity first (union).
// Returns nil, false, nil if the order is already optimal or there are <=1 subs.
func (s StaticAdvisor) selectivityOrder(subs []Outline, ascending bool) ([]int, bool, error) {
	if len(subs) <= 1 {
		return nil, false, nil
	}

	// Estimate selectivity per sub
	type entry struct {
		idx         int
		selectivity float64
	}
	entries := make([]entry, len(subs))
	for i, sub := range subs {
		est, err := s.costOutline(sub)
		if err != nil {
			return nil, false, err
		}
		entries[i] = entry{i, est.CheckSelectivity}
	}

	// Sort by selectivity
	slices.SortStableFunc(entries, func(a, b entry) int {
		result := cmp.Compare(a.selectivity, b.selectivity)
		if !ascending {
			result = -result
		}
		return result
	})

	// Check if anything moved
	changed := false
	for i, e := range entries {
		if e.idx != i {
			changed = true
			break
		}
	}
	if !changed {
		return nil, false, nil
	}

	// Build the permutation vector
	order := make([]int, len(entries))
	for i, e := range entries {
		order[i] = e.idx
	}
	return order, true, nil
}

// costOutline returns a cost estimate for the given outline using static assumptions.
// It recursively estimates costs for composite outlines by combining the costs
// of their sub-outlines according to their operational semantics.
//
// This is an internal method used by the advisor to make optimization decisions.
func (s StaticAdvisor) costOutline(outline Outline) (staticEstimate, error) {
	switch outline.Type {
	case NullIteratorType, FixedIteratorType:
		// Count fixed paths
		pathCount := 0
		if outline.Args != nil {
			pathCount = len(outline.Args.FixedPaths)
		}
		return staticEstimate{
			Cardinality:       pathCount,
			CheckCost:         1,
			CheckSelectivity:  s.CheckSelectivity,
			IterResourcesCost: pathCount,
			IterSubjectsCost:  pathCount,
		}, nil

	case DatastoreIteratorType:
		return staticEstimate{
			Cardinality:       s.NumberOfTuplesInRelation,
			CheckCost:         1,
			CheckSelectivity:  s.CheckSelectivity,
			IterResourcesCost: s.Fanout,
			IterSubjectsCost:  s.Fanout,
		}, nil

	case ArrowIteratorType:
		if len(outline.SubOutlines) != 2 {
			return staticEstimate{}, errors.New("StaticAdvisor: arrow requires exactly 2 suboutlines")
		}

		ls, err := s.costOutline(outline.SubOutlines[0])
		if err != nil {
			return staticEstimate{}, err
		}
		rs, err := s.costOutline(outline.SubOutlines[1])
		if err != nil {
			return staticEstimate{}, err
		}

		// For outlines, we assume left-to-right direction (default)
		// The hint will override this during compilation
		checkCost := ls.IterSubjectsCost + (ls.Cardinality * rs.CheckCost)
		iterResourcesCost := ls.IterResourcesCost + (ls.Cardinality * rs.IterResourcesCost)
		iterSubjectsCost := ls.IterSubjectsCost + (ls.Cardinality * rs.IterSubjectsCost)

		return staticEstimate{
			Cardinality:       ls.Cardinality * rs.Cardinality,
			CheckCost:         checkCost,
			CheckSelectivity:  ls.CheckSelectivity * rs.CheckSelectivity,
			IterResourcesCost: iterResourcesCost,
			IterSubjectsCost:  iterSubjectsCost,
		}, nil

	case IntersectionArrowIteratorType:
		if len(outline.SubOutlines) != 2 {
			return staticEstimate{}, errors.New("StaticAdvisor: intersection arrow requires exactly 2 suboutlines")
		}

		ls, err := s.costOutline(outline.SubOutlines[0])
		if err != nil {
			return staticEstimate{}, err
		}
		rs, err := s.costOutline(outline.SubOutlines[1])
		if err != nil {
			return staticEstimate{}, err
		}

		selectivity := math.Pow(ls.CheckSelectivity, float64(s.Fanout)) * rs.CheckSelectivity
		checkCost := ls.IterSubjectsCost + (ls.Cardinality * rs.CheckCost)

		return staticEstimate{
			Cardinality:       int(float64(ls.Cardinality*rs.Cardinality) * selectivity),
			CheckCost:         checkCost,
			CheckSelectivity:  selectivity,
			IterResourcesCost: ls.IterResourcesCost + (ls.Cardinality * rs.IterResourcesCost) + checkCost,
			IterSubjectsCost:  ls.IterSubjectsCost + (ls.Cardinality * rs.IterSubjectsCost),
		}, nil

	case UnionIteratorType:
		if len(outline.SubOutlines) == 0 {
			return staticEstimate{}, errors.New("StaticAdvisor: union with no suboutlines")
		}

		result := staticEstimate{}
		for _, sub := range outline.SubOutlines {
			est, err := s.costOutline(sub)
			if err != nil {
				return staticEstimate{}, err
			}
			result.Cardinality += est.Cardinality
			result.CheckCost += est.CheckCost
			result.IterResourcesCost += est.IterResourcesCost
			result.IterSubjectsCost += est.IterSubjectsCost
			result.CheckSelectivity = max(est.CheckSelectivity, result.CheckSelectivity)
		}
		return result, nil

	case IntersectionIteratorType:
		if len(outline.SubOutlines) == 0 {
			return staticEstimate{}, errors.New("StaticAdvisor: intersection with no suboutlines")
		}

		result := staticEstimate{CheckSelectivity: 1}
		for _, sub := range outline.SubOutlines {
			est, err := s.costOutline(sub)
			if err != nil {
				return staticEstimate{}, err
			}

			result.Cardinality += est.Cardinality
			result.CheckCost += est.CheckCost
			result.IterResourcesCost += est.IterResourcesCost
			result.IterSubjectsCost += est.IterSubjectsCost
			result.CheckSelectivity *= est.CheckSelectivity
		}
		return result, nil

	case ExclusionIteratorType:
		if len(outline.SubOutlines) != 2 {
			return staticEstimate{}, errors.New("StaticAdvisor: exclusion requires exactly 2 suboutlines")
		}

		mainEst, err := s.costOutline(outline.SubOutlines[0])
		if err != nil {
			return staticEstimate{}, err
		}
		excludedEst, err := s.costOutline(outline.SubOutlines[1])
		if err != nil {
			return staticEstimate{}, err
		}

		exclusionFactor := max(1.0-excludedEst.CheckSelectivity, 0)

		return staticEstimate{
			Cardinality:       int(float64(mainEst.Cardinality) * exclusionFactor),
			CheckCost:         mainEst.CheckCost + excludedEst.CheckCost,
			CheckSelectivity:  mainEst.CheckSelectivity * exclusionFactor,
			IterResourcesCost: mainEst.IterResourcesCost + excludedEst.IterResourcesCost,
			IterSubjectsCost:  mainEst.IterSubjectsCost + excludedEst.IterSubjectsCost,
		}, nil

	case CaveatIteratorType, AliasIteratorType, RecursiveIteratorType, RecursiveSentinelIteratorType, SelfIteratorType:
		// Pass-through iterators: cost is the cost of the single subiterator
		if len(outline.SubOutlines) == 1 {
			return s.costOutline(outline.SubOutlines[0])
		}
		// For sentinel/self with no subs, return default
		return staticEstimate{
			Cardinality:      1,
			CheckCost:        1,
			CheckSelectivity: s.CheckSelectivity,
		}, nil

	default:
		return staticEstimate{}, spiceerrors.MustBugf("StaticAdvisor: uncovered outline type: %c", outline.Type)
	}
}
