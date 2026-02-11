package query

import (
	"errors"
	"math"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// Estimate represents the estimated worst-case cost and selectivity metrics for an iterator.
// These estimates are used by the query optimizer to make decisions about query plan structure.
//
// Costs are a completely made-up unit, relevant only to the source of the statistics. They are
// not portable between different statistics sources, and are only comparable to each other.
// However, something of zero-cost is rare (and often useless), so a good value for a cost is on the range
// (1, MAXINT)
type Estimate struct {
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

// StatisticsSource provides cost estimates for iterators.
// Implementations can provide static estimates or dynamic estimates based on
// actual datastore statistics.
type StatisticsSource interface {
	// Cost returns a cost estimate for the given iterator.
	Cost(it Iterator) (Estimate, error)
}

// StaticStatistics provides static cost estimates for iterators based on
// configurable parameters. This is useful for basic query planning and
// when dynamic statistics are not available.
//
// Costs are static for StaticStatistics -- we take the base cost of a check to be 1 tuple check.
// For iterating subjects and resources, we take it to iterate all tuples for a given relation.
type StaticStatistics struct {
	// NumberOfTuplesInRelation is the assumed number of tuples in any relation (a complete average).
	NumberOfTuplesInRelation int

	// Fanout is the assumed average number of subjects per resource or
	// resources per subject.
	Fanout int

	// CheckSelectivity is the default probability (0.0-1.0) that a Check
	// operation will return true.
	CheckSelectivity float64
}

// DefaultStaticStatistics returns a StaticStatistics instance with default values
func DefaultStaticStatistics() StaticStatistics {
	return StaticStatistics{
		NumberOfTuplesInRelation: 10,
		Fanout:                   5,
		CheckSelectivity:         0.9,
	}
}

// Cost returns a cost estimate for the given iterator using static assumptions.
// It recursively estimates costs for composite iterators by combining the costs
// of their subiterators according to their operational semantics.
func (s StaticStatistics) Cost(iterator Iterator) (Estimate, error) {
	switch it := iterator.(type) {
	case *FixedIterator:
		return Estimate{
			Cardinality:       len(it.paths),
			CheckCost:         1,
			CheckSelectivity:  s.CheckSelectivity,
			IterResourcesCost: len(it.paths),
			IterSubjectsCost:  len(it.paths),
		}, nil
	case *DatastoreIterator:
		return Estimate{
			Cardinality:       s.NumberOfTuplesInRelation,
			CheckCost:         1,
			CheckSelectivity:  s.CheckSelectivity,
			IterResourcesCost: s.Fanout,
			IterSubjectsCost:  s.Fanout,
		}, nil
	case *ArrowIterator:
		ls, err := s.Cost(it.left)
		if err != nil {
			return Estimate{}, err
		}
		rs, err := s.Cost(it.right)
		if err != nil {
			return Estimate{}, err
		}

		// Calculate CheckCost based on execution direction
		var checkCost int
		var iterResourcesCost int
		var iterSubjectsCost int
		switch it.direction {
		case leftToRight:
			// IterSubjects on left, then Check on right for each result
			checkCost = ls.IterSubjectsCost + (ls.Cardinality * rs.CheckCost)
			// IterResources on left, then IterResources on right for each result
			iterResourcesCost = ls.IterResourcesCost + (ls.Cardinality * rs.IterResourcesCost)
			// IterSubjects on left, then IterSubjects on right for each result
			iterSubjectsCost = ls.IterSubjectsCost + (ls.Cardinality * rs.IterSubjectsCost)
		case rightToLeft:
			// IterResources on right, then Check on left for each result
			checkCost = rs.IterResourcesCost + (rs.Cardinality * ls.CheckCost)
			// IterResources on right, then IterResources on left for each result
			iterResourcesCost = rs.IterResourcesCost + (rs.Cardinality * ls.IterResourcesCost)
			// IterSubjects on right, then IterSubjects on left for each result
			iterSubjectsCost = rs.IterSubjectsCost + (rs.Cardinality * ls.IterSubjectsCost)
		}

		return Estimate{
			// Worst case, an arrow is the size of the cartesian product (full outer join) as we join the two subiterators.
			Cardinality:       ls.Cardinality * rs.Cardinality,
			CheckCost:         checkCost,
			CheckSelectivity:  ls.CheckSelectivity * rs.CheckSelectivity,
			IterResourcesCost: iterResourcesCost,
			IterSubjectsCost:  iterSubjectsCost,
		}, nil
	case *IntersectionArrowIterator:
		ls, err := s.Cost(it.left)
		if err != nil {
			return Estimate{}, err
		}
		rs, err := s.Cost(it.right)
		if err != nil {
			return Estimate{}, err
		}
		// IntersectionArrow also does IterSubjects on left, then Check on right,
		// but only yields results when ALL subjects satisfy (more selective), estimated based on the fanout from IterSubjects
		selectivity := math.Pow(ls.CheckSelectivity, float64(s.Fanout)) * rs.CheckSelectivity
		checkCost := ls.IterSubjectsCost + (ls.Cardinality * rs.CheckCost)
		return Estimate{
			Cardinality:      int(float64(ls.Cardinality*rs.Cardinality) * selectivity),
			CheckCost:        checkCost,
			CheckSelectivity: selectivity,
			// We have to iterResources on both the left and the right side to get the potential
			// candidates, but we then need to check each of the candidates, so we pay an additional
			// check cost.
			IterResourcesCost: ls.IterResourcesCost + (ls.Cardinality * rs.IterResourcesCost) + checkCost,
			IterSubjectsCost:  ls.IterSubjectsCost + (ls.Cardinality * rs.IterSubjectsCost),
		}, nil
	case *UnionIterator:
		if len(it.subIts) == 0 {
			return Estimate{}, errors.New("StaticStatistics: union with no subiterators")
		}

		// Union sums up all subiterator costs
		result := Estimate{}
		for _, subIt := range it.subIts {
			est, err := s.Cost(subIt)
			if err != nil {
				return Estimate{}, err
			}
			result.Cardinality += est.Cardinality
			result.CheckCost += est.CheckCost
			result.IterResourcesCost += est.IterResourcesCost
			result.IterSubjectsCost += est.IterSubjectsCost
			result.CheckSelectivity = max(est.CheckSelectivity, result.CheckSelectivity)
		}
		return result, nil
	case *IntersectionIterator:
		if len(it.subIts) == 0 {
			return Estimate{}, errors.New("StaticStatistics: intersection with no subiterators")
		}

		// Intersection: costs add up, but cardinality decreases with each iterator
		result := Estimate{CheckSelectivity: 1}
		for _, subIt := range it.subIts {
			est, err := s.Cost(subIt)
			if err != nil {
				return Estimate{}, err
			}

			result.Cardinality += est.Cardinality
			result.CheckCost += est.CheckCost
			result.IterResourcesCost += est.IterResourcesCost
			result.IterSubjectsCost += est.IterSubjectsCost
			result.CheckSelectivity *= est.CheckSelectivity
		}
		return result, nil
	case *ExclusionIterator:
		// Exclusion: A - B
		// We need to check both sides, but the result is filtered
		mainEst, err := s.Cost(it.mainSet)
		if err != nil {
			return Estimate{}, err
		}
		excludedEst, err := s.Cost(it.excluded)
		if err != nil {
			return Estimate{}, err
		}

		// Cardinality: main minus the excluded portion
		// Approximate as: main.Cardinality * (1 - excluded.CheckSelectivity)
		exclusionFactor := max(1.0-excludedEst.CheckSelectivity, 0)

		return Estimate{
			Cardinality:       int(float64(mainEst.Cardinality) * exclusionFactor),
			CheckCost:         mainEst.CheckCost + excludedEst.CheckCost,
			CheckSelectivity:  mainEst.CheckSelectivity * exclusionFactor,
			IterResourcesCost: mainEst.IterResourcesCost + excludedEst.IterResourcesCost,
			IterSubjectsCost:  mainEst.IterSubjectsCost + excludedEst.IterSubjectsCost,
		}, nil
	default:
		subs := iterator.Subiterators()
		if len(subs) == 1 {
			// We have any subiterators and are not a covered case.
			// We will pass through by default
			return s.Cost(subs[0])
		}
	}
	return Estimate{}, spiceerrors.MustBugf("StaticStatistics: uncovered combined iterator type")
}
