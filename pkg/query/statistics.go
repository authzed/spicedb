package query

import (
	"errors"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// Estimate represents the estimated cost and selectivity metrics for an iterator.
// These estimates are used by the query optimizer to make decisions about query plan structure.
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
type StaticStatistics struct {
	// DatastoreSize is the assumed number of tuples in a relation.
	DatastoreSize int

	// Fanout is the assumed average number of subjects per resource or
	// resources per subject.
	Fanout int

	// CheckSelectivity is the default probability (0.0-1.0) that a Check
	// operation will return true.
	CheckSelectivity float64

	// IntersectionArrowReduction is an additional reduction factor for
	// IntersectionArrow selectivity to account for "all subjects" semantics.
	IntersectionArrowReduction float64
}

// DefaultStaticStatistics returns a StaticStatistics instance with default values
func DefaultStaticStatistics() StaticStatistics {
	return StaticStatistics{
		DatastoreSize:              10,
		Fanout:                     5,
		CheckSelectivity:           0.9,
		IntersectionArrowReduction: 0.5,
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
			CheckCost:         10,
			CheckSelectivity:  s.CheckSelectivity,
			IterResourcesCost: len(it.paths),
			IterSubjectsCost:  len(it.paths),
		}, nil
	case *RelationIterator:
		return Estimate{
			Cardinality:       s.DatastoreSize,
			CheckCost:         s.DatastoreSize,
			CheckSelectivity:  s.CheckSelectivity,
			IterResourcesCost: s.DatastoreSize * s.Fanout,
			IterSubjectsCost:  s.DatastoreSize * s.Fanout,
		}, nil
	case *Arrow:
		ls, err := s.Cost(it.left)
		if err != nil {
			return Estimate{}, err
		}
		rs, err := s.Cost(it.right)
		if err != nil {
			return Estimate{}, err
		}
		// Arrow does IterSubjects on left (ls.Cardinality iterations),
		// then Check on right for each subject (rs.CheckCost per iteration)
		return Estimate{
			Cardinality:       ls.Cardinality * rs.Cardinality,
			CheckCost:         ls.IterSubjectsCost + (ls.Cardinality * rs.CheckCost),
			CheckSelectivity:  ls.CheckSelectivity * rs.CheckSelectivity,
			IterResourcesCost: ls.IterResourcesCost + (ls.Cardinality * rs.IterResourcesCost),
			IterSubjectsCost:  ls.IterSubjectsCost + (ls.Cardinality * rs.IterSubjectsCost),
		}, nil
	case *IntersectionArrow:
		ls, err := s.Cost(it.left)
		if err != nil {
			return Estimate{}, err
		}
		rs, err := s.Cost(it.right)
		if err != nil {
			return Estimate{}, err
		}
		// IntersectionArrow also does IterSubjects on left, then Check on right,
		// but only yields results when ALL subjects satisfy (more selective)
		selectivity := ls.CheckSelectivity * rs.CheckSelectivity * s.IntersectionArrowReduction // Additional reduction for "all" semantics
		return Estimate{
			Cardinality:       int(float64(ls.Cardinality*rs.Cardinality) * selectivity),
			CheckCost:         ls.IterSubjectsCost + (ls.Cardinality * rs.CheckCost),
			CheckSelectivity:  selectivity,
			IterResourcesCost: ls.IterResourcesCost + (ls.Cardinality * rs.IterResourcesCost),
			IterSubjectsCost:  ls.IterSubjectsCost + (ls.Cardinality * rs.IterSubjectsCost),
		}, nil
	case *Union:
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
			if est.CheckSelectivity > result.CheckSelectivity {
				result.CheckSelectivity = est.CheckSelectivity
			}
		}
		return result, nil
	case *Intersection:
		if len(it.subIts) == 0 {
			return Estimate{}, errors.New("StaticStatistics: intersection with no subiterators")
		}

		// Intersection: costs add up, but cardinality decreases with each iterator
		result := Estimate{CheckSelectivity: 1}
		for idx, subIt := range it.subIts {
			est, err := s.Cost(subIt)
			if err != nil {
				return Estimate{}, err
			}

			if idx == 0 {
				result.Cardinality = est.Cardinality
			} else {
				// Each subsequent intersection filters down the results
				result.Cardinality = int(float64(result.Cardinality) * est.CheckSelectivity)
			}

			result.CheckCost += est.CheckCost
			result.IterResourcesCost += est.IterResourcesCost
			result.IterSubjectsCost += est.IterSubjectsCost
			result.CheckSelectivity *= est.CheckSelectivity
		}
		return result, nil
	case *Exclusion:
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
		exclusionFactor := 1.0 - excludedEst.CheckSelectivity
		if exclusionFactor < 0 {
			exclusionFactor = 0
		}

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
