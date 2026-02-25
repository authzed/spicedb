package query

// optimizeArrowDirection chooses the optimal execution direction for Arrow iterators
// based on cost estimates from statistics
func optimizeArrowDirection(source StatisticsSource) TypedOptimizerFunc[*ArrowIterator] {
	return func(a *ArrowIterator) (Iterator, bool, error) {
		if source == nil {
			// No statistics available, keep default
			return a, false, nil
		}

		// Get cost estimates for both strategies
		leftEst, err := source.Cost(a.left)
		if err != nil {
			return nil, false, err
		}

		rightEst, err := source.Cost(a.right)
		if err != nil {
			return nil, false, err
		}

		// Cost for left-to-right: IterSubjects(left) + Check(right) for each result
		leftToRightCost := leftEst.IterSubjectsCost + (leftEst.Cardinality * rightEst.CheckCost)

		// Cost for right-to-left: IterResources(right) + Check(left) for each result
		rightToLeftCost := rightEst.IterResourcesCost + (rightEst.Cardinality * leftEst.CheckCost)

		// Choose the cheaper direction
		if rightToLeftCost < leftToRightCost && a.direction != rightToLeft {
			// Create new arrow with inverted direction
			newArrow := &ArrowIterator{
				canonicalKey:  a.canonicalKey,
				left:          a.left,
				right:         a.right,
				direction:     rightToLeft,
				isSchemaArrow: a.isSchemaArrow,
			}
			return newArrow, true, nil // Changed
		}

		// Keep current direction
		return a, false, nil // No change
	}
}
