package query

// defaultArrowFanout is a baseline fan-out to assume when there is no data for a direction.
// Put another way, it has to be at least this bad on average to consider flipping the arrow direction.
const defaultArrowFanout = 3.0

// CountAdvisor uses observed CountStats keyed by CanonicalKey to hint whether
// arrow iterators should be reversed. It compares the IterSubjectsResults of the
// left subtree against the IterResourcesResults of the right subtree: if the left
// fan-out is, on average, wider than the right fan-out, starting from the right is likely cheaper.
type CountAdvisor struct {
	stats map[CanonicalKey]CountStats
}

// NewCountAdvisor creates a CountAdvisor from a snapshot of observed stats.
func NewCountAdvisor(stats map[CanonicalKey]CountStats) *CountAdvisor {
	return &CountAdvisor{stats: stats}
}

// GetHints returns an ArrowDirectionHint for arrow nodes when observed result
// ratios suggest reversal is beneficial. For all other node types it returns nil.
func (a *CountAdvisor) GetHints(outline Outline, keySource CanonicalKeySource) ([]Hint, error) {
	if outline.Type != ArrowIteratorType || len(outline.SubOutlines) != 2 {
		return nil, nil
	}
	leftKey := keySource.GetCanonicalKey(outline.SubOutlines[0].ID)
	rightKey := keySource.GetCanonicalKey(outline.SubOutlines[1].ID)
	return []Hint{ArrowDirectionHint(chooseArrowDirection(leftKey, rightKey, a.stats))}, nil
}

// chooseArrowDirection picks the arrow execution direction by comparing
// observed left/right fan-outs. Shared between the outline-time advisor
// (compile-time hint emission) and the iterator-time re-advise on dispatch
// receipt — both need the same comparison logic against the same stats map,
// just sourced from different points in the pipeline.
func chooseArrowDirection(leftKey, rightKey CanonicalKey, stats map[CanonicalKey]CountStats) arrowDirection {
	leftStats := stats[leftKey]
	rightStats := stats[rightKey]

	leftFanout := defaultArrowFanout
	rightFanout := defaultArrowFanout

	if leftStats.IterSubjectsCalls != 0 {
		leftFanout = float64(leftStats.IterSubjectsResults) / float64(leftStats.IterSubjectsCalls)
	}
	if rightStats.IterResourcesCalls != 0 {
		rightFanout = float64(rightStats.IterResourcesResults) / float64(rightStats.IterResourcesCalls)
	}

	if rightFanout < leftFanout {
		return rightToLeft
	}
	return leftToRight
}

// GetMutations is a stub — no structural mutations from count data yet.
func (a *CountAdvisor) GetMutations(outline Outline, keySource CanonicalKeySource) ([]OutlineMutation, error) {
	return nil, nil
}
