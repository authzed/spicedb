package query

import "sync"

// QueryPlanMetadata aggregates CountStats keyed by iterator CanonicalKey across
// requests. The accumulated stats feed back into plan compilation via
// ApplyAdvisor, letting the count-based advisor make data-driven decisions
// (e.g. arrow direction reversal) on subsequent runs.
//
// Safe for concurrent use.
type QueryPlanMetadata struct {
	mu    sync.Mutex
	stats map[CanonicalKey]CountStats // GUARDED_BY(mu)
}

// NewQueryPlanMetadata creates a new QueryPlanMetadata tracker.
func NewQueryPlanMetadata() *QueryPlanMetadata {
	return &QueryPlanMetadata{
		stats: make(map[CanonicalKey]CountStats),
	}
}

// MergeCountStats merges per-key CountStats produced by a CountObserver into
// the aggregated metadata. Counts are summed.
func (m *QueryPlanMetadata) MergeCountStats(counts map[CanonicalKey]CountStats) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, newStats := range counts {
		existing := m.stats[key]
		existing.CheckCalls += newStats.CheckCalls
		existing.IterSubjectsCalls += newStats.IterSubjectsCalls
		existing.IterResourcesCalls += newStats.IterResourcesCalls
		existing.CheckResults += newStats.CheckResults
		existing.IterSubjectsResults += newStats.IterSubjectsResults
		existing.IterResourcesResults += newStats.IterResourcesResults
		m.stats[key] = existing
	}
}

// GetStats returns a copy of all aggregated stats.
func (m *QueryPlanMetadata) GetStats() map[CanonicalKey]CountStats {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make(map[CanonicalKey]CountStats, len(m.stats))
	for k, v := range m.stats {
		result[k] = v
	}
	return result
}

// ApplyAdvisor applies a CountAdvisor built from the accumulated stats to the
// outline. If no stats have been collected yet, the outline is returned
// unmodified.
func (m *QueryPlanMetadata) ApplyAdvisor(co CanonicalOutline) (CanonicalOutline, error) {
	stats := m.GetStats()
	if len(stats) == 0 {
		return co, nil
	}
	return ApplyAdvisor(co, NewCountAdvisor(stats))
}

// ReAdviseIterator walks a compiled iterator tree and refreshes per-iterator
// optimization choices against the locally accumulated stats. Structurally
// inert — only hint-level fields (currently ArrowIterator.direction) change,
// so the iterator's CanonicalKey is preserved and the cache key the receiver
// computed from req.Plan bytes remains valid.
//
// Used by the dispatch receiver: the sender baked its hints into the wire
// bytes against its stats; ReAdviseIterator gives the receiver one shot to
// adjust those choices against its own stats before execution.
func (m *QueryPlanMetadata) ReAdviseIterator(it Iterator) {
	stats := m.GetStats()
	if len(stats) == 0 {
		return
	}
	reAdviseIterator(it, stats)
}

// reAdviseIterator is the recursive worker for ReAdviseIterator. Walks the
// iterator tree depth-first; for each ArrowIterator pulls its left/right
// sub-iterators' canonical keys and updates the direction field directly via
// chooseArrowDirection. No new iterators allocated, no decompile/recompile.
func reAdviseIterator(it Iterator, stats map[CanonicalKey]CountStats) {
	if arrow, ok := it.(*ArrowIterator); ok {
		subs := arrow.Subiterators()
		if len(subs) == 2 {
			arrow.direction = chooseArrowDirection(subs[0].CanonicalKey(), subs[1].CanonicalKey(), stats)
		}
	}
	for _, sub := range it.Subiterators() {
		reAdviseIterator(sub, stats)
	}
}
