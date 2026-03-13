package query

import (
	"maps"
	"sync"
)

// CountStats collects the number of calls and results for each of the three
// iterator operations (Check, IterSubjects, IterResources).
type CountStats struct {
	CheckCalls           int
	IterSubjectsCalls    int
	IterResourcesCalls   int
	CheckResults         int
	IterSubjectsResults  int
	IterResourcesResults int
}

// CountObserver is a thread-safe Observer that collects call and result counts
// keyed by CanonicalKey. It does not record timing information.
type CountObserver struct {
	mu    sync.Mutex
	stats map[CanonicalKey]CountStats // GUARDED_BY(mu)
}

// NewCountObserver creates a new thread-safe count observer.
func NewCountObserver() *CountObserver {
	return &CountObserver{
		stats: make(map[CanonicalKey]CountStats),
	}
}

// ObserveEnterIterator increments the call counter for the given operation and key.
func (c *CountObserver) ObserveEnterIterator(op ObserverOperation, key CanonicalKey) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stats := c.stats[key]
	switch op {
	case CheckOperation:
		stats.CheckCalls++
	case IterSubjectsOperation:
		stats.IterSubjectsCalls++
	case IterResourcesOperation:
		stats.IterResourcesCalls++
	}
	c.stats[key] = stats
}

// ObservePath increments the result counter for the given operation and key.
func (c *CountObserver) ObservePath(op ObserverOperation, key CanonicalKey, _ *Path) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stats := c.stats[key]
	switch op {
	case CheckOperation:
		stats.CheckResults++
	case IterSubjectsOperation:
		stats.IterSubjectsResults++
	case IterResourcesOperation:
		stats.IterResourcesResults++
	}
	c.stats[key] = stats
}

// ObserveReturnIterator is a no-op for CountObserver; no timing is recorded.
func (c *CountObserver) ObserveReturnIterator(_ ObserverOperation, _ CanonicalKey) {}

// GetStats returns a copy of all collected count stats.
func (c *CountObserver) GetStats() map[CanonicalKey]CountStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := make(map[CanonicalKey]CountStats, len(c.stats))
	maps.Copy(result, c.stats)
	return result
}

// AggregateCountStats combines all the count stats from a map into a single
// aggregated CountStats. This is useful for getting total counts across all
// iterators in a query execution.
func AggregateCountStats(counts map[CanonicalKey]CountStats) CountStats {
	var total CountStats
	for _, s := range counts {
		total.CheckCalls += s.CheckCalls
		total.IterSubjectsCalls += s.IterSubjectsCalls
		total.IterResourcesCalls += s.IterResourcesCalls
		total.CheckResults += s.CheckResults
		total.IterSubjectsResults += s.IterSubjectsResults
		total.IterResourcesResults += s.IterResourcesResults
	}
	return total
}
