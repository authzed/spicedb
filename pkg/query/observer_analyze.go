package query

import (
	"maps"
	"sync"
	"time"
)

// AnalyzeStats collects the number of operations performed for each iterator as a query takes place.
type AnalyzeStats struct {
	CheckCalls           int
	IterSubjectsCalls    int
	IterResourcesCalls   int
	CheckResults         int
	IterSubjectsResults  int
	IterResourcesResults int
	CheckTime            time.Duration
	IterSubjectsTime     time.Duration
	IterResourcesTime    time.Duration
}

// AnalyzeObserver is a thread-safe Observer that collects execution statistics keyed by CanonicalKey.
type AnalyzeObserver struct {
	mu    sync.Mutex
	stats map[CanonicalKey]AnalyzeStats // GUARDED_BY(mu)
}

// NewAnalyzeObserver creates a new thread-safe analyze observer.
func NewAnalyzeObserver() *AnalyzeObserver {
	return &AnalyzeObserver{
		stats: make(map[CanonicalKey]AnalyzeStats),
	}
}

// ObserveEnterIterator increments the call counter for the given iterator key and operation.
func (a *AnalyzeObserver) ObserveEnterIterator(op ObserverOperation, key CanonicalKey) {
	a.mu.Lock()
	defer a.mu.Unlock()

	stats := a.stats[key]
	switch op {
	case CheckOperation:
		stats.CheckCalls++
	case IterSubjectsOperation:
		stats.IterSubjectsCalls++
	case IterResourcesOperation:
		stats.IterResourcesCalls++
	}
	a.stats[key] = stats
}

// ObservePath increments the result counter for the given iterator key and operation.
func (a *AnalyzeObserver) ObservePath(op ObserverOperation, key CanonicalKey, _ Path) {
	a.mu.Lock()
	defer a.mu.Unlock()

	stats := a.stats[key]
	switch op {
	case CheckOperation:
		stats.CheckResults++
	case IterSubjectsOperation:
		stats.IterSubjectsResults++
	case IterResourcesOperation:
		stats.IterResourcesResults++
	}
	a.stats[key] = stats
}

// ObserveTiming records the elapsed time for the given iterator key and operation.
func (a *AnalyzeObserver) ObserveTiming(op ObserverOperation, key CanonicalKey, dur time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()

	stats := a.stats[key]
	switch op {
	case CheckOperation:
		stats.CheckTime += dur
	case IterSubjectsOperation:
		stats.IterSubjectsTime += dur
	case IterResourcesOperation:
		stats.IterResourcesTime += dur
	}
	a.stats[key] = stats
}

// ObserveReturnIterator is a no-op for the analyze observer.
func (a *AnalyzeObserver) ObserveReturnIterator(_ ObserverOperation, _ CanonicalKey) {}

// GetStats returns a copy of all collected stats.
func (a *AnalyzeObserver) GetStats() map[CanonicalKey]AnalyzeStats {
	a.mu.Lock()
	defer a.mu.Unlock()

	result := make(map[CanonicalKey]AnalyzeStats, len(a.stats))
	maps.Copy(result, a.stats)
	return result
}
