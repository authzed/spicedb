package query

import (
	"fmt"
	"maps"
	"strings"
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
	mu         sync.Mutex
	stats      map[CanonicalKey]AnalyzeStats                    // GUARDED_BY(mu)
	startTimes map[ObserverOperation]map[CanonicalKey]time.Time // GUARDED_BY(mu)
}

// NewAnalyzeObserver creates a new thread-safe analyze observer.
func NewAnalyzeObserver() *AnalyzeObserver {
	return &AnalyzeObserver{
		stats:      make(map[CanonicalKey]AnalyzeStats),
		startTimes: make(map[ObserverOperation]map[CanonicalKey]time.Time),
	}
}

// ObserveEnterIterator increments the call counter and records the start time.
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

	if a.startTimes[op] == nil {
		a.startTimes[op] = make(map[CanonicalKey]time.Time)
	}
	a.startTimes[op][key] = time.Now()
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

// ObserveReturnIterator records elapsed time since ObserveEnterIterator was called.
func (a *AnalyzeObserver) ObserveReturnIterator(op ObserverOperation, key CanonicalKey) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if opTimes := a.startTimes[op]; opTimes != nil {
		if start, ok := opTimes[key]; ok {
			elapsed := time.Since(start)
			stats := a.stats[key]
			switch op {
			case CheckOperation:
				stats.CheckTime += elapsed
			case IterSubjectsOperation:
				stats.IterSubjectsTime += elapsed
			case IterResourcesOperation:
				stats.IterResourcesTime += elapsed
			}
			a.stats[key] = stats
			delete(opTimes, key)
		}
	}
}

// GetStats returns a copy of all collected stats.
func (a *AnalyzeObserver) GetStats() map[CanonicalKey]AnalyzeStats {
	a.mu.Lock()
	defer a.mu.Unlock()

	result := make(map[CanonicalKey]AnalyzeStats, len(a.stats))
	maps.Copy(result, a.stats)
	return result
}

// FormatAnalysis returns a formatted string showing the iterator tree with execution statistics
// for each iterator. Stats are looked up by iterator canonical key from the analyze map.
func FormatAnalysis(tree Iterator, analyze map[CanonicalKey]AnalyzeStats) string {
	if tree == nil {
		return "No iterator tree provided"
	}
	if len(analyze) == 0 {
		return "No analysis data available"
	}

	var sb strings.Builder
	formatNode(tree, analyze, &sb, "", true)
	return sb.String()
}

// AggregateAnalyzeStats combines all the analyze stats from a map into a single
// aggregated AnalyzeStats. This is useful for getting total counts across all
// iterators in a query execution.
func AggregateAnalyzeStats(analyze map[CanonicalKey]AnalyzeStats) AnalyzeStats {
	var total AnalyzeStats
	for _, stats := range analyze {
		total.CheckCalls += stats.CheckCalls
		total.IterSubjectsCalls += stats.IterSubjectsCalls
		total.IterResourcesCalls += stats.IterResourcesCalls
		total.CheckResults += stats.CheckResults
		total.IterSubjectsResults += stats.IterSubjectsResults
		total.IterResourcesResults += stats.IterResourcesResults
		total.CheckTime += stats.CheckTime
		total.IterSubjectsTime += stats.IterSubjectsTime
		total.IterResourcesTime += stats.IterResourcesTime
	}
	return total
}

// formatNode recursively formats a single iterator node and its children
// depth parameter tracks how deep we are in the tree (0 = root)
func formatNode(it Iterator, analyze map[CanonicalKey]AnalyzeStats, sb *strings.Builder, indent string, isLast bool) {
	if it == nil {
		return
	}

	// Get iterator info from Explain()
	explain := it.Explain()
	iterKey := it.CanonicalKey()
	stats := analyze[iterKey]

	// Draw tree branch for non-root nodes
	// Root nodes have both empty indent and are drawn without any prefix
	// Check if we're a child by seeing if we have any indentation OR if this is being called recursively
	isRoot := indent == "" && !strings.Contains(sb.String(), "Calls:")

	if !isRoot {
		if isLast {
			sb.WriteString(indent + "└─ ")
		} else {
			sb.WriteString(indent + "├─ ")
		}
	}

	// Write iterator name and key (truncated for readability)
	fmt.Fprintf(sb, "%s (Hash: %016x)\n", explain.Info, iterKey.Hash())

	// Write stats with indentation
	statsIndent := indent
	if indent != "" {
		if isLast {
			statsIndent += "   "
		} else {
			statsIndent += "│  "
		}
	}

	// Format call counts
	sb.WriteString(statsIndent + fmt.Sprintf("   Calls: Check=%d, IterSubjects=%d, IterResources=%d\n",
		stats.CheckCalls, stats.IterSubjectsCalls, stats.IterResourcesCalls))

	// Format result counts
	sb.WriteString(statsIndent + fmt.Sprintf("   Results: Check=%d, IterSubjects=%d, IterResources=%d\n",
		stats.CheckResults, stats.IterSubjectsResults, stats.IterResourcesResults))

	// Format timing
	sb.WriteString(statsIndent + fmt.Sprintf("   Time: Check=%v, IterSubjects=%v, IterResources=%v\n",
		stats.CheckTime, stats.IterSubjectsTime, stats.IterResourcesTime))

	// Recursively format children
	subIts := it.Subiterators()
	if len(subIts) > 0 {
		var childIndent string
		switch {
		case indent == "":
			// Root node - children get no indent prefix, but will show tree chars
			childIndent = ""
		case isLast:
			childIndent = indent + "   "
		default:
			childIndent = indent + "│  "
		}

		for i, subIt := range subIts {
			formatNode(subIt, analyze, sb, childIndent, i == len(subIts)-1)
		}
	}
}
