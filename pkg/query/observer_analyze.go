package query

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// AnalyzeStats collects the number of operations performed for each iterator as a query takes place,
// including both counts and timing information.
type AnalyzeStats struct {
	CountStats        // embedded: CheckCalls, IterSubjectsCalls, etc.
	CheckTime         time.Duration
	IterSubjectsTime  time.Duration
	IterResourcesTime time.Duration
}

// AnalyzeObserver is a thread-safe Observer that collects execution statistics keyed by CanonicalKey.
// It embeds a CountObserver for tracking calls and results, and adds timing information.
type AnalyzeObserver struct {
	*CountObserver // handles call and result counts
	mu             sync.Mutex
	startTimes     map[ObserverOperation]map[CanonicalKey]time.Time // GUARDED_BY(mu)
	timings        map[CanonicalKey]AnalyzeStats                    // GUARDED_BY(mu) - stores timing fields only
}

// NewAnalyzeObserver creates a new thread-safe analyze observer.
func NewAnalyzeObserver() *AnalyzeObserver {
	return &AnalyzeObserver{
		CountObserver: NewCountObserver(),
		startTimes:    make(map[ObserverOperation]map[CanonicalKey]time.Time),
		timings:       make(map[CanonicalKey]AnalyzeStats),
	}
}

// ObserveEnterIterator increments the call counter (via CountObserver) and records the start time.
func (a *AnalyzeObserver) ObserveEnterIterator(op ObserverOperation, key CanonicalKey) {
	// Delegate to CountObserver for call counting
	a.CountObserver.ObserveEnterIterator(op, key)

	// Record start time for timing measurement
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.startTimes[op] == nil {
		a.startTimes[op] = make(map[CanonicalKey]time.Time)
	}
	a.startTimes[op][key] = time.Now()
}

// ObservePath increments the result counter (via CountObserver).
func (a *AnalyzeObserver) ObservePath(op ObserverOperation, key CanonicalKey, path Path) {
	// Delegate entirely to CountObserver for result counting
	a.CountObserver.ObservePath(op, key, path)
}

// ObserveReturnIterator records elapsed time since ObserveEnterIterator was called.
// Note: CountObserver.ObserveReturnIterator is a no-op, so we don't call it here.
func (a *AnalyzeObserver) ObserveReturnIterator(op ObserverOperation, key CanonicalKey) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if opTimes := a.startTimes[op]; opTimes != nil {
		if start, ok := opTimes[key]; ok {
			elapsed := time.Since(start)
			stats := a.timings[key]
			switch op {
			case CheckOperation:
				stats.CheckTime += elapsed
			case IterSubjectsOperation:
				stats.IterSubjectsTime += elapsed
			case IterResourcesOperation:
				stats.IterResourcesTime += elapsed
			}
			a.timings[key] = stats
			delete(opTimes, key)
		}
	}
}

// GetStats returns a copy of all collected stats, merging counts from CountObserver with timings.
func (a *AnalyzeObserver) GetStats() map[CanonicalKey]AnalyzeStats {
	// Get counts from embedded CountObserver
	countStats := a.CountObserver.GetStats()

	a.mu.Lock()
	defer a.mu.Unlock()

	// Merge counts with timings
	result := make(map[CanonicalKey]AnalyzeStats, len(countStats))
	for key, counts := range countStats {
		stats := AnalyzeStats{CountStats: counts}
		if timing, ok := a.timings[key]; ok {
			stats.CheckTime = timing.CheckTime
			stats.IterSubjectsTime = timing.IterSubjectsTime
			stats.IterResourcesTime = timing.IterResourcesTime
		}
		result[key] = stats
	}
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
	// Extract CountStats for aggregation
	counts := make(map[CanonicalKey]CountStats, len(analyze))
	for k, v := range analyze {
		counts[k] = v.CountStats
	}

	// Aggregate counts using CountStats aggregator
	total := AnalyzeStats{
		CountStats: AggregateCountStats(counts),
	}

	// Aggregate timings
	for _, stats := range analyze {
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
