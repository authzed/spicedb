package query

import (
	"fmt"
	"strings"
)

// FormatAnalysis returns a formatted string showing the iterator tree with execution statistics
// for each iterator. Stats are looked up by iterator ID from the analyze map.
func FormatAnalysis(tree Iterator, analyze map[string]AnalyzeStats) string {
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
func AggregateAnalyzeStats(analyze map[string]AnalyzeStats) AnalyzeStats {
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
func formatNode(it Iterator, analyze map[string]AnalyzeStats, sb *strings.Builder, indent string, isLast bool) {
	if it == nil {
		return
	}

	// Get iterator info from Explain()
	explain := it.Explain()
	iterID := it.ID()
	stats := analyze[iterID]

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

	// Write iterator name and ID (truncated for readability)
	fmt.Fprintf(sb, "%s (ID: %s)\n", explain.Info, iterID[:8])

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
