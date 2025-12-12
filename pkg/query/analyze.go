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
