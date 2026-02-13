package query

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
)

func TestFormatAnalysisSimpleTree(t *testing.T) {
	// Create a simple iterator tree
	fixed := NewFixedIterator(
		Path{
			Resource: Object{ObjectType: "document", ObjectID: "doc1"},
			Subject:  ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."},
		},
		Path{
			Resource: Object{ObjectType: "document", ObjectID: "doc2"},
			Subject:  ObjectAndRelation{ObjectType: "user", ObjectID: "bob", Relation: "..."},
		},
	)

	// Create analyze map with stats
	analyze := map[string]AnalyzeStats{
		fixed.ID(): {
			CheckCalls:   1,
			CheckResults: 2,
		},
	}

	// Format analysis
	output := FormatAnalysis(fixed, analyze)

	// Verify output contains expected info
	require.Contains(t, output, "Fixed(2 paths)")
	require.Contains(t, output, "Calls: Check=1, IterSubjects=0, IterResources=0")
	require.Contains(t, output, "Results: Check=2, IterSubjects=0, IterResources=0")
}

func TestFormatAnalysisNestedTree(t *testing.T) {
	// Create a nested iterator tree: Union with two FixedIterators
	fixed1 := NewFixedIterator(
		Path{
			Resource: Object{ObjectType: "document", ObjectID: "doc1"},
			Subject:  ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."},
		},
	)

	fixed2 := NewFixedIterator(
		Path{
			Resource: Object{ObjectType: "document", ObjectID: "doc2"},
			Subject:  ObjectAndRelation{ObjectType: "user", ObjectID: "bob", Relation: "..."},
		},
	)

	union := NewUnionIterator(fixed1, fixed2)

	// Create analyze map with stats for all iterators
	analyze := map[string]AnalyzeStats{
		union.ID(): {
			CheckCalls:   1,
			CheckResults: 2,
		},
		fixed1.ID(): {
			CheckCalls:   1,
			CheckResults: 1,
		},
		fixed2.ID(): {
			CheckCalls:   1,
			CheckResults: 1,
		},
	}

	// Format analysis
	output := FormatAnalysis(union, analyze)

	// Verify tree structure
	require.Contains(t, output, "Union")
	require.Contains(t, output, "├─")
	require.Contains(t, output, "└─")
	require.Contains(t, output, "Fixed(1 paths)")

	// Verify stats are present for all nodes
	lines := strings.Split(output, "\n")
	callsCount := 0
	resultsCount := 0
	for _, line := range lines {
		if strings.Contains(line, "Calls:") {
			callsCount++
		}
		if strings.Contains(line, "Results:") {
			resultsCount++
		}
	}
	require.Equal(t, 3, callsCount, "Should have stats for 3 iterators")
	require.Equal(t, 3, resultsCount, "Should have stats for 3 iterators")
}

func TestFormatAnalysisEdgeCases(t *testing.T) {
	t.Run("nil tree", func(t *testing.T) {
		output := FormatAnalysis(nil, map[string]AnalyzeStats{})
		require.Equal(t, "No iterator tree provided", output)
	})

	t.Run("nil analyze map", func(t *testing.T) {
		fixed := NewFixedIterator()
		output := FormatAnalysis(fixed, nil)
		require.Equal(t, "No analysis data available", output)
	})

	t.Run("empty analyze map", func(t *testing.T) {
		fixed := NewFixedIterator()
		output := FormatAnalysis(fixed, map[string]AnalyzeStats{})
		require.Equal(t, "No analysis data available", output)
	})
}

func TestAnalysisIntegration(t *testing.T) {
	// Create a datastore
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	// Create a simple iterator
	fixed := NewFixedIterator(
		Path{
			Resource: Object{ObjectType: "document", ObjectID: "doc1"},
			Subject:  ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."},
		},
		Path{
			Resource: Object{ObjectType: "document", ObjectID: "doc2"},
			Subject:  ObjectAndRelation{ObjectType: "user", ObjectID: "bob", Relation: "..."},
		},
	)

	// Create a context with analysis enabled
	analyze := NewAnalyzeCollector()
	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)),
		WithAnalyze(analyze))

	// Execute a Check operation
	resources := []Object{{ObjectType: "document", ObjectID: "doc1"}}
	subject := ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."}
	pathSeq, err := ctx.Check(fixed, resources, subject)
	require.NoError(t, err)

	// Consume the results
	paths, err := CollectAll(pathSeq)
	require.NoError(t, err)
	require.Len(t, paths, 1)

	// Verify stats were recorded
	analyzeStats := analyze.GetStats()
	stats, exists := analyzeStats[fixed.ID()]
	require.True(t, exists, "Stats should exist for iterator")
	require.Equal(t, 1, stats.CheckCalls, "Should have 1 Check call")
	require.Equal(t, 1, stats.CheckResults, "Should have 1 Check result")

	// Format and verify analysis output
	output := FormatAnalysis(fixed, analyzeStats)
	require.Contains(t, output, "Fixed(2 paths)")
	require.Contains(t, output, "Calls: Check=1")
	require.Contains(t, output, "Results: Check=1")
}

func TestAggregateAnalyzeStats(t *testing.T) {
	t.Run("empty map", func(t *testing.T) {
		result := AggregateAnalyzeStats(map[string]AnalyzeStats{})
		require.Equal(t, AnalyzeStats{}, result)
	})

	t.Run("single entry", func(t *testing.T) {
		stats := map[string]AnalyzeStats{
			"id1": {
				CheckCalls:           5,
				IterSubjectsCalls:    3,
				IterResourcesCalls:   2,
				CheckResults:         10,
				IterSubjectsResults:  6,
				IterResourcesResults: 4,
			},
		}
		result := AggregateAnalyzeStats(stats)
		require.Equal(t, stats["id1"], result)
	})

	t.Run("multiple entries", func(t *testing.T) {
		stats := map[string]AnalyzeStats{
			"id1": {
				CheckCalls:           5,
				IterSubjectsCalls:    3,
				IterResourcesCalls:   2,
				CheckResults:         10,
				IterSubjectsResults:  6,
				IterResourcesResults: 4,
			},
			"id2": {
				CheckCalls:           3,
				IterSubjectsCalls:    2,
				IterResourcesCalls:   1,
				CheckResults:         6,
				IterSubjectsResults:  4,
				IterResourcesResults: 2,
			},
			"id3": {
				CheckCalls:           2,
				IterSubjectsCalls:    1,
				IterResourcesCalls:   1,
				CheckResults:         4,
				IterSubjectsResults:  2,
				IterResourcesResults: 2,
			},
		}
		result := AggregateAnalyzeStats(stats)
		expected := AnalyzeStats{
			CheckCalls:           10,
			IterSubjectsCalls:    6,
			IterResourcesCalls:   4,
			CheckResults:         20,
			IterSubjectsResults:  12,
			IterResourcesResults: 8,
		}
		require.Equal(t, expected, result)
	})

	t.Run("multiple entries with timing", func(t *testing.T) {
		stats := map[string]AnalyzeStats{
			"id1": {
				CheckCalls:           5,
				IterSubjectsCalls:    3,
				IterResourcesCalls:   2,
				CheckResults:         10,
				IterSubjectsResults:  6,
				IterResourcesResults: 4,
				CheckTime:            100 * time.Millisecond,
				IterSubjectsTime:     50 * time.Millisecond,
				IterResourcesTime:    25 * time.Millisecond,
			},
			"id2": {
				CheckCalls:           3,
				IterSubjectsCalls:    2,
				IterResourcesCalls:   1,
				CheckResults:         6,
				IterSubjectsResults:  4,
				IterResourcesResults: 2,
				CheckTime:            75 * time.Millisecond,
				IterSubjectsTime:     30 * time.Millisecond,
				IterResourcesTime:    15 * time.Millisecond,
			},
		}
		result := AggregateAnalyzeStats(stats)
		expected := AnalyzeStats{
			CheckCalls:           8,
			IterSubjectsCalls:    5,
			IterResourcesCalls:   3,
			CheckResults:         16,
			IterSubjectsResults:  10,
			IterResourcesResults: 6,
			CheckTime:            175 * time.Millisecond,
			IterSubjectsTime:     80 * time.Millisecond,
			IterResourcesTime:    40 * time.Millisecond,
		}
		require.Equal(t, expected, result)
	})
}
