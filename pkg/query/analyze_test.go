package query

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
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

	union := NewUnion(fixed1, fixed2)

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
	analyze := make(map[string]AnalyzeStats)
	ctx := &Context{
		Context:  context.Background(),
		Executor: LocalExecutor{},
		Reader:   ds.SnapshotReader(datastore.NoRevision),
		Analyze:  analyze,
	}

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
	stats, exists := analyze[fixed.ID()]
	require.True(t, exists, "Stats should exist for iterator")
	require.Equal(t, 1, stats.CheckCalls, "Should have 1 Check call")
	require.Equal(t, 1, stats.CheckResults, "Should have 1 Check result")

	// Format and verify analysis output
	output := FormatAnalysis(fixed, analyze)
	require.Contains(t, output, "Fixed(2 paths)")
	require.Contains(t, output, "Calls: Check=1")
	require.Contains(t, output, "Results: Check=1")
}
