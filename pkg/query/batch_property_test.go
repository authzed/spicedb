package query_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/queryopttest"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt/optimization"
	"github.com/authzed/spicedb/pkg/query/queryopt/parallelcheck"
)

func TestSharedBatchExecutionSemanticEquivalence(t *testing.T) {
	// Keep the outline fixed: compare the shared batch executor with scalar
	// execution over the common generated schema/relationship corpus. Arrow and
	// userset fanout supply multi-checks inside otherwise scalar public requests.
	queryopttest.Check(t, queryopttest.Config{
		Optimizer: optimization.Optimizer{
			Name: "batch-check-execution",
			Run: func(_ *optimization.Context, outline query.CanonicalOutline) (optimization.Result, error) {
				return optimization.Result{Outline: outline}, nil
			},
		},
		ContextOptions: []query.ContextOption{
			query.WithBatchedArrows(true),
			query.WithCheckExecution(query.CheckExecutionOptions{BatchChecks: true, BatchSize: 2}),
		},
	})
}

func TestSharedParallelBatchExecutionSemanticEquivalence(t *testing.T) {
	optimizer, err := parallelcheck.New(3)
	require.NoError(t, err)
	// Use the shared generated corpus to combine parallel named branches with
	// batched arrows, recursive checks, intersections and conditional paths.
	queryopttest.Check(t, queryopttest.Config{
		Optimizer: optimizer,
		ContextOptions: []query.ContextOption{
			query.WithCheckConcurrency(3),
			query.WithBatchedArrows(true),
			query.WithCheckExecution(query.CheckExecutionOptions{BatchChecks: true, BatchSize: 2}),
		},
	})
}
