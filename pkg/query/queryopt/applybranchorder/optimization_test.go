package applybranchorder_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/queryopttest"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/query/queryopt/applybranchorder"
	"github.com/authzed/spicedb/pkg/query/queryopt/optimization"
)

func TestBranchOrderIsConsumed(t *testing.T) {
	// Each branch grants a different subject; the hint changes traversal order.
	co := queryopttest.Canonical(t, queryopttest.Union(
		queryopttest.Fixed(t, "doc:d#read@user:a"),
		queryopttest.Fixed(t, "doc:d#read@user:b"),
	))
	first := co.Root.SubOutlines[0].ID
	producer := optimization.Optimizer{
		Name:          "test-producer",
		ProducesHints: []planhint.Kind{planhint.BranchOrderKind},
		Run: func(_ *optimization.Context, c query.CanonicalOutline) (optimization.Result, error) {
			return optimization.Result{
				Outline: c,
				Hints: []planhint.Hint{
					{Target: planhint.Target{Node: uint64(c.Root.ID)}, Payload: planhint.BranchOrder{Order: []int{1, 0}}},
				},
			}, nil
		},
	}
	p, err := queryopt.NewPipeline([]optimization.Optimizer{applybranchorder.New(), producer})
	require.NoError(t, err)
	out, err := p.Run(co, optimization.NewContext(optimization.RequestParams{}, nil, nil))
	require.NoError(t, err)
	require.Equal(t, first, out.Root.SubOutlines[1].ID)
	require.Equal(t, planhint.Accepted, out.HintLedger.Decisions()[0].Outcome)
}
