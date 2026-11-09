package parallelcheck_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/queryopttest"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/query/queryopt/optimization"
	"github.com/authzed/spicedb/pkg/query/queryopt/parallelcheck"
)

func TestFixedConcurrencyRecommendations(t *testing.T) {
	pass, err := parallelcheck.New(4)
	require.NoError(t, err)
	require.Empty(t, pass.RequiredSignals)
	require.Empty(t, pass.OptionalSignals)
	pipeline, err := queryopt.NewPipeline([]optimization.Optimizer{pass})
	require.NoError(t, err)
	co := queryopttest.Canonical(t, queryopttest.Union(
		queryopttest.Intersection(queryopttest.Fixed(t, "document:d#reader@user:u"), queryopttest.Fixed(t, "document:d#writer@user:u")),
		queryopttest.Arrow(queryopttest.Empty(), queryopttest.Empty()),
		queryopttest.Exclusion(queryopttest.Fixed(t, "document:d#reader@user:u"), queryopttest.Fixed(t, "document:d#banned@user:u")),
	))
	out, err := pipeline.Run(co, optimization.NewContext(optimization.RequestParams{Operation: query.OperationCheck}, nil, nil))
	require.NoError(t, err)
	require.Len(t, out.HintLedger.Hints(), 3)

	for _, hint := range out.HintLedger.Hints() {
		require.Equal(t, planhint.CheckConcurrency{Limit: 4}, hint.Payload)
		require.Nil(t, hint.Evidence)
	}

	_, err = out.Compile()
	require.NoError(t, err)
	_, err = parallelcheck.New(0)
	require.Error(t, err)
}

func TestConcurrencyKeepsLogicalKeys(t *testing.T) {
	co := queryopttest.Canonical(
		t,
		queryopttest.Union(queryopttest.Fixed(t, "document:d#reader@user:u"), queryopttest.Fixed(t, "document:d#writer@user:u")),
	)
	original, err := co.Compile()
	require.NoError(t, err)
	pass, err := parallelcheck.New(2)
	require.NoError(t, err)
	pipeline, err := queryopt.NewPipeline([]optimization.Optimizer{pass})
	require.NoError(t, err)
	out, err := pipeline.Run(co, optimization.NewContext(optimization.RequestParams{Operation: query.OperationCheck}, nil, nil))
	require.NoError(t, err)
	compiled, err := out.Compile()
	require.NoError(t, err)
	require.Equal(t, original.CanonicalKey(), compiled.CanonicalKey())
	require.Equal(t, original.CanonicalKey(), compiled.Clone().CanonicalKey())
	require.Equal(t, query.UnionIteratorType, out.Root.Type)
}

func TestMustNewRequiresPositiveLimit(t *testing.T) {
	require.Equal(t, "fixed-check-concurrency", parallelcheck.MustNew(2).Name)
	require.Panics(t, func() { parallelcheck.MustNew(0) })
}
