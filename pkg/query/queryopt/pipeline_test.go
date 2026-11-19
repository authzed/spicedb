package queryopt_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/queryopttest"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/query/queryopt/optimization"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func nop(name string) optimization.Optimizer {
	return optimization.Optimizer{Name: name, Run: func(_ *optimization.Context, co query.CanonicalOutline) (optimization.Result, error) {
		return optimization.Result{Outline: co}, nil
	}}
}

func TestPipelineDependencies(t *testing.T) {
	a, b := nop("a"), nop("b")
	a.RequiredHints = []planhint.Kind{planhint.BranchOrderKind}
	_, err := queryopt.NewPipeline([]optimization.Optimizer{a})
	require.Error(t, err)
	a.RequiredHints = nil
	a.After = []string{"b"}
	b.After = []string{"a"}
	_, err = queryopt.NewPipeline([]optimization.Optimizer{a, b})
	require.ErrorContains(t, err, "cycle")
}

func TestOptionalProducerPrecedesConsumer(t *testing.T) {
	order := []string{}
	a, b := nop("producer"), nop("consumer")
	a.ProducesHints = []planhint.Kind{planhint.BranchOrderKind}
	b.OptionalHints = []planhint.Kind{planhint.BranchOrderKind}
	b.Priority = 100

	for _, o := range []*optimization.Optimizer{&a, &b} {
		name := o.Name
		o.Run = func(_ *optimization.Context, co query.CanonicalOutline) (optimization.Result, error) {
			order = append(order, name)
			return optimization.Result{Outline: co}, nil
		}
	}

	p, err := queryopt.NewPipeline([]optimization.Optimizer{b, a})
	require.NoError(t, err)
	co := queryopttest.Canonical(t, queryopttest.Empty())
	_, err = p.Run(co, optimization.NewContext(optimization.RequestParams{}, nil, nil))
	require.NoError(t, err)
	require.Equal(t, []string{"producer", "consumer"}, order)
}

func TestUndeclaredHintEmissionFails(t *testing.T) {
	a := nop("a")
	a.Run = func(_ *optimization.Context, co query.CanonicalOutline) (optimization.Result, error) {
		return optimization.Result{
			Outline: co,
			Hints:   []planhint.Hint{{Target: planhint.Target{Node: uint64(co.Root.ID)}, Payload: planhint.ArrowDirection{}}},
		}, nil
	}
	p, err := queryopt.NewPipeline([]optimization.Optimizer{a})
	require.NoError(t, err)
	co := queryopttest.Canonical(t, queryopttest.Empty())
	_, err = p.Run(co, optimization.NewContext(optimization.RequestParams{}, nil, nil))
	require.ErrorContains(t, err, "undeclared")
}

func TestUnknownDeclarationsRejectedAtConstruction(t *testing.T) {
	for _, opt := range []optimization.Optimizer{
		{Name: "unknown-signal", RequiredSignals: []signal.Name{"does-not-exist"}},
		{Name: "unknown-optional", OptionalSignals: []signal.Name{"does-not-exist"}},
		{Name: "unknown-kind", ProducesHints: []planhint.Kind{"does-not-exist"}},
	} {
		opt.Run = func(_ *optimization.Context, co query.CanonicalOutline) (optimization.Result, error) {
			return optimization.Result{Outline: co}, nil
		}
		_, err := queryopt.NewPipeline([]optimization.Optimizer{opt})
		require.Error(t, err)
	}
}
