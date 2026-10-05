package queryopt_test

import (
	"reflect"
	"testing"

	"github.com/authzed/spicedb/internal/queryopttest"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/query/queryopt/applybranchorder"
	"github.com/authzed/spicedb/pkg/query/queryopt/arrowdirection"
	"github.com/authzed/spicedb/pkg/query/queryopt/optimization"
	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

type wrongPredictions struct{}

func (wrongPredictions) Read(_ signal.Name, key any, _, _ reflect.Type) (any, signal.Availability, signal.Evidence, error) {
	mean := 1.0

	if key.(signal.ExecutionKey).Operation == signal.IterSubjects {
		mean = 100
	}

	return resultsize.Estimate{Mean: mean, Samples: 10, Weight: 10}, signal.Available, signal.Evidence{}, nil
}

func TestComposedDecisionSemantics(t *testing.T) {
	queryopttest.Check(
		t,
		queryopttest.Config{
			Optimizers: []optimization.Optimizer{arrowdirection.New(), applybranchorder.New()},
			Signals:    func(queryopttest.Request) signal.ReadView { return wrongPredictions{} },
			Hints: func(co query.CanonicalOutline, _ queryopttest.Request) []planhint.Hint {
				hints := []planhint.Hint{}
				_ = query.WalkOutlinePreOrder(co.Root, func(n query.Outline) error {
					if (n.Type == query.UnionIteratorType || n.Type == query.IntersectionIteratorType) && len(n.SubOutlines) > 1 {
						order := make([]int, len(n.SubOutlines))

						for i := range order {
							order[i] = len(order) - 1 - i
						}

						hints = append(
							hints,
							planhint.Hint{
								Producer: "property",
								Target:   planhint.Target{Node: uint64(n.ID)},
								Payload:  planhint.BranchOrder{Order: order},
							},
						)
					}

					return nil
				})
				return hints
			},
		},
	)
}
