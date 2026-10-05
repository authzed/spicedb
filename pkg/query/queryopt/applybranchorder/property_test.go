package applybranchorder_test

import (
	"testing"

	"github.com/authzed/spicedb/internal/queryopttest"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/query/queryopt/applybranchorder"
)

func TestSemanticEquivalence(t *testing.T) {
	queryopttest.Check(t, queryopttest.Config{
		Optimizer: applybranchorder.New(),
		Hints: func(co query.CanonicalOutline, _ queryopttest.Request) []planhint.Hint {
			hints := []planhint.Hint{}
			_ = query.WalkOutlinePreOrder(co.Root, func(n query.Outline) error {
				if (n.Type == query.UnionIteratorType || n.Type == query.IntersectionIteratorType) && len(n.SubOutlines) > 1 {
					order := make([]int, len(n.SubOutlines))

					for i := range order {
						order[i] = len(order) - 1 - i
					}

					hints = append(hints, planhint.Hint{
						Producer: "test-recommendation",
						Target:   planhint.Target{Node: uint64(n.ID)},
						Payload:  planhint.BranchOrder{Order: order},
					})
				}

				return nil
			})
			return hints
		},
	})
}
