// Package parallelcheck recommends fixed, bounded concurrency for set Checks.
package parallelcheck

import (
	"errors"

	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/query/queryopt/optimization"
)

// MustNew returns the recommendation producer, panicking for an invalid limit.
// Use it for constants or limits already validated when resolving a profile.
func MustNew(limit uint16) optimization.Optimizer {
	pass, err := New(limit)
	if err != nil {
		panic(err)
	}

	return pass
}

// New returns a signal-free recommendation producer using the configured limit.
func New(limit uint16) (optimization.Optimizer, error) {
	if limit == 0 {
		return optimization.Optimizer{}, errors.New("check concurrency must be positive")
	}

	return optimization.Optimizer{
		Name:          "fixed-check-concurrency",
		Description:   "Recommend fixed per-reducer union/intersection/exclusion Check concurrency",
		ProducesHints: []planhint.Kind{planhint.CheckConcurrencyKind},
		Run: func(ctx *optimization.Context, co query.CanonicalOutline) (optimization.Result, error) {
			// Enumeration has different streaming and frontier ownership. Recommend this
			// policy only for Check requests, where each child produces one membership path.
			if ctx.Params.Operation != query.OperationCheck {
				return optimization.Result{Outline: co}, nil
			}

			var hints []planhint.Hint
			err := query.WalkOutlinePreOrder(co.Root, func(node query.Outline) error {
				// Set reducers with siblings can overlap independent operand checks. The runtime
				// checks observer/frontier ownership before honoring the recommendation.
				switch node.Type {
				case query.UnionIteratorType, query.IntersectionIteratorType, query.ExclusionIteratorType:
				default:
					return nil
				}

				if len(node.SubOutlines) < 2 {
					return nil
				}

				hints = append(hints, planhint.Hint{
					Target:  planhint.Target{Node: uint64(node.ID)},
					Payload: planhint.CheckConcurrency{Limit: limit},
				})
				return nil
			})
			return optimization.Result{Outline: co, Hints: hints}, err
		},
	}, nil
}
