// Package arrowdirection recommends execution direction using observed fanout.
package arrowdirection

import (
	"errors"
	"math"

	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/query/queryopt/optimization"
	"github.com/authzed/spicedb/pkg/query/signals/historicalresultsize"
	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

// New returns the pass that recommends arrow direction from observed result sizes.
func New() optimization.Optimizer {
	return optimization.Optimizer{
		Name:            "observed-arrow-direction",
		OptionalSignals: []signal.Name{resultsize.Name, historicalresultsize.Name},
		ProducesHints:   []planhint.Kind{planhint.ArrowDirectionKind},
		Run:             run,
	}
}

func run(ctx *optimization.Context, co query.CanonicalOutline) (optimization.Result, error) {
	// Use the integration's key builder so estimates describe the same filtered
	// population, batching strategy, and dispatch role as execution observations.
	// The signal view supplies schema/revision scope.
	key := ctx.Key
	if key == nil {
		return optimization.Result{}, errors.New("arrow-direction optimization requires an execution key builder")
	}

	// Debug evidence is optional, but the recommendation itself is always emitted.
	// Consult the active ledger so a reused traced outline keeps its collection policy.
	collectEvidence := ctx.HintEvidenceEnabled()
	hints := []planhint.Hint{}
	err := query.WalkOutlinePreOrder(co.Root, func(n query.Outline) error {
		// A normal arrow can start from either endpoint while preserving membership.
		// Intersection arrows have different all() semantics and are outside this rule.
		alternatives, ok := n.ArrowCheckAlternatives()
		if !ok {
			return nil
		}

		// Estimate the fanout of each possible starting side. With no evidence, use
		// the existing static advisor's estimate of three so missing data still produces
		// a deterministic recommendation. These counts estimate work, not permission truth.
		direction := planhint.LeftToRight
		bestCost := math.Inf(1)
		var evidence []planhint.EvidenceReference

		for _, alternative := range alternatives {
			// The query layer describes which subtree and enumeration operation start
			// this strategy. Its Check population measures different work and is not used.
			k := key(alternative.Enumeration, alternative.Operation)
			cost := float64(3)

			// Exact-revision evidence is preferred. Historical evidence is an aged fallback
			// when this revision has no usable estimate; lookup errors remain planning errors.
			e, name, err := signal.LookupFirst(ctx.Signals, k, resultsize.Ref, historicalresultsize.Ref)
			if err != nil {
				return err
			}

			if e.IsAvailable() {
				cost = e.Value.Mean

				// Only tracing needs the supporting provider/snapshot references. Avoid
				// allocating them on ordinary requests while still using the estimate's mean.
				if collectEvidence {
					evidence = append(evidence, planhint.EvidenceReference{
						Signal:     name,
						ID:         e.Evidence.ID,
						Historical: e.Evidence.Historical,
					})
				}
			}

			// Alternatives are ordered forward first. A strict comparison retains
			// forward execution on ties; poor estimates affect work, not membership.
			if cost < bestCost {
				direction = alternative.Direction
				bestCost = cost
			}
		}

		// Emit a recommendation instead of rewriting the tree. The pipeline fills
		// producer/fingerprint metadata, and compilation consumes the direction hint.
		// Occurrence IDs keep identical arrows in separate branches independently targetable.
		hints = append(hints, planhint.Hint{
			Target:   planhint.Target{Node: uint64(n.ID)},
			Payload:  planhint.ArrowDirection{Direction: direction},
			Evidence: evidence,
		})
		return nil
	})
	return optimization.Result{Outline: co, Hints: hints}, err
}
