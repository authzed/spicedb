package remote

import (
	"context"
	"time"

	"github.com/authzed/cel-go/cel"
	"github.com/authzed/cel-go/common/types"
	"github.com/authzed/cel-go/common/types/ref"
)

// hedgedStrategy starts the secondaries immediately and delays the primary by an adaptive
// amount: the configured quantile of the secondaries' first-result latency, capped at a
// maximum delay. Any secondary error releases the primary early. This is the default strategy,
// used when a dispatch expression returns a plain list.
//
// In CEL: hedged(['secondary']) or hedged(['secondary'], duration('5ms')).
type hedgedStrategy struct {
	// maxDelay caps the primary's delay for this query. Zero means it was not set: the cap is
	// then the largest MaximumPrimaryHedgingDelay of the participating secondaries.
	maxDelay time.Duration
}

func (hedgedStrategy) name() string { return "hedged" }

func (s hedgedStrategy) newPolicy(r raceRequest) racePolicy {
	maxDelay := s.maxDelay
	if maxDelay == 0 {
		for _, p := range r.participants {
			maxDelay = max(maxDelay, p.maxPrimaryDelay)
		}
	}

	waitTime := r.stats.hedgingDelay(r.reqKey, r.resource, r.subject, maxDelay)
	return &hedgedPolicy{sleeper: primarySleeper{
		reqKey:     r.reqKey,
		waitTime:   waitTime,
		cancelFunc: func() {}, // No-op until sleep is called.
	}}
}

type hedgedPolicy struct {
	basePolicy
	sleeper primarySleeper
}

func (h *hedgedPolicy) start(ctx context.Context, p participant) bool {
	if p.primary {
		h.sleeper.sleep(ctx)
	}
	return true
}

func (h *hedgedPolicy) done(p participant, err error) {
	if !p.primary && err != nil {
		// Let the primary start immediately if it is still waiting.
		h.sleeper.cancelSleep()
	}
}

var hedgedDescriptor = strategyDescriptor{
	function: "hedged",
	overloads: []cel.FunctionOpt{
		cel.Overload("hedged_list_string",
			[]*cel.Type{cel.ListType(cel.StringType)}, dispatchPlanCelType,
			cel.UnaryBinding(func(list ref.Val) ref.Val {
				names, err := toStringSlice(list)
				if err != nil {
					return types.NewErr("hedged: %s", err)
				}
				return dispatchPlanVal{dispatchPlan{secondaries: names, strategy: hedgedStrategy{}}}
			})),
		cel.Overload("hedged_list_string_duration",
			[]*cel.Type{cel.ListType(cel.StringType), cel.DurationType}, dispatchPlanCelType,
			cel.BinaryBinding(func(list, maxDelay ref.Val) ref.Val {
				names, err := toStringSlice(list)
				if err != nil {
					return types.NewErr("hedged: %s", err)
				}
				d, ok := maxDelay.(types.Duration)
				if !ok {
					return types.MaybeNoSuchOverloadErr(maxDelay)
				}
				if d.Duration <= 0 {
					return types.NewErr("hedged: maximum delay must be positive, got %s", d.Duration)
				}
				return dispatchPlanVal{dispatchPlan{secondaries: names, strategy: hedgedStrategy{maxDelay: d.Duration}}}
			})),
	},
}
