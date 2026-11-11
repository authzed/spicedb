package remote

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/authzed/cel-go/cel"
	"github.com/authzed/cel-go/common/types"
	"github.com/authzed/cel-go/common/types/ref"
)

// preferSecondaryStrategy starts the secondaries immediately, and starts the primary only once
// every secondary has failed, or once the deadline has passed. When the deadline fires, the
// secondaries keep running and race the primary.
//
// In CEL: prefer_secondary(['secondary'], duration('50ms')).
type preferSecondaryStrategy struct {
	deadline time.Duration
}

func (preferSecondaryStrategy) name() string { return preferSecondaryFunction }

func (s preferSecondaryStrategy) newPolicy(r raceRequest) racePolicy {
	var secondaries int64
	for _, p := range r.participants {
		if !p.primary {
			secondaries++
		}
	}

	return &preferSecondaryPolicy{
		reqKey:      r.reqKey,
		deadline:    s.deadline,
		secondaries: secondaries,
		released:    make(chan struct{}),
	}
}

type preferSecondaryPolicy struct {
	basePolicy
	reqKey      string
	deadline    time.Duration
	secondaries int64
	failed      atomic.Int64
	released    chan struct{} // closed once every secondary has failed
}

func (p *preferSecondaryPolicy) start(ctx context.Context, pt participant) bool {
	if !pt.primary {
		return true
	}

	timer := time.NewTimer(p.deadline)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-p.released:
		metricsFor(p.reqKey).releasedOnAllFailures.Inc()
	case <-timer.C:
		metricsFor(p.reqKey).releasedAtDeadline.Inc()
	}
	return true
}

// done counts only failures: a successful secondary ends the race anyway, because the winner
// cancels the other participants.
func (p *preferSecondaryPolicy) done(pt participant, err error) {
	if !pt.primary && err != nil && p.failed.Add(1) == p.secondaries {
		close(p.released)
	}
}

const (
	preferSecondaryFunction         = "prefer_secondary"
	preferSecondaryDurationArgument = "deadline"
)

var preferSecondaryDescriptor = strategyDescriptor{
	function:         preferSecondaryFunction,
	durationArgument: preferSecondaryDurationArgument,
	overloads: []cel.FunctionOpt{
		cel.Overload("prefer_secondary_list_string_duration",
			[]*cel.Type{cel.ListType(cel.StringType), cel.DurationType}, dispatchPlanCelType,
			cel.BinaryBinding(func(list, deadline ref.Val) ref.Val {
				names, err := toStringSlice(list)
				if err != nil {
					return types.NewErr("%s: %s", preferSecondaryFunction, err)
				}
				d, ok := deadline.(types.Duration)
				if !ok {
					return types.MaybeNoSuchOverloadErr(deadline)
				}
				if err := checkStrategyDuration(preferSecondaryFunction, preferSecondaryDurationArgument, d.Duration); err != nil {
					return types.NewErr("%s", err)
				}
				return dispatchPlanVal{dispatchPlan{secondaries: names, strategy: preferSecondaryStrategy{deadline: d.Duration}}}
			})),
	},
}
