package remote

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/authzed/cel-go/cel"
	"github.com/authzed/cel-go/common/types"
	"github.com/authzed/cel-go/common/types/ref"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/dispatch/secondary"
)

// External strategies are registered through secondary.Function, by programs that embed SpiceDB.
// They run on the same race engine as the built-in strategies, through an adapter between the
// engine's participants and the public secondary.Participant. Built-in strategies do not go
// through it, so the adapter costs nothing on their path.

// externalDescriptor declares the CEL function of a registered strategy function: name([...]).
func externalDescriptor(fn secondary.Function) strategyDescriptor {
	strategy := externalStrategy{function: fn.Name, strategy: fn.Strategy}
	return strategyDescriptor{
		function: fn.Name,
		methods:  fn.Methods,
		overloads: []cel.FunctionOpt{
			// CEL needs a unique ID per overload; <function>_<argument types> is the convention
			// the built-in strategies follow (hedged_list_string).
			cel.Overload(fn.Name+"_list_string",
				[]*cel.Type{cel.ListType(cel.StringType)}, dispatchPlanCelType,
				cel.UnaryBinding(func(list ref.Val) ref.Val {
					names, err := toStringSlice(list)
					if err != nil {
						return types.NewErr("%s: %s", fn.Name, err)
					}
					return dispatchPlanVal{dispatchPlan{secondaries: names, strategy: strategy}}
				})),
		},
	}
}

// externalStrategy runs the races of a registered strategy function.
type externalStrategy struct {
	function string
	strategy secondary.Strategy
}

func (s externalStrategy) name() string { return s.function }

// newPolicy creates the external policy for a race. If the strategy panics, the race runs the
// primary only.
func (s externalStrategy) newPolicy(r raceRequest) (policy racePolicy) {
	defer func() {
		if recovered := recover(); recovered != nil {
			reportExternalPanic(s.function, "NewPolicy", recovered)
			policy = primaryOnlyPolicy{}
		}
	}()

	participants := make([]secondary.Participant, len(r.participants))
	for i, p := range r.participants {
		participants[i] = publicParticipant(p)
	}

	adapted := externalPolicy{function: s.function, policy: s.strategy.NewPolicy(secondary.Race{
		Method:       r.reqKey,
		Request:      r.request,
		Participants: participants,
	})}
	if observer, ok := adapted.policy.(secondary.ResultObserver); ok {
		// Only policies that observe results are given them: the engine checks for observers
		// once per race, so the others pay nothing per result.
		return observingExternalPolicy{externalPolicy: adapted, observer: observer}
	}
	return adapted
}

func publicParticipant(p participant) secondary.Participant {
	return secondary.Participant{Name: p.name, Primary: p.primary}
}

// externalPolicy adapts a secondary.Policy to the engine's racePolicy.
//
// The policy is code of the program that embeds SpiceDB, called from participant goroutines that
// nothing else recovers. So that a bug in it fails a dispatch instead of the process, every call
// recovers from a panic: it is logged and counted, and the call is taken as the safest answer
// (the participant does not start, does not claim the race, or the engine's default error is
// returned).
type externalPolicy struct {
	function string
	policy   secondary.Policy
}

func (e externalPolicy) start(ctx context.Context, p participant) bool {
	defer e.recoverCallback("Start")
	return e.policy.Start(ctx, publicParticipant(p))
}

func (e externalPolicy) readyToClaim(ctx context.Context, p participant, received int, complete bool) bool {
	defer e.recoverCallback("ReadyToClaim")
	return e.policy.ReadyToClaim(ctx, publicParticipant(p), received, complete)
}

func (externalPolicy) firstResult(participant, time.Duration) {}

func (e externalPolicy) done(p participant, err error) {
	defer e.recoverCallback("Done")
	e.policy.Done(publicParticipant(p), err)
}

func (e externalPolicy) noWinnerError(errs map[string]error) error {
	defer e.recoverCallback("NoWinnerError")
	return e.policy.NoWinnerError(errs)
}

// observingExternalPolicy adapts a secondary.Policy that also implements secondary.ResultObserver.
type observingExternalPolicy struct {
	externalPolicy
	observer secondary.ResultObserver
}

func (o observingExternalPolicy) observe(p participant, result any) {
	defer o.recoverCallback("ObserveResult")
	o.observer.ObserveResult(publicParticipant(p), result)
}

// recoverCallback recovers from a panic in a policy callback, which then returns its results'
// zero values. It must be deferred directly.
func (e externalPolicy) recoverCallback(callback string) {
	if recovered := recover(); recovered != nil {
		reportExternalPanic(e.function, callback, recovered)
	}
}

func reportExternalPanic(function, callback string, recovered any) {
	externalStrategyPanicsCounter.WithLabelValues(function).Inc()
	log.Error().
		Str("strategy", function).
		Str("callback", callback).
		Interface("panic", recovered).
		Bytes("stack", debug.Stack()).
		Msg("secondary dispatch strategy panicked; failing the participant instead of the process")
}

// primaryOnlyPolicy runs the primary only: it replaces a policy the strategy failed to create.
type primaryOnlyPolicy struct {
	basePolicy
}

func (primaryOnlyPolicy) start(_ context.Context, p participant) bool { return p.primary }
