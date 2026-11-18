package remote

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/authzed/cel-go/cel"
	"github.com/authzed/cel-go/common/types"
	"github.com/authzed/cel-go/common/types/ref"

	"github.com/authzed/spicedb/pkg/dispatch/secondary"
)

// External strategies are registered through secondary.Function, by programs that embed SpiceDB.
// They run on the same race engine as the built-in strategies, through an adapter between the
// engine's participants and the public secondary.Participant. Built-in strategies do not go
// through it, so the adapter costs nothing on their path.

// celIdentifier matches the names a CEL function can be called by.
var celIdentifier = regexp.MustCompile(`^[_a-zA-Z][_a-zA-Z0-9]*$`)

const externalDurationArgument = "duration"

// externalDescriptor declares the CEL function of a registered strategy function.
func externalDescriptor(fn secondary.Function) (strategyDescriptor, error) {
	if !celIdentifier.MatchString(fn.Name) {
		return strategyDescriptor{}, fmt.Errorf("secondary dispatch strategy function name %q is not a valid CEL identifier", fn.Name)
	}
	if fn.New == nil && fn.NewWithDuration == nil {
		return strategyDescriptor{}, fmt.Errorf("secondary dispatch strategy function %q has neither New nor NewWithDuration", fn.Name)
	}

	descriptor := strategyDescriptor{
		function: fn.Name,
		methods:  fn.Methods,
	}

	if fn.New != nil {
		descriptor.overloads = append(descriptor.overloads, cel.Overload(fn.Name+"_list_string",
			[]*cel.Type{cel.ListType(cel.StringType)}, dispatchPlanCelType,
			cel.UnaryBinding(func(list ref.Val) ref.Val {
				names, err := toStringSlice(list)
				if err != nil {
					return types.NewErr("%s: %s", fn.Name, err)
				}
				return externalPlan(fn.Name, names, fn.New)
			})))
	}

	if fn.NewWithDuration != nil {
		descriptor.durationArgument = externalDurationArgument
		descriptor.overloads = append(descriptor.overloads, cel.Overload(fn.Name+"_list_string_duration",
			[]*cel.Type{cel.ListType(cel.StringType), cel.DurationType}, dispatchPlanCelType,
			cel.BinaryBinding(func(list, duration ref.Val) ref.Val {
				names, err := toStringSlice(list)
				if err != nil {
					return types.NewErr("%s: %s", fn.Name, err)
				}
				d, ok := duration.(types.Duration)
				if !ok {
					return types.MaybeNoSuchOverloadErr(duration)
				}
				if err := checkStrategyDuration(fn.Name, externalDurationArgument, d.Duration); err != nil {
					return types.NewErr("%s", err)
				}
				return externalPlan(fn.Name, names, func() (secondary.Strategy, error) {
					return fn.NewWithDuration(d.Duration)
				})
			})))
	}

	return descriptor, nil
}

// externalPlan builds the plan for a call to a registered strategy function.
func externalPlan(function string, names []string, newStrategy func() (secondary.Strategy, error)) ref.Val {
	strategy, err := newStrategy()
	if err == nil && strategy == nil {
		err = errors.New("no strategy returned")
	}
	if err != nil {
		return types.NewErr("%s: %s", function, err)
	}
	return dispatchPlanVal{dispatchPlan{secondaries: names, strategy: externalStrategy{function: function, strategy: strategy}}}
}

// externalStrategy runs a strategy built by a registered strategy function.
type externalStrategy struct {
	function string
	strategy secondary.Strategy
}

func (s externalStrategy) name() string { return s.function }

func (s externalStrategy) newPolicy(r raceRequest) racePolicy {
	participants := make([]secondary.Participant, len(r.participants))
	for i, p := range r.participants {
		participants[i] = publicParticipant(p)
	}

	policy := s.strategy.NewPolicy(secondary.Race{
		Method:       r.reqKey,
		Resource:     r.resource,
		Subject:      r.subject,
		Request:      r.request,
		Participants: participants,
	})

	adapted := externalPolicy{policy: policy}
	if observer, ok := policy.(secondary.ResultObserver); ok {
		// Only policies that observe results are given them: the engine checks for observers
		// once per race.
		return observingExternalPolicy{externalPolicy: adapted, observer: observer}
	}
	return adapted
}

func publicParticipant(p participant) secondary.Participant {
	return secondary.Participant{Name: p.name, Primary: p.primary}
}

// externalPolicy adapts a secondary.Policy to the engine's racePolicy.
type externalPolicy struct {
	policy secondary.Policy
}

func (e externalPolicy) start(ctx context.Context, p participant) bool {
	return e.policy.Start(ctx, publicParticipant(p))
}

func (e externalPolicy) readyToClaim(ctx context.Context, p participant, received int, complete bool) bool {
	return e.policy.ReadyToClaim(ctx, publicParticipant(p), received, complete)
}

func (e externalPolicy) firstResult(p participant, latency time.Duration) {
	e.policy.FirstResult(publicParticipant(p), latency)
}

func (e externalPolicy) done(p participant, err error) {
	e.policy.Done(publicParticipant(p), err)
}

func (e externalPolicy) noWinnerError(errs map[string]error) error {
	return e.policy.NoWinnerError(errs)
}

// observingExternalPolicy adapts a secondary.Policy that also observes results.
type observingExternalPolicy struct {
	externalPolicy
	observer secondary.ResultObserver
}

func (o observingExternalPolicy) observe(p participant, result any) {
	o.observer.ObserveResult(publicParticipant(p), result)
}
