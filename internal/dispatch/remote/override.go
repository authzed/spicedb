package remote

import (
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/authzed/cel-go/cel"

	"github.com/authzed/spicedb/pkg/dispatch/secondary"
	"github.com/authzed/spicedb/pkg/middleware/dispatchstrategy"
)

// maxCachedOverrides bounds the plans StrategyOverrides keeps. Overrides come from clients, so
// the number of distinct ones is not bounded by the configuration; past the bound, plans are
// still resolved, only not kept.
const maxCachedOverrides = 1024

// StrategyOverrides resolves the strategy overrides requests carry: secondary dispatch
// expressions that replace the configured ones for a request and every dispatch it causes (see
// package dispatchstrategy).
//
// An override is a dispatch expression that cannot reference `request`, so it evaluates to the
// same plan for every dispatch of a request kind. That plan is computed once, when the override
// is validated, and kept: dispatches only look it up.
type StrategyOverrides struct {
	env         *cel.Env
	strategies  []strategyDescriptor
	secondaries []string

	mu    sync.RWMutex
	plans map[overrideKey]dispatchPlan // GUARDED_BY(mu)
}

type overrideKey struct {
	requestKind string
	expr        string
}

var _ dispatchstrategy.Validator = (*StrategyOverrides)(nil)

// NewStrategyOverrides returns the overrides of a dispatcher with the given secondaries.
// functions adds strategy functions registered by a program that embeds SpiceDB to the built-in
// ones, as for ParseDispatchExpression.
func NewStrategyOverrides(secondaries []string, functions ...secondary.Function) (*StrategyOverrides, error) {
	if len(secondaries) == 0 {
		return nil, errors.New("strategy overrides require at least one secondary dispatcher")
	}

	env, strategies, err := newDispatchEnv(functions, false)
	if err != nil {
		return nil, err
	}

	return &StrategyOverrides{
		env:         env,
		strategies:  strategies,
		secondaries: slices.Clone(secondaries),
		plans:       make(map[overrideKey]dispatchPlan),
	}, nil
}

// Validate implements dispatchstrategy.Validator: expr must parse and evaluate as the dispatch
// expression of every one of the request kinds, and name only configured secondaries.
func (o *StrategyOverrides) Validate(expr string, requestKinds ...string) error {
	for _, kind := range requestKinds {
		if _, err := o.plan(kind, expr); err != nil {
			return err
		}
	}
	return nil
}

// plan returns the plan of the override expr for dispatches of a request kind.
func (o *StrategyOverrides) plan(requestKind, expr string) (dispatchPlan, error) {
	key := overrideKey{requestKind: requestKind, expr: expr}

	o.mu.RLock()
	plan, ok := o.plans[key]
	o.mu.RUnlock()
	if ok {
		return plan, nil
	}

	plan, err := o.resolve(requestKind, expr)
	if err != nil {
		return dispatchPlan{}, err
	}

	o.mu.Lock()
	if len(o.plans) < maxCachedOverrides {
		o.plans[key] = plan
	}
	o.mu.Unlock()
	return plan, nil
}

func (o *StrategyOverrides) resolve(requestKind, expr string) (dispatchPlan, error) {
	if !slices.Contains(supportsSecondaries, requestKind) {
		return dispatchPlan{}, fmt.Errorf("request kind `%s` does not support secondary dispatchers", requestKind)
	}

	prg, err := compileDispatchExpression(o.env, o.strategies, requestKind, expr)
	if err != nil {
		return dispatchPlan{}, err
	}

	plan, err := evalDispatchProgram(prg, cel.NoVars())
	if err != nil {
		return dispatchPlan{}, err
	}

	for _, name := range plan.secondaries {
		if !slices.Contains(o.secondaries, name) {
			return dispatchPlan{}, fmt.Errorf("unknown secondary dispatcher `%s`", name)
		}
	}

	plan.override = true
	return plan, nil
}
