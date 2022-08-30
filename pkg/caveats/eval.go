package caveats

import (
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/interpreter"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// EvaluationConfig is configuration given to an EvaluateCaveatWithConfig call.
type EvaluationConfig struct {
	// MaxCost is the max cost of the caveat to be executed.
	MaxCost uint64
}

// CaveatResult holds the result of evaluating a caveat.
type CaveatResult struct {
	val          ref.Val
	details      *cel.EvalDetails
	parentCaveat *CompiledCaveat
	isPartial    bool
}

// Value returns the computed value for the result.
func (cr CaveatResult) Value() bool {
	if cr.isPartial {
		return false
	}

	return cr.val.Value().(bool)
}

// IsPartial returns true if the caveat was only partially evaluated.
func (cr CaveatResult) IsPartial() bool {
	return cr.isPartial
}

// PartialValue returns the partially evaluated caveat. Only applies if IsPartial is true.
func (cr CaveatResult) PartialValue() (*CompiledCaveat, error) {
	if !cr.isPartial {
		return nil, fmt.Errorf("result is fully evaluated")
	}

	expr := interpreter.PruneAst(cr.parentCaveat.ast.Expr(), cr.details.State())
	return &CompiledCaveat{cr.parentCaveat.celEnv, cel.ParsedExprToAst(&exprpb.ParsedExpr{Expr: expr})}, nil
}

// EvaluateCaveat evaluates the compiled caveat with the specified values, and returns
// the result or an error.
func EvaluateCaveat(caveat *CompiledCaveat, contextValues map[string]any) (*CaveatResult, error) {
	return EvaluateCaveatWithConfig(caveat, contextValues, nil)
}

// EvaluateCaveatWithConfig evaluates the compiled caveat with the specified values, and returns
// the result or an error.
func EvaluateCaveatWithConfig(caveat *CompiledCaveat, contextValues map[string]any, config *EvaluationConfig) (*CaveatResult, error) {
	env := caveat.celEnv
	celopts := make([]cel.ProgramOption, 0, 3)

	// TODO(jschorr): Turn off if we know we have all the context values necessary?
	// Option: enables partial evaluation and state tracking for partial evaluation.
	celopts = append(celopts, cel.EvalOptions(cel.OptTrackState))
	celopts = append(celopts, cel.EvalOptions(cel.OptPartialEval))

	// Option: Cost limit on the evaluation.
	if config != nil && config.MaxCost > 0 {
		celopts = append(celopts, cel.CostLimit(config.MaxCost))
	}

	prg, err := env.Program(caveat.ast, celopts...)
	if err != nil {
		return nil, err
	}

	pvars, err := cel.PartialVars(contextValues)
	if err != nil {
		return nil, err
	}

	val, details, err := prg.Eval(pvars)
	if err != nil {
		// From program.go:
		// *  `val`, `details`, `nil` - Successful evaluation of a non-error result.
		// *  `val`, `details`, `err` - Successful evaluation to an error result.
		// *  `nil`, `details`, `err` - Unsuccessful evaluation.
		// TODO(jschorr): See if there is a better way to detect partial eval.
		if val != nil && strings.Contains(err.Error(), "no such attribute") {
			return &CaveatResult{val, details, caveat, true}, nil
		}

		return nil, err
	}

	return &CaveatResult{val, details, caveat, false}, nil
}
