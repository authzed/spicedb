package caveats

import (
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/interpreter"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

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
	env := caveat.celEnv
	opts := []cel.ProgramOption{}

	// Option: enables state tracking for partial evaluation.
	// TODO(jschorr): Turn off if we know we have all the context values necessary?
	opts = append(opts, cel.EvalOptions(cel.OptTrackState))

	prg, err := env.Program(caveat.ast, opts...)
	if err != nil {
		return nil, err
	}

	val, details, err := prg.Eval(contextValues)
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
