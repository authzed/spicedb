package caveats

import (
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/cel-go/cel"
	"github.com/authzed/cel-go/common/types"
	"github.com/authzed/cel-go/common/types/ref"
)

// EvaluationConfig is configuration given to an EvaluateCaveatWithConfig call.
type EvaluationConfig struct {
	// MaxCost is the max cost of the caveat to be executed.
	MaxCost uint64
}

// CaveatResult holds the result of evaluating a caveat.
type CaveatResult struct {
	val             ref.Val
	details         *cel.EvalDetails
	parentCaveat    *CompiledCaveat
	contextValues   map[string]any
	missingVarNames []string
	isPartial       bool
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

	ast, err := cr.parentCaveat.celEnv.ResidualAst(cr.parentCaveat.ast, cr.details)
	if err != nil {
		return nil, err
	}

	return &CompiledCaveat{cr.parentCaveat.celEnv, ast, cr.parentCaveat.name}, nil
}

// ContextValues returns the context values used when computing this result.
func (cr CaveatResult) ContextValues() map[string]any {
	return cr.contextValues
}

// ContextStruct returns the context values used when computing this result as
// a structpb.
func (cr CaveatResult) ContextStruct() (*structpb.Struct, error) {
	return ConvertContextToStruct(cr.contextValues)
}

// ExpressionString returns the human-readable expression string for the evaluated expression.
func (cr CaveatResult) ExpressionString() (string, error) {
	return cr.parentCaveat.ExprString()
}

// ParentCaveat returns the caveat that was evaluated to produce this result.
func (cr CaveatResult) ParentCaveat() *CompiledCaveat {
	return cr.parentCaveat
}

// MissingVarNames returns the name(s) of the missing variables.
func (cr CaveatResult) MissingVarNames() ([]string, error) {
	if !cr.isPartial {
		return nil, fmt.Errorf("result is fully evaluated")
	}

	return cr.missingVarNames, nil
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

	// Mark any unspecified variables as unknown, to ensure that partial application
	// will result in producing a type of Unknown.
	activation, err := env.PartialVars(contextValues)
	if err != nil {
		return nil, err
	}

	val, details, err := prg.Eval(activation)
	if err != nil {
		return nil, EvaluationError{err}
	}

	// If the value produced has Unknown type, then it means required context was missing.
	if types.IsUnknown(val) {
		unknownVal := val.(*types.Unknown)
		missingVarNames := make([]string, 0, len(unknownVal.IDs()))
		for _, id := range unknownVal.IDs() {
			trails, ok := unknownVal.GetAttributeTrails(id)
			if ok {
				for _, attributeTrail := range trails {
					missingVarNames = append(missingVarNames, attributeTrail.String())
				}
			}
		}

		return &CaveatResult{
			val:             val,
			details:         details,
			parentCaveat:    caveat,
			contextValues:   contextValues,
			missingVarNames: missingVarNames,
			isPartial:       true,
		}, nil
	}

	return &CaveatResult{
		val:             val,
		details:         details,
		parentCaveat:    caveat,
		contextValues:   contextValues,
		missingVarNames: nil,
		isPartial:       false,
	}, nil
}
