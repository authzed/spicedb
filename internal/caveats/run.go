package caveats

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// RunCaveatExpressionDebugOption are the options for running caveat expression evaluation
// with debugging enabled or disabled.
type RunCaveatExpressionDebugOption int

const (
	// RunCaveatExpressionNoDebugging runs the evaluation without debugging enabled.
	RunCaveatExpressionNoDebugging RunCaveatExpressionDebugOption = 0

	// RunCaveatExpressionWithDebugInformation runs the evaluation with debugging enabled.
	RunCaveatExpressionWithDebugInformation RunCaveatExpressionDebugOption = 1
)

// RunCaveatExpression runs a caveat expression over the given context and returns the result.
func RunCaveatExpression(
	ctx context.Context,
	expr *core.CaveatExpression,
	context map[string]any,
	reader datastore.CaveatReader,
	debugOption RunCaveatExpressionDebugOption,
) (ExpressionResult, error) {
	env := caveats.NewEnvironment()
	return runExpression(ctx, env, expr, context, reader, debugOption)
}

// ExpressionResult is the result of a caveat expression being run.
type ExpressionResult interface {
	// Value is the resolved value for the expression. For partially applied expressions, this value will be false.
	Value() bool

	// IsPartial returns whether the expression was only partially applied.
	IsPartial() bool

	// MissingVarNames returns the names of the parameters missing from the context.
	MissingVarNames() ([]string, error)
}

type syntheticResult struct {
	value           bool
	isPartialResult bool

	op                  core.CaveatOperation_Operation
	exprResultsForDebug []ExpressionResult
	missingVarNames     *mapz.Set[string]
}

func (sr syntheticResult) Value() bool {
	return sr.value
}

func (sr syntheticResult) IsPartial() bool {
	return sr.isPartialResult
}

func (sr syntheticResult) MissingVarNames() ([]string, error) {
	if sr.isPartialResult {
		if sr.missingVarNames != nil {
			return sr.missingVarNames.AsSlice(), nil
		}

		missingVarNames := mapz.NewSet[string]()
		for _, exprResult := range sr.exprResultsForDebug {
			if exprResult.IsPartial() {
				found, err := exprResult.MissingVarNames()
				if err != nil {
					return nil, err
				}

				missingVarNames.Extend(found)
			}
		}

		return missingVarNames.AsSlice(), nil
	}

	return nil, fmt.Errorf("not a partial value")
}

func isFalseResult(result ExpressionResult) bool {
	return !result.Value() && !result.IsPartial()
}

func isTrueResult(result ExpressionResult) bool {
	return result.Value() && !result.IsPartial()
}

func runExpression(
	ctx context.Context,
	env *caveats.Environment,
	expr *core.CaveatExpression,
	context map[string]any,
	reader datastore.CaveatReader,
	debugOption RunCaveatExpressionDebugOption,
) (ExpressionResult, error) {
	// Collect all referenced caveat definitions in the expression.
	caveatNames := mapz.NewSet[string]()
	collectCaveatNames(expr, caveatNames)

	if caveatNames.IsEmpty() {
		return nil, fmt.Errorf("received empty caveat expression")
	}

	// Bulk lookup all of the referenced caveat definitions.
	caveatDefs, err := reader.LookupCaveatsWithNames(ctx, caveatNames.AsSlice())
	if err != nil {
		return nil, err
	}

	lc := loadedCaveats{
		caveatDefs:          map[string]*core.CaveatDefinition{},
		deserializedCaveats: map[string]*caveats.CompiledCaveat{},
	}

	for _, cd := range caveatDefs {
		lc.caveatDefs[cd.Definition.GetName()] = cd.Definition
	}

	return runExpressionWithCaveats(ctx, env, expr, context, lc, debugOption)
}

type loadedCaveats struct {
	caveatDefs          map[string]*core.CaveatDefinition
	deserializedCaveats map[string]*caveats.CompiledCaveat
}

func (lc loadedCaveats) Get(caveatDefName string) (*core.CaveatDefinition, *caveats.CompiledCaveat, error) {
	caveat, ok := lc.caveatDefs[caveatDefName]
	if !ok {
		return nil, nil, datastore.NewCaveatNameNotFoundErr(caveatDefName)
	}

	deserialized, ok := lc.deserializedCaveats[caveatDefName]
	if ok {
		return caveat, deserialized, nil
	}

	parameterTypes, err := caveattypes.DecodeParameterTypes(caveat.ParameterTypes)
	if err != nil {
		return nil, nil, err
	}

	justDeserialized, err := caveats.DeserializeCaveat(caveat.SerializedExpression, parameterTypes)
	if err != nil {
		return caveat, nil, err
	}

	lc.deserializedCaveats[caveatDefName] = justDeserialized
	return caveat, justDeserialized, nil
}

func runExpressionWithCaveats(
	ctx context.Context,
	env *caveats.Environment,
	expr *core.CaveatExpression,
	context map[string]any,
	loadedCaveats loadedCaveats,
	debugOption RunCaveatExpressionDebugOption,
) (ExpressionResult, error) {
	if expr.GetCaveat() != nil {
		caveat, compiled, err := loadedCaveats.Get(expr.GetCaveat().CaveatName)
		if err != nil {
			return nil, err
		}

		// Create a combined context, with the written context taking precedence over that specified.
		untypedFullContext := maps.Clone(context)
		if untypedFullContext == nil {
			untypedFullContext = map[string]any{}
		}

		relationshipContext := expr.GetCaveat().GetContext().AsMap()
		maps.Copy(untypedFullContext, relationshipContext)

		// Perform type checking and conversion on the context map.
		typedParameters, err := caveats.ConvertContextToParameters(
			untypedFullContext,
			caveat.ParameterTypes,
			caveats.SkipUnknownParameters,
		)
		if err != nil {
			return nil, NewParameterTypeError(expr, err)
		}

		result, err := caveats.EvaluateCaveat(compiled, typedParameters)
		if err != nil {
			var evalErr caveats.EvaluationError
			if errors.As(err, &evalErr) {
				return nil, NewEvaluationError(expr, evalErr)
			}

			return nil, err
		}

		return result, nil
	}

	cop := expr.GetOperation()
	var currentResult ExpressionResult = syntheticResult{
		value:           cop.Op == core.CaveatOperation_AND,
		isPartialResult: false,
	}

	var exprResultsForDebug []ExpressionResult
	if debugOption == RunCaveatExpressionWithDebugInformation {
		exprResultsForDebug = make([]ExpressionResult, 0, len(cop.Children))
	}

	var missingVarNames *mapz.Set[string]
	if debugOption == RunCaveatExpressionNoDebugging {
		missingVarNames = mapz.NewSet[string]()
	}

	and := func(existing ExpressionResult, found ExpressionResult) (ExpressionResult, error) {
		if !existing.IsPartial() && !existing.Value() {
			return syntheticResult{
				value:               false,
				op:                  core.CaveatOperation_AND,
				exprResultsForDebug: exprResultsForDebug,
				isPartialResult:     false,
				missingVarNames:     nil,
			}, nil
		}

		if !found.IsPartial() && !found.Value() {
			return syntheticResult{
				value:               false,
				op:                  core.CaveatOperation_AND,
				exprResultsForDebug: exprResultsForDebug,
				isPartialResult:     false,
				missingVarNames:     nil,
			}, nil
		}

		value := existing.Value() && found.Value()
		if existing.IsPartial() || found.IsPartial() {
			value = false
		}

		return syntheticResult{
			value:               value,
			op:                  core.CaveatOperation_AND,
			exprResultsForDebug: exprResultsForDebug,
			isPartialResult:     existing.IsPartial() || found.IsPartial(),
			missingVarNames:     missingVarNames,
		}, nil
	}

	or := func(existing ExpressionResult, found ExpressionResult) (ExpressionResult, error) {
		if !existing.IsPartial() && existing.Value() {
			return syntheticResult{
				value:               true,
				op:                  core.CaveatOperation_OR,
				exprResultsForDebug: exprResultsForDebug,
				isPartialResult:     false,
				missingVarNames:     nil,
			}, nil
		}

		if !found.IsPartial() && found.Value() {
			return syntheticResult{
				value:               true,
				op:                  core.CaveatOperation_OR,
				exprResultsForDebug: exprResultsForDebug,
				isPartialResult:     false,
				missingVarNames:     nil,
			}, nil
		}

		value := existing.Value() || found.Value()
		if existing.IsPartial() || found.IsPartial() {
			value = false
		}

		return syntheticResult{
			value:               value,
			op:                  core.CaveatOperation_OR,
			exprResultsForDebug: exprResultsForDebug,
			isPartialResult:     existing.IsPartial() || found.IsPartial(),
			missingVarNames:     missingVarNames,
		}, nil
	}

	invert := func(existing ExpressionResult) (ExpressionResult, error) {
		value := !existing.Value()
		if existing.IsPartial() {
			value = false
		}

		return syntheticResult{
			value:               value,
			op:                  core.CaveatOperation_NOT,
			exprResultsForDebug: exprResultsForDebug,
			isPartialResult:     existing.IsPartial(),
			missingVarNames:     missingVarNames,
		}, nil
	}

	for _, child := range cop.Children {
		childResult, err := runExpressionWithCaveats(ctx, env, child, context, loadedCaveats, debugOption)
		if err != nil {
			return nil, err
		}

		if debugOption != RunCaveatExpressionNoDebugging {
			exprResultsForDebug = append(exprResultsForDebug, childResult)
		} else if childResult.IsPartial() {
			missingVars, err := childResult.MissingVarNames()
			if err != nil {
				return nil, err
			}

			missingVarNames.Extend(missingVars)
		}

		switch cop.Op {
		case core.CaveatOperation_AND:
			cr, err := and(currentResult, childResult)
			if err != nil {
				return nil, err
			}

			currentResult = cr
			if debugOption == RunCaveatExpressionNoDebugging && isFalseResult(currentResult) {
				return currentResult, nil
			}

		case core.CaveatOperation_OR:
			cr, err := or(currentResult, childResult)
			if err != nil {
				return nil, err
			}

			currentResult = cr
			if debugOption == RunCaveatExpressionNoDebugging && isTrueResult(currentResult) {
				return currentResult, nil
			}

		case core.CaveatOperation_NOT:
			return invert(childResult)

		default:
			return nil, spiceerrors.MustBugf("unknown caveat operation: %v", cop.Op)
		}
	}

	return currentResult, nil
}

func collectCaveatNames(expr *core.CaveatExpression, caveatNames *mapz.Set[string]) {
	if expr.GetCaveat() != nil {
		caveatNames.Add(expr.GetCaveat().CaveatName)
		return
	}

	cop := expr.GetOperation()
	for _, child := range cop.Children {
		collectCaveatNames(child, caveatNames)
	}
}
