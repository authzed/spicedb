package caveats

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var tracer = otel.Tracer("spicedb/internal/caveats/run")

// RunCaveatExpressionDebugOption are the options for running caveat expression evaluation
// with debugging enabled or disabled.
type RunCaveatExpressionDebugOption int

const (
	// RunCaveatExpressionNoDebugging runs the evaluation without debugging enabled.
	RunCaveatExpressionNoDebugging RunCaveatExpressionDebugOption = 0

	// RunCaveatExpressionWithDebugInformation runs the evaluation with debugging enabled.
	RunCaveatExpressionWithDebugInformation RunCaveatExpressionDebugOption = 1
)

// RunSingleCaveatExpression runs a caveat expression over the given context and returns the result.
// This instantiates its own CaveatRunner, and should therefore only be used in one-off situations.
func RunSingleCaveatExpression(
	ctx context.Context,
	expr *core.CaveatExpression,
	context map[string]any,
	reader datastore.CaveatReader,
	debugOption RunCaveatExpressionDebugOption,
) (ExpressionResult, error) {
	runner := NewCaveatRunner()
	return runner.RunCaveatExpression(ctx, expr, context, reader, debugOption)
}

// CaveatRunner is a helper for running caveats, providing a cache for deserialized caveats.
type CaveatRunner struct {
	caveatDefs          map[string]*core.CaveatDefinition
	deserializedCaveats map[string]*caveats.CompiledCaveat
}

// NewCaveatRunner creates a new CaveatRunner.
func NewCaveatRunner() *CaveatRunner {
	return &CaveatRunner{
		caveatDefs:          map[string]*core.CaveatDefinition{},
		deserializedCaveats: map[string]*caveats.CompiledCaveat{},
	}
}

// RunCaveatExpression runs a caveat expression over the given context and returns the result.
func (cr *CaveatRunner) RunCaveatExpression(
	ctx context.Context,
	expr *core.CaveatExpression,
	context map[string]any,
	reader datastore.CaveatReader,
	debugOption RunCaveatExpressionDebugOption,
) (ExpressionResult, error) {
	ctx, span := tracer.Start(ctx, "RunCaveatExpression")
	defer span.End()

	if err := cr.PopulateCaveatDefinitionsForExpr(ctx, expr, reader); err != nil {
		return nil, err
	}

	env := caveats.NewEnvironment()
	return cr.runExpressionWithCaveats(ctx, env, expr, context, debugOption)
}

// PopulateCaveatDefinitionsForExpr populates the CaveatRunner's cache with the definitions
// referenced in the given caveat expression.
func (cr *CaveatRunner) PopulateCaveatDefinitionsForExpr(ctx context.Context, expr *core.CaveatExpression, reader datastore.CaveatReader) error {
	ctx, span := tracer.Start(ctx, "PopulateCaveatDefinitions")
	defer span.End()

	// Collect all referenced caveat definitions in the expression.
	caveatNames := mapz.NewSet[string]()
	collectCaveatNames(expr, caveatNames)

	span.AddEvent("collected caveat names")
	span.SetAttributes(attribute.StringSlice("caveat-names", caveatNames.AsSlice()))

	if caveatNames.IsEmpty() {
		return fmt.Errorf("received empty caveat expression")
	}

	// Remove any caveats already loaded.
	for name := range cr.caveatDefs {
		caveatNames.Delete(name)
	}

	if caveatNames.IsEmpty() {
		return nil
	}

	// Bulk lookup all of the referenced caveat definitions.
	caveatDefs, err := reader.LookupCaveatsWithNames(ctx, caveatNames.AsSlice())
	if err != nil {
		return err
	}
	span.AddEvent("looked up caveats")

	for _, cd := range caveatDefs {
		cr.caveatDefs[cd.Definition.GetName()] = cd.Definition
	}

	return nil
}

// get retrieves a caveat definition and its deserialized form. The caveat name must be
// present in the CaveatRunner's cache.
func (cr *CaveatRunner) get(caveatDefName string) (*core.CaveatDefinition, *caveats.CompiledCaveat, error) {
	caveat, ok := cr.caveatDefs[caveatDefName]
	if !ok {
		return nil, nil, datastore.NewCaveatNameNotFoundErr(caveatDefName)
	}

	deserialized, ok := cr.deserializedCaveats[caveatDefName]
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

	cr.deserializedCaveats[caveatDefName] = justDeserialized
	return caveat, justDeserialized, nil
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

func (cr *CaveatRunner) runExpressionWithCaveats(
	ctx context.Context,
	env *caveats.Environment,
	expr *core.CaveatExpression,
	context map[string]any,
	debugOption RunCaveatExpressionDebugOption,
) (ExpressionResult, error) {
	ctx, span := tracer.Start(ctx, "runExpressionWithCaveats")
	defer span.End()

	if expr.GetCaveat() != nil {
		span.SetAttributes(attribute.String("caveat-name", expr.GetCaveat().CaveatName))

		caveat, compiled, err := cr.get(expr.GetCaveat().CaveatName)
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
	span.SetAttributes(attribute.String("caveat-operation", cop.Op.String()))

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
		childResult, err := cr.runExpressionWithCaveats(ctx, env, child, context, debugOption)
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
