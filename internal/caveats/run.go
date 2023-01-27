package caveats

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// RunCaveatExpressionDebugOption are the options for running caveat expression evaluation
// with debugging enabled or disabled.
type RunCaveatExpressionDebugOption int

const (
	// RunCaveatExpressionNoDebugging runs the evaluation without debugging enabled.
	RunCaveatExpressionNoDebugging RunCaveatExpressionDebugOption = 0

	// RunCaveatExpressionNoDebugging runs the evaluation with debugging enabled.
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

	// ContextValues returns the context values used when computing this result.
	ContextValues() map[string]any

	// ContextStruct returns the context values as a structpb Struct.
	ContextStruct() (*structpb.Struct, error)

	// ExpressionString returns the human-readable expression for the caveat expression.
	ExpressionString() (string, error)
}

type syntheticResult struct {
	value         bool
	contextValues map[string]any
	exprString    string
}

func (sr syntheticResult) Value() bool {
	return sr.value
}

func (sr syntheticResult) IsPartial() bool {
	return false
}

func (sr syntheticResult) MissingVarNames() ([]string, error) {
	return nil, fmt.Errorf("not a partial value")
}

func (sr syntheticResult) ContextValues() map[string]any {
	return sr.contextValues
}

func (sr syntheticResult) ContextStruct() (*structpb.Struct, error) {
	return caveats.ConvertContextToStruct(sr.contextValues)
}

func (sr syntheticResult) ExpressionString() (string, error) {
	return sr.exprString, nil
}

func runExpression(
	ctx context.Context,
	env *caveats.Environment,
	expr *core.CaveatExpression,
	context map[string]any,
	reader datastore.CaveatReader,
	debugOption RunCaveatExpressionDebugOption,
) (ExpressionResult, error) {
	if expr.GetCaveat() != nil {
		caveat, _, err := reader.ReadCaveatByName(ctx, expr.GetCaveat().CaveatName)
		if err != nil {
			return nil, err
		}

		compiled, err := caveats.DeserializeCaveat(caveat.SerializedExpression)
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
			var evalErr caveats.EvaluationErr
			if errors.As(err, &evalErr) {
				return nil, NewEvaluationErr(expr, evalErr)
			}

			return nil, err
		}

		return result, nil
	}

	cop := expr.GetOperation()
	boolResult := false
	if cop.Op == core.CaveatOperation_AND {
		boolResult = true
	}

	var contextValues map[string]any
	var exprStringPieces []string

	buildExprString := func() (string, error) {
		switch cop.Op {
		case core.CaveatOperation_AND:
			return strings.Join(exprStringPieces, " && "), nil

		case core.CaveatOperation_OR:
			return strings.Join(exprStringPieces, " || "), nil

		case core.CaveatOperation_NOT:
			return strings.Join(exprStringPieces, " "), nil

		default:
			return "", spiceerrors.MustBugf("unknown caveat operation: %v", cop.Op)
		}
	}

	for _, child := range cop.Children {
		childResult, err := runExpression(ctx, env, child, context, reader, debugOption)
		if err != nil {
			return nil, err
		}

		if childResult.IsPartial() {
			return childResult, nil
		}

		switch cop.Op {
		case core.CaveatOperation_AND:
			boolResult = boolResult && childResult.Value()

			if debugOption == RunCaveatExpressionWithDebugInformation {
				contextValues = combineMaps(contextValues, childResult.ContextValues())
				exprString, err := childResult.ExpressionString()
				if err != nil {
					return nil, err
				}

				exprStringPieces = append(exprStringPieces, exprString)
			}

			if !boolResult {
				built, err := buildExprString()
				if err != nil {
					return nil, err
				}

				return syntheticResult{false, contextValues, built}, nil
			}

		case core.CaveatOperation_OR:
			boolResult = boolResult || childResult.Value()

			if debugOption == RunCaveatExpressionWithDebugInformation {
				contextValues = combineMaps(contextValues, childResult.ContextValues())
				exprString, err := childResult.ExpressionString()
				if err != nil {
					return nil, err
				}

				exprStringPieces = append(exprStringPieces, exprString)
			}

			if boolResult {
				built, err := buildExprString()
				if err != nil {
					return nil, err
				}

				return syntheticResult{true, contextValues, built}, nil
			}

		case core.CaveatOperation_NOT:
			if debugOption == RunCaveatExpressionWithDebugInformation {
				contextValues = combineMaps(contextValues, childResult.ContextValues())
				exprString, err := childResult.ExpressionString()
				if err != nil {
					return nil, err
				}

				exprStringPieces = append(exprStringPieces, "!("+exprString+")")
			}

			built, err := buildExprString()
			if err != nil {
				return nil, err
			}

			return syntheticResult{!childResult.Value(), contextValues, built}, nil

		default:
			return nil, spiceerrors.MustBugf("unknown caveat operation: %v", cop.Op)
		}
	}

	built, err := buildExprString()
	if err != nil {
		return nil, err
	}

	return syntheticResult{boolResult, contextValues, built}, nil
}

func combineMaps(first map[string]any, second map[string]any) map[string]any {
	if first == nil {
		first = make(map[string]any, len(second))
	}

	cloned := maps.Clone(first)
	maps.Copy(cloned, second)
	return cloned
}
