package caveats

import (
	"fmt"
	"maps"
	"strconv"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// BuildDebugInformation returns a human-readable string representation of the given
// ExpressionResult and a Struct representation of the context values used in the expression.
func BuildDebugInformation(exprResult ExpressionResult) (string, *structpb.Struct, error) {
	// If a concrete result, return its information directly.
	if concrete, ok := exprResult.(*caveats.CaveatResult); ok {
		exprString, err := concrete.ParentCaveat().ExprString()
		if err != nil {
			return "", nil, err
		}

		contextStruct, err := concrete.ContextStruct()
		if err != nil {
			return "", nil, err
		}

		return exprString, contextStruct, nil
	}

	// Collect parameters which are shared across expressions.
	syntheticResult, ok := exprResult.(syntheticResult)
	if !ok {
		return "", nil, spiceerrors.MustBugf("unknown ExpressionResult type: %T", exprResult)
	}

	resultsByParam := mapz.NewMultiMap[string, *caveats.CaveatResult]()
	if err := collectParameterUsage(syntheticResult, resultsByParam); err != nil {
		return "", nil, err
	}

	// Build the synthetic debug information.
	exprString, contextMap, err := buildDebugInformation(syntheticResult, resultsByParam)
	if err != nil {
		return "", nil, err
	}

	// Convert the context map to a struct.
	contextStruct, err := caveats.ConvertContextToStruct(contextMap)
	if err != nil {
		return "", nil, err
	}

	return exprString, contextStruct, nil
}

func buildDebugInformation(sr syntheticResult, resultsByParam *mapz.MultiMap[string, *caveats.CaveatResult]) (string, map[string]any, error) {
	childExprStrings := make([]string, 0, len(sr.exprResultsForDebug))
	combinedContext := map[string]any{}

	for _, child := range sr.exprResultsForDebug {
		if _, ok := child.(*caveats.CaveatResult); ok {
			childExprString, contextMap, err := buildDebugInformationForConcrete(child.(*caveats.CaveatResult), resultsByParam)
			if err != nil {
				return "", nil, err
			}

			childExprStrings = append(childExprStrings, "("+childExprString+")")
			maps.Copy(combinedContext, contextMap)
			continue
		}

		childExprString, contextMap, err := buildDebugInformation(child.(syntheticResult), resultsByParam)
		if err != nil {
			return "", nil, err
		}

		childExprStrings = append(childExprStrings, "("+childExprString+")")
		maps.Copy(combinedContext, contextMap)
	}

	var combinedExprString string
	switch sr.op {
	case corev1.CaveatOperation_AND:
		combinedExprString = strings.Join(childExprStrings, " && ")

	case corev1.CaveatOperation_OR:
		combinedExprString = strings.Join(childExprStrings, " || ")

	case corev1.CaveatOperation_NOT:
		if len(childExprStrings) != 1 {
			return "", nil, spiceerrors.MustBugf("NOT operator must have exactly one child")
		}

		combinedExprString = "!" + childExprStrings[0]

	default:
		return "", nil, fmt.Errorf("unknown operator: %v", sr.op)
	}

	return combinedExprString, combinedContext, nil
}

func buildDebugInformationForConcrete(cr *caveats.CaveatResult, resultsByParam *mapz.MultiMap[string, *caveats.CaveatResult]) (string, map[string]any, error) {
	// For each paramter used in the context of the caveat, check if it is shared across multiple
	// caveats. If so, rewrite the parameter to a unique name.
	existingContextMap := cr.ContextValues()
	contextMap := make(map[string]any, len(existingContextMap))

	caveat := *cr.ParentCaveat()

	for paramName, paramValue := range existingContextMap {
		index := mapz.IndexOfValueInMultimap(resultsByParam, paramName, cr)
		if resultsByParam.CountOf(paramName) > 1 {
			newName := paramName + "__" + strconv.Itoa(index)
			if resultsByParam.Has(newName) {
				return "", nil, fmt.Errorf("failed to generate unique name for parameter: %s", newName)
			}

			rewritten, err := caveat.RewriteVariable(paramName, newName)
			if err != nil {
				return "", nil, err
			}

			caveat = rewritten
			contextMap[newName] = paramValue
			continue
		}

		contextMap[paramName] = paramValue
	}

	exprString, err := caveat.ExprString()
	if err != nil {
		return "", nil, err
	}

	return exprString, contextMap, nil
}

func collectParameterUsage(sr syntheticResult, resultsByParam *mapz.MultiMap[string, *caveats.CaveatResult]) error {
	for _, exprResult := range sr.exprResultsForDebug {
		if concrete, ok := exprResult.(*caveats.CaveatResult); ok {
			for paramName := range concrete.ContextValues() {
				resultsByParam.Add(paramName, concrete)
			}
		} else {
			cast, ok := exprResult.(syntheticResult)
			if !ok {
				return spiceerrors.MustBugf("unknown ExpressionResult type: %T", exprResult)
			}

			if err := collectParameterUsage(cast, resultsByParam); err != nil {
				return err
			}
		}
	}

	return nil
}
