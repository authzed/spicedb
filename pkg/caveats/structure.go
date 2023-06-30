package caveats

import (
	"fmt"

	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
)

// referencedParameters traverses the expression given and finds all parameters which are referenced
// in the expression for the purpose of usage tracking.
func referencedParameters(definedParameters *mapz.Set[string], expr *exprpb.Expr, referencedParams *mapz.Set[string]) {
	if expr == nil {
		return
	}

	switch t := expr.ExprKind.(type) {
	case *exprpb.Expr_ConstExpr:
		// nothing to do

	case *exprpb.Expr_IdentExpr:
		if definedParameters.Has(t.IdentExpr.Name) {
			referencedParams.Add(t.IdentExpr.Name)
		}

	case *exprpb.Expr_SelectExpr:
		referencedParameters(definedParameters, t.SelectExpr.Operand, referencedParams)

	case *exprpb.Expr_CallExpr:
		referencedParameters(definedParameters, t.CallExpr.Target, referencedParams)
		for _, arg := range t.CallExpr.Args {
			referencedParameters(definedParameters, arg, referencedParams)
		}

	case *exprpb.Expr_ListExpr:
		for _, elem := range t.ListExpr.Elements {
			referencedParameters(definedParameters, elem, referencedParams)
		}

	case *exprpb.Expr_StructExpr:
		for _, entry := range t.StructExpr.Entries {
			referencedParameters(definedParameters, entry.Value, referencedParams)
		}

	case *exprpb.Expr_ComprehensionExpr:
		referencedParameters(definedParameters, t.ComprehensionExpr.AccuInit, referencedParams)
		referencedParameters(definedParameters, t.ComprehensionExpr.IterRange, referencedParams)
		referencedParameters(definedParameters, t.ComprehensionExpr.LoopCondition, referencedParams)
		referencedParameters(definedParameters, t.ComprehensionExpr.LoopStep, referencedParams)
		referencedParameters(definedParameters, t.ComprehensionExpr.Result, referencedParams)

	default:
		panic(fmt.Sprintf("unknown CEL expression kind: %T", t))
	}
}
