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

	switch t := expr.GetExprKind().(type) {
	case *exprpb.Expr_ConstExpr:
		// nothing to do

	case *exprpb.Expr_IdentExpr:
		if definedParameters.Has(t.IdentExpr.GetName()) {
			referencedParams.Add(t.IdentExpr.GetName())
		}

	case *exprpb.Expr_SelectExpr:
		referencedParameters(definedParameters, t.SelectExpr.GetOperand(), referencedParams)

	case *exprpb.Expr_CallExpr:
		referencedParameters(definedParameters, t.CallExpr.GetTarget(), referencedParams)
		for _, arg := range t.CallExpr.GetArgs() {
			referencedParameters(definedParameters, arg, referencedParams)
		}

	case *exprpb.Expr_ListExpr:
		for _, elem := range t.ListExpr.GetElements() {
			referencedParameters(definedParameters, elem, referencedParams)
		}

	case *exprpb.Expr_StructExpr:
		for _, entry := range t.StructExpr.GetEntries() {
			referencedParameters(definedParameters, entry.GetValue(), referencedParams)
		}

	case *exprpb.Expr_ComprehensionExpr:
		referencedParameters(definedParameters, t.ComprehensionExpr.GetAccuInit(), referencedParams)
		referencedParameters(definedParameters, t.ComprehensionExpr.GetIterRange(), referencedParams)
		referencedParameters(definedParameters, t.ComprehensionExpr.GetLoopCondition(), referencedParams)
		referencedParameters(definedParameters, t.ComprehensionExpr.GetLoopStep(), referencedParams)
		referencedParameters(definedParameters, t.ComprehensionExpr.GetResult(), referencedParams)

	default:
		panic(fmt.Sprintf("unknown CEL expression kind: %T", t))
	}
}
