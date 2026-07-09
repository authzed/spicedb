package staticformatcheck

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// checkedFuncNames holds the names of the format-style functions whose format
// string argument must be a compile-time constant. The format string is
// expected to be the last argument before the variadic format arguments.
//
// Matching is intentionally by name only, not scoped to a type or package, so
// names here must be distinctive enough not to collide with unrelated
// same-named methods. WithSourceErrorf is unique to the schema compiler's
// dslNode; a generic name like Errorf would over-match fmt.Errorf,
// testing.T.Errorf, and others and cannot be added without type scoping.
var checkedFuncNames = map[string]struct{}{
	"WithSourceErrorf": {},
}

func Analyzer() *analysis.Analyzer {
	return &analysis.Analyzer{
		Name:     "staticformatcheck",
		Doc:      "reports calls to WithSourceErrorf whose format string argument is not a compile-time constant",
		Run:      run,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
	}
}

func run(pass *analysis.Pass) (any, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	nodeFilter := []ast.Node{(*ast.CallExpr)(nil)}
	inspect.Preorder(nodeFilter, func(n ast.Node) {
		call := n.(*ast.CallExpr)

		selectorExpr, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return
		}

		if _, ok := checkedFuncNames[selectorExpr.Sel.Name]; !ok {
			return
		}

		// Ensure the callee is a function and derive the format string index
		// from the call site's signature: the format string is the parameter
		// immediately before the variadic format arguments. For method
		// expressions the receiver appears as the first parameter, which shifts
		// the format string index accordingly.
		if _, ok := pass.TypesInfo.Uses[selectorExpr.Sel].(*types.Func); !ok {
			return
		}

		signature, ok := pass.TypesInfo.TypeOf(call.Fun).(*types.Signature)
		if !ok || !signature.Variadic() {
			return
		}

		formatArgIndex := signature.Params().Len() - 2
		if formatArgIndex < 0 || len(call.Args) <= formatArgIndex {
			return
		}

		formatParamType, ok := signature.Params().At(formatArgIndex).Type().(*types.Basic)
		if !ok || formatParamType.Kind() != types.String {
			return
		}

		formatArg := call.Args[formatArgIndex]
		if tv, ok := pass.TypesInfo.Types[formatArg]; ok && tv.Value != nil {
			// The format string is a compile-time constant.
			return
		}

		pass.Reportf(formatArg.Pos(), "format string argument to `%s` must be a static string", selectorExpr.Sel.Name)
	})

	return nil, nil
}
