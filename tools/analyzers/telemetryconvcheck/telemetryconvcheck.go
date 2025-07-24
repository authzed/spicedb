package telemetryconvcheck

import (
	"flag"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"slices"
	"strings"

	"github.com/samber/lo"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

const defaultConvPackage = "github.com/authzed/spicedb/internal/telemetry/otelconv"

var (
	validPrefixes = []string{"spicedb.internal.", "spicedb.external."}
	convPackage   *string
)

func Analyzer() *analysis.Analyzer {
	flagSet := flag.NewFlagSet("telemetryconvcheck", flag.ExitOnError)
	skip := flagSet.String("skip-pkg", "", "package(s) to skip for linting")
	convPackage = flagSet.String("conv-pkg", defaultConvPackage, "package that should contain the constants to use instead of the hardcoded strings")

	return &analysis.Analyzer{
		Name: "telemetryconvcheck",
		Doc:  "disallow OpenTelemetry event and attribute names that use hardcoded strings instead of the constants from otelconv",
		Run: func(pass *analysis.Pass) (any, error) {
			// Check for a skipped package
			if len(*skip) > 0 {
				skipped := lo.Map(strings.Split(*skip, ","), func(skipped string, _ int) string { return strings.TrimSpace(skipped) })
				for _, s := range skipped {
					if strings.Contains(pass.Pkg.Path(), s) {
						return nil, nil
					}
				}
			}

			inspectorInst := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
			inspectorInst.Preorder([]ast.Node{(*ast.CallExpr)(nil)}, func(node ast.Node) {
				callExpr := node.(*ast.CallExpr)
				functionName := getFunctionName(callExpr)
				if functionName == "" {
					return
				}

				if !(isOtelTracingCall(pass, callExpr) || containsPattern(functionName, otelTracingFunctionPatterns)) {
					return
				}

				checkArguments(pass, callExpr, functionName)
			})

			return nil, nil
		},
		Requires: []*analysis.Analyzer{inspect.Analyzer},
		Flags:    *flagSet,
	}
}

var otelTracingFunctionPatterns = []string{
	// Event methods
	"AddEvent",

	// Attribute creation functions
	"attribute.String",
	"attribute.Int",
	"attribute.Int64",
	"attribute.Float64",
	"attribute.Bool",
	"attribute.StringSlice",
	"attribute.IntSlice",
	"attribute.Int64Slice",
	"attribute.Float64Slice",
	"attribute.BoolSlice",

	// Attribute setting methods
	"SetAttributes",
	"WithAttributes",
}

func containsPattern(functionName string, patterns []string) bool {
	for _, pattern := range patterns {
		if strings.Contains(functionName, pattern) {
			return true
		}
	}
	return false
}

func isOtelTracingCall(pass *analysis.Pass, callExpr *ast.CallExpr) bool {
	selExpr, ok := callExpr.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	typ := pass.TypesInfo.TypeOf(selExpr.X)
	if typ == nil {
		return false
	}

	typeName := typ.String()
	return strings.Contains(typeName, "go.opentelemetry.io/otel/trace") ||
		strings.Contains(typeName, "trace.Span") ||
		strings.Contains(typeName, "attribute.Key")
}

func getFunctionName(callExpr *ast.CallExpr) string {
	switch fun := callExpr.Fun.(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		if ident, ok := fun.X.(*ast.Ident); ok {
			return ident.Name + "." + fun.Sel.Name
		}
		return fun.Sel.Name
	default:
		return ""
	}
}

func checkArguments(pass *analysis.Pass, callExpr *ast.CallExpr, functionName string) {
	switch {
	case strings.Contains(functionName, "AddEvent"):
		if len(callExpr.Args) > 0 {
			checkStringArgument(pass, callExpr.Args[0], "event name")
		}
		if len(callExpr.Args) > 1 {
			for _, arg := range callExpr.Args[1:] {
				if optCall, ok := arg.(*ast.CallExpr); ok {
					if name := getFunctionName(optCall); name == "WithAttributes" || name == "trace.WithAttributes" {
						checkAttributes(pass, optCall.Args)
					}
				}
			}
		}
	case strings.Contains(functionName, "SetAttributes") || strings.Contains(functionName, "WithAttributes"):
		checkAttributes(pass, callExpr.Args)
	case strings.Contains(functionName, "attribute."):
		if len(callExpr.Args) > 0 {
			checkStringArgument(pass, callExpr.Args[0], "attribute key")
		}
	}
}

func checkAttributes(pass *analysis.Pass, args []ast.Expr) {
	for _, arg := range args {
		ast.Inspect(arg, func(node ast.Node) bool {
			callExpr, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}

			functionName := getFunctionName(callExpr)
			if strings.Contains(functionName, "attribute.") && len(callExpr.Args) > 0 {
				checkStringArgument(pass, callExpr.Args[0], "attribute key")
			}
			return true
		})
	}
}

func checkStringArgument(pass *analysis.Pass, arg ast.Expr, description string) {
	if lit, ok := arg.(*ast.BasicLit); ok && lit.Kind == token.STRING {
		pass.Reportf(arg.Pos(), "use a constant from github.com/authzed/spicedb/internal/telemetry/otelconv for %s instead of the hardcoded string %s", description, lit.Value)
		return
	}

	var ident *ast.Ident
	switch n := arg.(type) {
	case *ast.Ident:
		ident = n
	case *ast.SelectorExpr:
		ident = n.Sel
	default:
		return
	}

	obj := pass.TypesInfo.ObjectOf(ident)
	if obj == nil {
		return
	}

	if _, ok := obj.(*types.Const); !ok {
		return
	}

	pkg := obj.Pkg()
	if pkg == nil || pkg.Path() != *convPackage {
		var pkgPath string
		if pkg != nil {
			pkgPath = pkg.Path()
		} else {
			pkgPath = "the current package"
		}
		pass.Reportf(
			arg.Pos(),
			"use a constant from `%s` for %s, not from `%s`",
			*convPackage,
			description,
			pkgPath,
		)
	}

	if constObj, ok := obj.(*types.Const); ok {
		if constObj.Val().Kind() != constant.String {
			pass.Reportf(arg.Pos(), "constant %s from %s should be a string, but has type %s", ident.Name, *convPackage, constObj.Type().String())
		}
		constValue := constant.StringVal(constObj.Val())
		if !slices.ContainsFunc(validPrefixes, func(prefix string) bool {
			return strings.HasPrefix(constValue, prefix)
		}) {
			pass.Reportf(
				arg.Pos(),
				"constant %s from %s should start with one of %q, but has value %q",
				ident.Name,
				*convPackage,
				validPrefixes,
				constValue,
			)
		}
	}
}
