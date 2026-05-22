package singleflightcheck

import (
	"flag"
	"fmt"
	"go/ast"
	"regexp"
	"strings"

	"github.com/samber/lo"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

func Analyzer() *analysis.Analyzer {
	flagSet := flag.NewFlagSet("singleflightcheck", flag.ExitOnError)
	skipPkg := flagSet.String("skip-pkg", "", "package(s) to skip for linting")
	skipFiles := flagSet.String("skip-files", "", "patterns of files to skip for linting")

	return &analysis.Analyzer{
		Name: "singleflightcheck",
		Doc:  "reports uses of golang.org/x/sync/singleflight.Group.Do in functions that have a context.Context parameter; use resenje.org/singleflight instead",
		Run: func(pass *analysis.Pass) (any, error) {
			// Check for a skipped package.
			if len(*skipPkg) > 0 {
				skipped := lo.Map(strings.Split(*skipPkg, ","), func(skipped string, _ int) string { return strings.TrimSpace(skipped) })
				for _, s := range skipped {
					if strings.Contains(pass.Pkg.Path(), s) {
						return nil, nil
					}
				}
			}

			// Check for a skipped file.
			skipFilePatterns := make([]string, 0)
			if len(*skipFiles) > 0 {
				skipFilePatterns = lo.Map(strings.Split(*skipFiles, ","), func(skipped string, _ int) string { return strings.TrimSpace(skipped) })
			}
			for _, pattern := range skipFilePatterns {
				_, err := regexp.Compile(pattern)
				if err != nil {
					return nil, fmt.Errorf("invalid skip-files pattern `%s`: %w", pattern, err)
				}
			}

			inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

			nodeFilter := []ast.Node{
				(*ast.File)(nil),
				(*ast.CallExpr)(nil),
			}

			inspect.WithStack(nodeFilter, func(n ast.Node, push bool, stack []ast.Node) bool {
				switch s := n.(type) {
				case *ast.File:
					for _, pattern := range skipFilePatterns {
						isMatch, _ := regexp.MatchString(pattern, pass.Fset.Position(s.Package).Filename)
						if isMatch {
							return false
						}
					}
					return true

				case *ast.CallExpr:
					// Check if this is a call to .Do on a selector expression.
					selector, ok := s.Fun.(*ast.SelectorExpr)
					if !ok || selector.Sel.Name != "Do" {
						return true
					}

					// Check that the receiver type is golang.org/x/sync/singleflight.Group.
					receiverType := pass.TypesInfo.TypeOf(selector.X)
					if receiverType == nil {
						return true
					}

					typeStr := receiverType.String()
					if !strings.Contains(typeStr, "golang.org/x/sync/singleflight") {
						return true
					}

					// Check if the enclosing function has a context.Context parameter.
					if !enclosingFuncHasContext(stack) {
						return true
					}

					pass.Reportf(n.Pos(), "In package %s: use resenje.org/singleflight instead of golang.org/x/sync/singleflight in functions with context.Context", pass.Pkg.Path())
					return false

				default:
					return true
				}
			})

			return nil, nil
		},
		Requires: []*analysis.Analyzer{inspect.Analyzer},
		Flags:    *flagSet,
	}
}

func enclosingFuncHasContext(stack []ast.Node) bool {
	for i := len(stack) - 1; i >= 0; i-- {
		var params *ast.FieldList
		switch f := stack[i].(type) {
		case *ast.FuncDecl:
			params = f.Type.Params
		case *ast.FuncLit:
			params = f.Type.Params
		default:
			continue
		}

		if params == nil {
			return false
		}

		for _, param := range params.List {
			if sel, ok := param.Type.(*ast.SelectorExpr); ok {
				if ident, ok := sel.X.(*ast.Ident); ok {
					if ident.Name == "context" && sel.Sel.Name == "Context" {
						return true
					}
				}
			}
		}
		return false
	}
	return false
}
