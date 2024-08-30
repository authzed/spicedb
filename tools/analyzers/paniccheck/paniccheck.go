package paniccheck

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
	flagSet := flag.NewFlagSet("paniccheck", flag.ExitOnError)
	skipPkg := flagSet.String("skip-pkg", "", "package(s) to skip for linting")
	skipFiles := flagSet.String("skip-files", "", "patterns of files to skip for linting")

	return &analysis.Analyzer{
		Name: "paniccheck",
		Doc:  "reports panics used incorrectly",
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
				skipFilePatterns = lo.Map(strings.Split(*skipPkg, ","), func(skipped string, _ int) string { return strings.TrimSpace(skipped) })
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
					identExpr, ok := s.Fun.(*ast.Ident)
					if !ok {
						return false
					}

					if identExpr.Name != "panic" {
						return false
					}

				default:
					return true
				}

				var parentFuncName string
				var parentFuncType *ast.FuncType

				for _, parent := range stack {
					// Find the parent function declaration or function literal.
					// Note that the stack orders from the top of the file, inward.
					if funcDecl, ok := parent.(*ast.FuncDecl); ok {
						parentFuncName = funcDecl.Name.Name
						parentFuncType = funcDecl.Type
						break
					}

					if funcLit, ok := parent.(*ast.FuncLit); ok {
						parentFuncName = ""
						parentFuncType = funcLit.Type
						break
					}
				}

				// Allowance rules:
				// 1) Parent function name starts with `must` or `Must`.
				if strings.HasPrefix(parentFuncName, "must") || strings.HasPrefix(parentFuncName, "Must") {
					return false
				}

				// 2) Under the default branch of a switch *AND* the parent function does not return
				// an error.
				// CallExpr -> BlockStatement -> CaseClause
				if len(stack) > 3 {
					if cc, ok := stack[len(stack)-3].(*ast.CaseClause); ok && len(cc.List) == 0 {
						hasError := false

						if parentFuncType != nil && parentFuncType.Results != nil {
							for _, result := range parentFuncType.Results.List {
								resultType := result.Type
								foundType := pass.TypesInfo.TypeOf(resultType)
								if foundType == nil {
									return false
								}

								if foundType.String() == "error" {
									hasError = true
									break
								}
							}
						}

						if !hasError {
							return false
						}
					}
				}

				pass.Reportf(n.Pos(), "In package %s: found disallowed panic statement", pass.Pkg.Path())
				return false
			})

			return nil, nil
		},
		Requires: []*analysis.Analyzer{inspect.Analyzer},
		Flags:    *flagSet,
	}
}
