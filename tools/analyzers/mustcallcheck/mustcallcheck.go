package mustcallcheck

import (
	"flag"
	"fmt"
	"go/ast"
	"regexp"
	"slices"
	"strings"

	"github.com/samber/lo"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

var (
	nameAllowList = []string{
		// MustBugf returns an error in production, only panics in tests
		"MustBugf",
		// MustPanicf is explicitly for violating the linter when necessary
		"MustPanicf",
	}
	packageAllowList = []string{
		// cobrautil has many Must* functions but doesn't have non-Must variants
		"cobrautil",
	}
)

func Analyzer() *analysis.Analyzer {
	flagSet := flag.NewFlagSet("mustcallcheck", flag.ExitOnError)
	skipPkg := flagSet.String("skip-pkg", "", "package(s) to skip for linting")
	skipFiles := flagSet.String("skip-files", "", "patterns of files to skip for linting")

	return &analysis.Analyzer{
		Name: "mustcallcheck",
		Doc:  "reports Must* method calls in error-returning functions",
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
					// Extract the method name from the call expression
					var methodName string
					switch fun := s.Fun.(type) {
					case *ast.Ident:
						// Direct function call: MustParse()
						methodName = fun.Name
					case *ast.SelectorExpr:
						// Method call: obj.MustParse() or pkg.MustParse()
						methodName = fun.Sel.Name
					default:
						return false
					}

					// Check if the method name starts with "Must"
					if !strings.HasPrefix(methodName, "Must") {
						return false
					}

					// Skip function names on allow list
					if slices.Contains(nameAllowList, methodName) {
						return false
					}

					// Skip packages on the allow list
					if sel, ok := s.Fun.(*ast.SelectorExpr); ok {
						if pkgIdent, ok := sel.X.(*ast.Ident); ok {
							if slices.Contains(packageAllowList, pkgIdent.Name) {
								return false
							}
						}
					}

					// Find the parent function declaration or function literal
					var parentFuncType *ast.FuncType
					for i := len(stack) - 1; i >= 0; i-- {
						parent := stack[i]
						if funcDecl, ok := parent.(*ast.FuncDecl); ok {
							parentFuncType = funcDecl.Type
							break
						}
						if funcLit, ok := parent.(*ast.FuncLit); ok {
							parentFuncType = funcLit.Type
							break
						}
					}

					// If we couldn't find a parent function, skip
					if parentFuncType == nil {
						return false
					}

					// Check if the parent function returns an error
					hasErrorReturn := false
					if parentFuncType.Results != nil {
						for _, result := range parentFuncType.Results.List {
							resultType := result.Type
							foundType := pass.TypesInfo.TypeOf(resultType)
							if foundType == nil {
								continue
							}

							if foundType.String() == "error" {
								hasErrorReturn = true
								break
							}
						}
					}

					// If the parent function returns an error, report the Must* call
					if hasErrorReturn {
						// Suggest the non-Must version by removing "Must" prefix
						suggestedMethod := strings.TrimPrefix(methodName, "Must")
						pass.Reportf(n.Pos(), "In package %s: found call to %s in error-returning function; use %s instead to properly handle errors", pass.Pkg.Path(), methodName, suggestedMethod)
					}

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
