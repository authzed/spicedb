package nilvaluecheck

import (
	"flag"
	"go/ast"
	"go/token"
	"slices"
	"strings"

	"github.com/samber/lo"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

func Analyzer() *analysis.Analyzer {
	flagSet := flag.NewFlagSet("nilvaluecheck", flag.ExitOnError)
	disallowedPaths := flagSet.String("disallowed-nil-return-type-paths", "", "full paths of the types for whom nil returns are disallowed")
	skip := flagSet.String("skip-pkg", "", "package(s) to skip for linting")

	return &analysis.Analyzer{
		Name: "nilvaluecheck",
		Doc:  "reports nil values used for specific types",
		Run: func(pass *analysis.Pass) (any, error) {
			// Check for a skipped package.
			if len(*skip) > 0 {
				skipped := lo.Map(strings.Split(*skip, ","), func(skipped string, _ int) string { return strings.TrimSpace(skipped) })
				for _, s := range skipped {
					if strings.Contains(pass.Pkg.Path(), s) {
						return nil, nil
					}
				}
			}

			inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

			nodeFilter := []ast.Node{
				(*ast.ReturnStmt)(nil),
				(*ast.DeclStmt)(nil),
			}

			typePaths := lo.Map(strings.Split(*disallowedPaths, ","), func(path string, _ int) string { return strings.TrimSpace(path) })
			hasTypePath := func(path string) bool {
				return slices.Contains(typePaths, path)
			}

			inspect.WithStack(nodeFilter, func(n ast.Node, push bool, stack []ast.Node) bool {
				var funcType *ast.FuncType

				if len(*disallowedPaths) == 0 {
					return false
				}

				for _, parent := range stack {
					// Find the parent function declaration or function literal.
					// Note that the stack orders from the top of the file, inward.
					if funcDecl, ok := parent.(*ast.FuncDecl); ok {
						funcType = funcDecl.Type
					}

					if r, ok := parent.(*ast.FuncLit); ok {
						funcType = r.Type
					}
				}

				if funcType == nil || funcType.Results == nil {
					return false
				}

				switch s := n.(type) {
				case *ast.ReturnStmt:
					// Check for a return value of `nil` for any disallowed type.
					for index, resultTypeExpr := range funcType.Results.List {
						if resultTypeExpr.Type == nil {
							return false
						}

						foundType := pass.TypesInfo.TypeOf(resultTypeExpr.Type)
						if foundType == nil {
							return false
						}

						resultType := foundType.String()
						if hasTypePath(resultType) {
							if ident, ok := s.Results[index].(*ast.Ident); ok && ident.Name == "nil" {
								pass.Reportf(s.Pos(), "In package %s: found `nil` returned for value of nil-disallowed type %s", pass.Pkg.Path(), resultType)
							}
						}
					}

				case *ast.DeclStmt:
					// Check for a variable without an assigned value to a disallowed type.
					if genDecl, ok := s.Decl.(*ast.GenDecl); ok && genDecl.Tok == token.VAR {
						valueSpec := genDecl.Specs[0].(*ast.ValueSpec)
						if valueSpec.Type == nil {
							return false
						}

						foundType := pass.TypesInfo.TypeOf(valueSpec.Type)
						if foundType == nil {
							return false
						}

						varType := foundType.String()
						if len(valueSpec.Values) == 0 && hasTypePath(varType) {
							names := make([]string, 0, len(valueSpec.Names))
							for _, name := range valueSpec.Names {
								names = append(names, name.Name)
							}

							pass.Reportf(s.Pos(), "In package %s: found default nil assignment to %s for nil-disallowed type %s", pass.Pkg.Path(), strings.Join(names, ", "), varType)
						}
					}
				}
				return false
			})

			return nil, nil
		},
		Requires: []*analysis.Analyzer{inspect.Analyzer},
		Flags:    *flagSet,
	}
}
