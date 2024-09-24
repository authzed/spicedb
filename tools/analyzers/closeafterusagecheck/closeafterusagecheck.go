package closeafterusagecheck

import (
	"flag"
	"go/ast"
	"slices"
	"strings"

	"github.com/samber/lo"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

type nodeAndStack struct {
	node  ast.Node
	stack []ast.Node
}

func Analyzer() *analysis.Analyzer {
	flagSet := flag.NewFlagSet("closeafterusagecheck", flag.ExitOnError)
	mustBeClosedTypes := flagSet.String(
		"must-be-closed-after-usage-types",
		"",
		`full paths of the types for whom Close() must be immediately closed after usage`,
	)
	skip := flagSet.String("skip-pkg", "", "package(s) to skip for linting")

	return &analysis.Analyzer{
		Name: "closeafterusagecheck",
		Doc:  "reports any variables of the specified types that are not Close()-ed immediately after usage",
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

			entries := strings.Split(*mustBeClosedTypes, ";")
			typePaths := map[string]struct{}{}
			for _, entry := range entries {
				if len(entry) == 0 {
					continue
				}

				typePaths[entry] = struct{}{}
			}

			inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

			nodeFilter := []ast.Node{
				(*ast.ExprStmt)(nil),
				(*ast.AssignStmt)(nil),
				(*ast.CallExpr)(nil),
				(*ast.Ident)(nil),
				(*ast.DeferStmt)(nil),
			}

			varsToWatch := map[string]nodeAndStack{}
			varsLastUsed := map[string]nodeAndStack{}
			varsClosed := map[string]nodeAndStack{}

			inspect.WithStack(nodeFilter, func(n ast.Node, push bool, stack []ast.Node) bool {
				switch s := n.(type) {
				case *ast.DeferStmt:
					// Skip so we don't treat this as a valid immediately Close.
					return false

				case *ast.CallExpr:
					foundType := pass.TypesInfo.TypeOf(s.Fun)
					if foundType == nil {
						return false
					}

					if selector, ok := s.Fun.(*ast.SelectorExpr); ok {
						if selector.Sel.Name == "Close" {
							if ident, ok := selector.X.(*ast.Ident); ok {
								foundType := pass.TypesInfo.TypeOf(selector.X)
								if foundType == nil {
									return false
								}

								varTypeString := foundType.String()
								if _, ok := typePaths[varTypeString]; ok {
									varName := fullName(ident, stack)
									varsClosed[varName] = nodeAndStack{n, slices.Clone(stack)}
									return false
								}
							}
						}
					}

					return true

				case *ast.Ident:
					varName := fullName(s, stack)
					if _, ok := varsToWatch[varName]; ok {
						varsLastUsed[varName] = nodeAndStack{s, slices.Clone(stack)}
					}

				case *ast.AssignStmt:
					for _, expr := range s.Lhs {
						foundType := pass.TypesInfo.TypeOf(expr)
						if foundType == nil {
							continue
						}

						varTypeString := foundType.String()
						if _, ok := typePaths[varTypeString]; ok {
							varName := fullName(expr.(*ast.Ident), stack)
							varsToWatch[varName] = nodeAndStack{expr, slices.Clone(stack)}
						}
					}
				}

				return true
			})

			for varName, nas := range varsToWatch {
				closed, ok := varsClosed[varName]
				if !ok {
					pass.Reportf(nas.node.Pos(), "In package %s: variable %s is missing required non-defer-ed Close() call after usage", pass.Pkg.Path(), varName)
					continue
				}

				lastUsed, ok := varsLastUsed[varName]
				if !ok {
					lastUsed = nas
				}

				if !isStatementRightAfter(lastUsed, closed) {
					pass.Reportf(lastUsed.node.End(), "In package %s: expected variable %s to have a call to Close after here", pass.Pkg.Path(), varName)
					continue
				}
			}

			return nil, nil
		},
		Requires: []*analysis.Analyzer{inspect.Analyzer},
		Flags:    *flagSet,
	}
}

func fullName(ident *ast.Ident, stack []ast.Node) string {
	for _, node := range stack {
		if fun, ok := node.(*ast.FuncDecl); ok {
			return fun.Name.Name + "." + ident.Name
		}
	}
	return ident.Name
}

func isStatementRightAfter(first nodeAndStack, second nodeAndStack) bool {
	containingStatement, firstIndex, secondIndex := findSharedContainingStatement(
		append(slices.Clone(first.stack), first.node),
		append(slices.Clone(second.stack), second.node),
	)

	if containingStatement == nil {
		return false
	}

	return secondIndex == firstIndex+1
}

func findSharedContainingStatement(first []ast.Node, second []ast.Node) (ast.Node, int, int) {
	for i := 0; i < min(len(first), len(second)); i++ {
		if i > 0 && first[i] != second[i] {
			for j := i - 1; j >= 0; j-- {
				if block, ok := first[j].(*ast.BlockStmt); ok {
					return block, stIndex(block.List, first[j+1]), stIndex(block.List, second[j+1])
				}

				if cse, ok := first[j].(*ast.CaseClause); ok {
					return cse, stIndex(cse.Body, first[j+1]), stIndex(cse.Body, second[j+1])
				}
			}
		}
	}

	return nil, -1, -1
}

func stIndex(statements []ast.Stmt, node ast.Node) int {
	return slices.IndexFunc(statements, func(current ast.Stmt) bool {
		return current == node
	})
}
