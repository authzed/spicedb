package iferrafterrowclosecheck

import (
	"flag"
	"go/ast"
	"strings"

	"github.com/samber/lo"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

func Analyzer() *analysis.Analyzer {
	flagSet := flag.NewFlagSet("iferrafterrowclosecheck", flag.ExitOnError)
	skip := flagSet.String("skip-pkg", "", "package(s) to skip for linting")

	return &analysis.Analyzer{
		Name: "iferrafterrowclosecheck",
		Doc:  "reports any rows.Close() calls not immediately followed by error checks",
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
				(*ast.ExprStmt)(nil),
				(*ast.DeferStmt)(nil),
			}

			inspect.WithStack(nodeFilter, func(n ast.Node, push bool, stack []ast.Node) bool {
				switch s := n.(type) {
				case *ast.DeferStmt:
					// Skip defer statements - we only care about non-deferred rows.Close()
					return false

				case *ast.ExprStmt:
					// Check if this expression statement is a rows.Close() call
					if callExpr, ok := s.X.(*ast.CallExpr); ok {
						if selector, ok := callExpr.Fun.(*ast.SelectorExpr); ok {
							if selector.Sel.Name == "Close" {
								if ident, ok := selector.X.(*ast.Ident); ok {
									// Check if this looks like a rows variable
									if strings.HasSuffix(strings.ToLower(ident.Name), "rows") || ident.Name == "rows" {
										// Found a rows.Close() call, now check if it's followed by error check
										if !hasImmediateErrorCheck(s, ident.Name, stack) {
											pass.Reportf(s.Pos(), "rows.Close() call should be immediately followed by 'if rows.Err() != nil' check")
										}
									}
								}
							}
						}
					}
					return false
				}

				return true
			})

			return nil, nil
		},
		Requires: []*analysis.Analyzer{inspect.Analyzer},
		Flags:    *flagSet,
	}
}

func hasImmediateErrorCheck(closeStmt *ast.ExprStmt, rowsName string, stack []ast.Node) bool {
	// Find the most immediate containing block statement (search backwards in stack)
	var containingBlock *ast.BlockStmt
	for i := len(stack) - 1; i >= 0; i-- {
		if block, ok := stack[i].(*ast.BlockStmt); ok {
			containingBlock = block
			break
		}
	}

	if containingBlock == nil {
		return false
	}

	// Find the index of our closeStmt in the block
	closeStmtIndex := -1
	for i, stmt := range containingBlock.List {
		if stmt == closeStmt {
			closeStmtIndex = i
			break
		}
	}

	if closeStmtIndex == -1 || closeStmtIndex+1 >= len(containingBlock.List) {
		return false
	}

	// Check if the next statement is an if statement with rows.Err() != nil condition
	nextStmt := containingBlock.List[closeStmtIndex+1]
	if ifStmt, ok := nextStmt.(*ast.IfStmt); ok {
		if binExpr, ok := ifStmt.Cond.(*ast.BinaryExpr); ok {
			if binExpr.Op.String() == "!=" {
				if callExpr, ok := binExpr.X.(*ast.CallExpr); ok {
					if selector, ok := callExpr.Fun.(*ast.SelectorExpr); ok {
						if selector.Sel.Name == "Err" {
							if ident, ok := selector.X.(*ast.Ident); ok {
								if ident.Name == rowsName {
									if nilIdent, ok := binExpr.Y.(*ast.Ident); ok && nilIdent.Name == "nil" {
										return true
									}
								}
							}
						}
					}
				}
			}
		}
	}

	return false
}
