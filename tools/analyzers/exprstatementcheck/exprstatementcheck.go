package exprstatementcheck

import (
	"flag"
	"fmt"
	"go/ast"
	"slices"
	"strings"

	"github.com/samber/lo"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

type disallowedExprStatementConfig struct {
	fullTypePath       string
	errorMessage       string
	ignoredMethodNames []string
}

func Analyzer() *analysis.Analyzer {
	flagSet := flag.NewFlagSet("exprstatementcheck", flag.ExitOnError)
	disallowedExprTypes := flagSet.String(
		"disallowed-expr-statement-types",
		"",
		`semicolon delimited configuration of the types that should be disallowed as expression statements. 
			Formats:
				- "full type path:error message"
				- "full type path:MethodNameUnderWhichToIgnore:error message"

			Example: "*github.com/rs/zerolog.Event:MarshalZerologObject:error message here"
		`,
	)
	skip := flagSet.String("skip-pkg", "", "package(s) to skip for linting")

	return &analysis.Analyzer{
		Name: "exprstatementcheck",
		Doc:  "reports expression statements that have the disallowed value types",
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

			entries := strings.Split(*disallowedExprTypes, ";")
			typePathsAndMessages := map[string]disallowedExprStatementConfig{}
			for _, entry := range entries {
				if len(entry) == 0 {
					continue
				}

				parts := strings.Split(entry, ":")
				if len(parts) == 2 {
					typePathsAndMessages[parts[0]] = disallowedExprStatementConfig{
						fullTypePath: parts[0],
						errorMessage: parts[1],
					}
				} else if len(parts) == 3 {
					typePathsAndMessages[parts[0]] = disallowedExprStatementConfig{
						fullTypePath:       parts[0],
						ignoredMethodNames: strings.Split(parts[1], ","),
						errorMessage:       parts[2],
					}
				} else {
					return nil, fmt.Errorf("invalid value for disallowed-expr-statement-types: `%s`", entry)
				}
			}

			inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

			nodeFilter := []ast.Node{
				(*ast.ExprStmt)(nil),
			}

			inspect.WithStack(nodeFilter, func(n ast.Node, push bool, stack []ast.Node) bool {
			switchStatement:
				switch s := n.(type) {
				case *ast.ExprStmt:
					foundType := pass.TypesInfo.TypeOf(s.X)
					if foundType == nil {
						return false
					}

					if config, ok := typePathsAndMessages[foundType.String()]; ok {
						if len(config.ignoredMethodNames) > 0 {
							for _, parent := range stack {
								if funcDecl, ok := parent.(*ast.FuncDecl); ok {
									if slices.Contains(config.ignoredMethodNames, funcDecl.Name.Name) {
										break switchStatement
									}
								}
							}
						}

						pass.Reportf(s.Pos(), "In package %s: %s", pass.Pkg.Path(), config.errorMessage)
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
