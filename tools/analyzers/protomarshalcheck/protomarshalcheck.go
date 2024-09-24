package protomarshalcheck

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
	flagSet := flag.NewFlagSet("protomarshalcheck", flag.ExitOnError)
	skipPkg := flagSet.String("skip-pkg", "", "package(s) to skip for linting")
	skipFiles := flagSet.String("skip-files", "", "patterns of files to skip for linting")

	return &analysis.Analyzer{
		Name: "protomarshalcheck",
		Doc:  "reports calls to `proto.Marshal` and `proto.Unmarshal`, which should be replaced with their VT counterparts",
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
					selectorExpr, ok := s.Fun.(*ast.SelectorExpr)
					if !ok {
						return false
					}

					expression, ok := selectorExpr.X.(*ast.Ident)
					if !ok {
						return false
					}
					if expression.Name != "proto" {
						return false
					}

					if selectorExpr.Sel.Name == "Unmarshal" {
						pass.Reportf(s.Pos(), "`use someMessage.UnmarshalVT instead`")
					}

					if selectorExpr.Sel.Name == "Marshal" {
						pass.Reportf(s.Pos(), "`use someStruct.MarshalVT instead`")
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
