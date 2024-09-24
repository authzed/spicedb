package lendowncastcheck

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

var disallowedDowncastTypes = map[string]bool{
	"int8":    true,
	"int16":   true,
	"int32":   true,
	"int64":   true,
	"uint":    true,
	"uint8":   true,
	"uint16":  true,
	"uint32":  true,
	"float32": true,
	"float64": true,
}

func Analyzer() *analysis.Analyzer {
	flagSet := flag.NewFlagSet("lendowncastcheck", flag.ExitOnError)
	skipPkg := flagSet.String("skip-pkg", "", "package(s) to skip for linting")
	skipFiles := flagSet.String("skip-files", "", "patterns of files to skip for linting")

	return &analysis.Analyzer{
		Name: "lendowncastcheck",
		Doc:  "reports downcasting of len() calls",
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

					if _, ok := disallowedDowncastTypes[identExpr.Name]; !ok {
						return false
					}

					if len(s.Args) != 1 {
						return false
					}

					childExpr, ok := s.Args[0].(*ast.CallExpr)
					if !ok {
						return false
					}

					childIdentExpr, ok := childExpr.Fun.(*ast.Ident)
					if !ok {
						return false
					}

					if childIdentExpr.Name != "len" {
						return false
					}

					pass.Reportf(s.Pos(), "In package %s: found downcast of `len` call to %s", pass.Pkg.Path(), identExpr.Name)
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
