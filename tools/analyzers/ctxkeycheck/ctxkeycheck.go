package ctxkeycheck

import (
	"flag"
	"fmt"
	"go/ast"
	"go/types"
	"regexp"
	"strings"

	"github.com/samber/lo"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
	"golang.org/x/tools/go/types/typeutil"
)

func Analyzer() *analysis.Analyzer {
	flagSet := flag.NewFlagSet("ctxkeycheck", flag.ExitOnError)
	skipPkg := flagSet.String("skip-pkg", "", "package(s) to skip for linting")
	skipFiles := flagSet.String("skip-files", "", "patterns of files to skip for linting")
	allowedKeyTypes := flagSet.String("allowed-key-types", "", "full type path(s) of context key types that may be used with context.WithValue, e.g. keys owned by another module")

	return &analysis.Analyzer{
		Name: "ctxkeycheck",
		Doc:  "reports calls to `context.WithValue`, which should be replaced with a typed key from github.com/authzed/ctxkey",
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

			allowedKeys := make(map[string]struct{})
			if len(*allowedKeyTypes) > 0 {
				for _, allowed := range strings.Split(*allowedKeyTypes, ",") {
					allowedKeys[strings.TrimSpace(allowed)] = struct{}{}
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
					if !push {
						return true
					}

					// Resolve the callee through type information, so that a renamed
					// import of `context` is still caught and a local `context`
					// identifier is not.
					fn := typeutil.StaticCallee(pass.TypesInfo, s)
					if fn != nil && fn.FullName() == "context.WithValue" && len(s.Args) == 3 {
						// Keys owned by another module have to be set the way that module reads them.
						if keyType := pass.TypesInfo.TypeOf(s.Args[1]); keyType != nil {
							if _, ok := allowedKeys[types.TypeString(keyType, nil)]; ok {
								return true
							}
						}

						pass.Reportf(s.Pos(), "use a typed key from github.com/authzed/ctxkey instead of context.WithValue")
					}

					// Keep descending: the call's arguments can contain further calls.
					return true

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
