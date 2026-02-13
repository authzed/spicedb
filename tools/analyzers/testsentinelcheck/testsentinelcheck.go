package testsentinelcheck

import (
	"flag"
	"go/ast"
	"path/filepath"
	"strings"

	"github.com/samber/lo"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

func Analyzer() *analysis.Analyzer {
	flagSet := flag.NewFlagSet("testsentinelcheck", flag.ExitOnError)
	skipPkg := flagSet.String("skip-pkg", "", "package(s) to skip for linting")

	return &analysis.Analyzer{
		Name: "testsentinelcheck",
		Doc:  "reports usage of test-only sentinel values like NoSchemaHashForTesting outside of test files",
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

			inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

			nodeFilter := []ast.Node{
				(*ast.File)(nil),
				(*ast.Ident)(nil),
			}

			var currentFile string
			var isTestFile bool

			inspect.WithStack(nodeFilter, func(n ast.Node, push bool, stack []ast.Node) bool {
				switch s := n.(type) {
				case *ast.File:
					// Track the current file being analyzed
					currentFile = pass.Fset.Position(s.Package).Filename
					baseName := filepath.Base(currentFile)
					isTestFile = strings.HasSuffix(baseName, "_test.go")
					return true

				case *ast.Ident:
					// Skip if we're in a test file
					if isTestFile {
						return false
					}

					// Check if this identifier is NoSchemaHashForTesting
					if s.Name == "NoSchemaHashForTesting" {
						// Verify this is actually referencing the datastore constant
						// by checking if it's a selector expression or qualified identifier
						obj := pass.TypesInfo.ObjectOf(s)
						if obj != nil {
							// Skip if this is the definition of the constant itself
							if obj.Pos() == s.Pos() {
								return false
							}

							if obj.Pkg() != nil {
								pkgPath := obj.Pkg().Path()
								// Check if it's from the datastore package
								if strings.HasSuffix(pkgPath, "github.com/authzed/spicedb/pkg/datastore") {
									pass.Reportf(s.Pos(), "NoSchemaHashForTesting should only be used in test files (file: %s)", filepath.Base(currentFile))
								}
							}
						}
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
