package zerologmarshalcheck

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

func Analyzer() *analysis.Analyzer {
	flagSet := flag.NewFlagSet("zerologmarshalcheck", flag.ExitOnError)
	skipPkg := flagSet.String("skip-pkg", "", "package(s) to skip for linting")
	skipFiles := flagSet.String("skip-files", "", "patterns of files to skip for linting")
	logSinkTypesString := flagSet.String("log-sink-types", "*github.com/rs/zerolog.Event", "full paths of the types that are log sinks")

	return &analysis.Analyzer{
		Name: "zerologmarshalcheck",
		Doc:  "reports calls to `Err` that will cause infinite recursion in `MarshalZerologObject` methods",
		Run: func(pass *analysis.Pass) (any, error) {
			logSinkTypes := strings.Split(*logSinkTypesString, ",")

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

				case *ast.FuncDecl:
					if s.Name.Name != "MarshalZerologObject" {
						return false
					}

					return true

				case *ast.CallExpr:
					// Find the parent function and grab its parameter.
					var parentFunc *ast.FuncDecl
					for _, parent := range stack {
						if funcDecl, ok := parent.(*ast.FuncDecl); ok {
							parentFunc = funcDecl
							break
						}
					}

					if parentFunc == nil {
						return false
					}

					// Ensure the parent function has a receiver.
					if parentFunc.Recv == nil {
						return false
					}

					if len(parentFunc.Recv.List) != 1 {
						return false
					}

					if len(parentFunc.Recv.List[0].Names) != 1 {
						return false
					}

					receiverName := parentFunc.Recv.List[0].Names[0].Name

					// Ensure the parent function has a single parameter.
					funcParams := parentFunc.Type.Params.List
					if len(funcParams) != 1 {
						return false
					}

					paramType := pass.TypesInfo.TypeOf(funcParams[0].Type)
					if paramType == nil {
						return false
					}

					// Ensure the single parameter is a log sink.
					if !slices.Contains(logSinkTypes, paramType.String()) {
						return false
					}

					paramName := funcParams[0].Names[0].Name

					selectorExpr, ok := s.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}

					if selectorExpr.Sel.Name != "Err" {
						return true
					}

					identExpr, ok := selectorExpr.X.(*ast.Ident)
					if !ok {
						return true
					}

					if identExpr.Name != paramName {
						return true
					}

					// Ensure the parameter for the Err call is not the parent object itself. If so,
					// this is a recursive call.
					if len(s.Args) != 1 {
						return true
					}

					argIdent, ok := s.Args[0].(*ast.Ident)
					if !ok {
						return true
					}

					if argIdent.Name == receiverName {
						pass.Reportf(s.Pos(), "`Err(%s)` will cause infinite recursion; change to `Err(%s.error)`", receiverName, receiverName)
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
