package mutexcheck

import (
	"flag"
	"fmt"
	"go/ast"
	"go/token"
	"regexp"
	"strings"

	"github.com/samber/lo"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

var guardedByRegex = regexp.MustCompile(`GUARDED_BY\((?P<mutex>.*?)\)`)

func Analyzer() *analysis.Analyzer {
	flagSet := flag.NewFlagSet("mutexcheck", flag.ExitOnError)
	skipPkg := flagSet.String("skip-pkg", "", "package(s) to skip for linting")
	skipFiles := flagSet.String("skip-files", "", "patterns of files to skip for linting")

	return &analysis.Analyzer{
		Name: "mutexcheck",
		Doc:  "reports mutexes that aren't marked as used by a comment GUARDED_BY",
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
				(*ast.StructType)(nil),
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

				case *ast.StructType:
					if s.Fields == nil {
						return false
					}

					declaredMutexes := make(map[string]token.Pos)
					referencesPerMutexes := make(map[string][]token.Pos)

					for _, field := range s.Fields.List {
						fieldNames, isMutex := isMutexType(field)

						scan := getCommentsToAnalyze(field)

						if isMutex {
							for _, mutexFieldName := range fieldNames {
								declaredMutexes[mutexFieldName] = field.Pos()

								for _, ss := range scan {
									if strings.Contains(ss.text, "GUARDED_BY") {
										pass.Reportf(ss.pos, "mutex shouldn't be guarded by a mutex")
									}
								}
							}
						} else {
							for _, ss := range scan {
								matches := guardedByRegex.FindStringSubmatch(ss.text)
								if matches != nil {
									guardedBy := matches[guardedByRegex.SubexpIndex("mutex")]
									_, ok := referencesPerMutexes[guardedBy]
									if !ok {
										referencesPerMutexes[guardedBy] = []token.Pos{field.End()}
									} else {
										referencesPerMutexes[guardedBy] = append(referencesPerMutexes[guardedBy], s.End())
									}
								}
							}
						}
					}

					for declaredMutex, declaredMutexPos := range declaredMutexes {
						val, refs := referencesPerMutexes[declaredMutex]
						if !refs || len(val) == 0 {
							pass.Reportf(declaredMutexPos, "mutex isn't guarding anything, please annotate one of the fields with GUARDED_BY")
						}
					}

					for refMutex, refMutexPositions := range referencesPerMutexes {
						_, ok := declaredMutexes[refMutex]
						if !ok {
							for _, position := range refMutexPositions {
								pass.Reportf(position, "field is guarded by an unknown mutex")
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

type comment struct {
	text string
	pos  token.Pos
}

// getCommentsToAnalyze returns a list of possible texts that can contain GUARDED_BY.
func getCommentsToAnalyze(field *ast.Field) []comment {
	scan := make([]comment, 0)

	if field.Doc != nil {
		scan = append(scan, comment{field.Doc.Text(), field.Doc.Pos()})
	}

	if field.Comment != nil {
		for _, cc := range field.Comment.List {
			scan = append(scan, comment{cc.Text, cc.Pos()})
		}
	}
	return scan
}

// isMutexType returns true if a field's type is a mutex and if so,
// returns a list of the variable names.
// For example if the field is "mu1, mu2 sync.RWMutex" it returns "mu1" and "mu2"
func isMutexType(field *ast.Field) ([]string, bool) {
	var se *ast.SelectorExpr

	switch t := field.Type.(type) {
	case *ast.StarExpr:
		// it's a pointer
		innerSelector, ok := t.X.(*ast.SelectorExpr)
		if !ok {
			return nil, false
		}
		se = innerSelector
	case *ast.SelectorExpr:
		se = t
	default:
		return nil, false
	}

	ident, ok := se.X.(*ast.Ident)
	if !ok || ident.Name != "sync" {
		return nil, false
	}
	if se.Sel.Name != "Mutex" && se.Sel.Name != "RWMutex" {
		return nil, false
	}

	var fieldNames []string
	if len(field.Names) == 0 {
		// it's an anonymous field
		fieldNames = append(fieldNames, se.Sel.Name)
	}

	for _, m := range field.Names {
		fieldNames = append(fieldNames, m.Name)
	}

	return fieldNames, true
}
