package exprstatementcheck

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	analyzer := Analyzer()
	analyzer.Flags.Set("disallowed-expr-statement-types", "*missingsend.Event:missing Send or Msg for zerolog log statement")

	// NOTE: importing of external packages is currently broken for analysistest,
	// so we cannot check zerolog itself.
	// See: https://github.com/golang/go/issues/46041

	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, analyzer, "missingsend")
}
