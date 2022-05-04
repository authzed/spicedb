package nilvaluecheck

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	analyzer := Analyzer()
	analyzer.Flags.Set("disallowed-nil-return-type-paths", "*nilreturn.someStruct")

	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, analyzer, "nilreturn")
}
