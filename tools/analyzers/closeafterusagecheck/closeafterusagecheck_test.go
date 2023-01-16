package closeafterusagecheck

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	analyzer := Analyzer()
	analyzer.Flags.Set("must-be-closed-after-usage-types", "*notimmediatelyclosed.SomeIterator")

	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, analyzer, "notimmediatelyclosed")
}
