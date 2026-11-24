package ctxkeycheck

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	analyzer := Analyzer()
	analyzer.Flags.Set("allowed-key-types", "thirdparty.ctxKey")

	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, analyzer, "badctxkey")
	analysistest.Run(t, testdata, analyzer, "goodctxkey")
}
