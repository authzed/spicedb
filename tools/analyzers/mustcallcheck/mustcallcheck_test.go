package mustcallcheck

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	analyzer := Analyzer()

	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, analyzer, "badmustcalls")
	analysistest.Run(t, testdata, analyzer, "goodmustcalls")
}
