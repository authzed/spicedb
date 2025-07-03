package mutexcheck

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	analyzer := Analyzer()

	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, analyzer, "goodmutexes")
	analysistest.Run(t, testdata, analyzer, "badmutexes")
}
