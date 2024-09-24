package zerologmarshalcheck

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	analyzer := Analyzer()
	analyzer.Flags.Set("log-sink-types", "disallowedmarshal.WithError,validmarshal.WithError")

	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, analyzer, "disallowedmarshal")
	analysistest.Run(t, testdata, analyzer, "validmarshal")
}
