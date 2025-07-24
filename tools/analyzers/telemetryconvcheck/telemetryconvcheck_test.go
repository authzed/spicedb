package telemetryconvcheck

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	analyzer := Analyzer()

	testdata := analysistest.TestData()

	// Test original functionality
	analysistest.Run(t, testdata, analyzer, "validtelemetry")
	analysistest.Run(t, testdata, analyzer, "invalidtelemetry")

	// Test prefix check
	analysistest.Run(t, testdata, analyzer, "validprefixes")
	analysistest.Run(t, testdata, analyzer, "invalidprefixes")
}
