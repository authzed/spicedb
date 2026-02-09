package testsentinelcheck_test

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"

	"github.com/authzed/spicedb/tools/analyzers/testsentinelcheck"
)

func TestAnalyzer(t *testing.T) {
	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, testsentinelcheck.Analyzer(), "testsentinel")
}
