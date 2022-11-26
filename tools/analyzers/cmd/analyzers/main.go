package main

import (
	"github.com/authzed/spicedb/tools/analyzers/closeafterusagecheck"
	"github.com/authzed/spicedb/tools/analyzers/exprstatementcheck"
	"github.com/authzed/spicedb/tools/analyzers/nilvaluecheck"
	"golang.org/x/tools/go/analysis/multichecker"
)

func main() {
	multichecker.Main(
		nilvaluecheck.Analyzer(),
		exprstatementcheck.Analyzer(),
		closeafterusagecheck.Analyzer(),
	)
}
