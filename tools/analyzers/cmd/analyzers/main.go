package main

import (
	"github.com/authzed/spicedb/tools/analyzers/closeafterusagecheck"
	"github.com/authzed/spicedb/tools/analyzers/exprstatementcheck"
	"github.com/authzed/spicedb/tools/analyzers/lendowncastcheck"
	"github.com/authzed/spicedb/tools/analyzers/nilvaluecheck"
	"github.com/authzed/spicedb/tools/analyzers/paniccheck"
	"github.com/authzed/spicedb/tools/analyzers/protomarshalcheck"
	"github.com/authzed/spicedb/tools/analyzers/zerologmarshalcheck"
	"golang.org/x/tools/go/analysis/multichecker"
)

func main() {
	multichecker.Main(
		nilvaluecheck.Analyzer(),
		exprstatementcheck.Analyzer(),
		closeafterusagecheck.Analyzer(),
		paniccheck.Analyzer(),
		lendowncastcheck.Analyzer(),
		protomarshalcheck.Analyzer(),
		zerologmarshalcheck.Analyzer(),
	)
}
