package main

import (
	"golang.org/x/tools/go/analysis/multichecker"

	"github.com/authzed/spicedb/tools/analyzers/closeafterusagecheck"
	"github.com/authzed/spicedb/tools/analyzers/exprstatementcheck"
	"github.com/authzed/spicedb/tools/analyzers/iferrafterrowclosecheck"
	"github.com/authzed/spicedb/tools/analyzers/lendowncastcheck"
	"github.com/authzed/spicedb/tools/analyzers/mutexcheck"
	"github.com/authzed/spicedb/tools/analyzers/nilvaluecheck"
	"github.com/authzed/spicedb/tools/analyzers/paniccheck"
	"github.com/authzed/spicedb/tools/analyzers/protomarshalcheck"
	"github.com/authzed/spicedb/tools/analyzers/telemetryconvcheck"
	"github.com/authzed/spicedb/tools/analyzers/zerologmarshalcheck"
)

func main() {
	multichecker.Main(
		mutexcheck.Analyzer(),
		nilvaluecheck.Analyzer(),
		exprstatementcheck.Analyzer(),
		closeafterusagecheck.Analyzer(),
		iferrafterrowclosecheck.Analyzer(),
		paniccheck.Analyzer(),
		lendowncastcheck.Analyzer(),
		protomarshalcheck.Analyzer(),
		zerologmarshalcheck.Analyzer(),
		telemetryconvcheck.Analyzer(),
	)
}
