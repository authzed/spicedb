package main

import (
	"fmt"
	"os"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/multichecker"

	"github.com/authzed/spicedb/tools/analyzers/closeafterusagecheck"
	"github.com/authzed/spicedb/tools/analyzers/exprstatementcheck"
	"github.com/authzed/spicedb/tools/analyzers/iferrafterrowclosecheck"
	"github.com/authzed/spicedb/tools/analyzers/lendowncastcheck"
	"github.com/authzed/spicedb/tools/analyzers/mustcallcheck"
	"github.com/authzed/spicedb/tools/analyzers/mutexcheck"
	"github.com/authzed/spicedb/tools/analyzers/nilvaluecheck"
	"github.com/authzed/spicedb/tools/analyzers/paniccheck"
	"github.com/authzed/spicedb/tools/analyzers/protomarshalcheck"
	"github.com/authzed/spicedb/tools/analyzers/singleflightcheck"
	"github.com/authzed/spicedb/tools/analyzers/telemetryconvcheck"
	"github.com/authzed/spicedb/tools/analyzers/zerologmarshalcheck"
)

func main() {
	analyzers := []*analysis.Analyzer{
		mutexcheck.Analyzer(),
		nilvaluecheck.Analyzer(),
		exprstatementcheck.Analyzer(),
		closeafterusagecheck.Analyzer(),
		iferrafterrowclosecheck.Analyzer(),
		paniccheck.Analyzer(),
		mustcallcheck.Analyzer(),
		lendowncastcheck.Analyzer(),
		protomarshalcheck.Analyzer(),
		zerologmarshalcheck.Analyzer(),
		singleflightcheck.Analyzer(),
		telemetryconvcheck.Analyzer(),
	}

	// multichecker disables all analyzers unless explicitly enabled via -<name> flags, which means forgetting to
	// add the flag in lint.go silently skips the analyzer.
	// so here we enable all analyzers by default.
	enableFlags := make([]string, 0, len(analyzers))
	for _, a := range analyzers {
		enableFlags = append(enableFlags, fmt.Sprintf("-%s", a.Name))
	}
	os.Args = append(os.Args[:1], append(enableFlags, os.Args[1:]...)...)

	multichecker.Main(analyzers...)
}
