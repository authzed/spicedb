package main

import (
	"github.com/authzed/spicedb/tools/analyzers/nilvaluecheck"
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() {
	singlechecker.Main(nilvaluecheck.Analyzer())
}
