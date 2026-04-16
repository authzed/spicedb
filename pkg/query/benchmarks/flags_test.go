package benchmarks

import (
	"flag"

	bm "github.com/authzed/spicedb/pkg/benchmarks"
)

var (
	includeDelay = flag.Bool("bench-delay", false, "include delay variants in query planner benchmarks")
	includePlain = flag.Bool("bench-plain", false, "include plain (non-advised) benchmark variants")
)

// directBenchmarkNames is the curated subset of registry benchmarks run
// directly against the query planner (without gRPC). The full registry is
// exercised through the integration benchmarks in
// internal/services/integrationtesting/.
var directBenchmarkNames = []string{
	"DeepArrow",
	"WideArrow",
	"DoubleWideArrow",
	"ShareWith",
}

// directBenchmarks returns the registry entries for directBenchmarkNames.
func directBenchmarks() []bm.Benchmark {
	out := make([]bm.Benchmark, 0, len(directBenchmarkNames))
	for _, name := range directBenchmarkNames {
		b, ok := bm.Get(name)
		if ok {
			out = append(out, b)
		}
	}
	return out
}
