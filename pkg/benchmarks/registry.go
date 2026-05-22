package benchmarks

import (
	"context"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// Tag categorizes benchmarks for filtering.
type Tag string

const (
	Arrows    Tag = "arrows"
	Recursion Tag = "recursion"
)

// Benchmark is a named scenario that can write its schema and data to a datastore
// and return sets of valid queries with expected results.
type Benchmark struct {
	Name  string
	Tags  []Tag
	Setup func(ctx context.Context, ds datastore.Datastore) (*QuerySets, error)
}

var benchmarkRegistry = make(map[string]Benchmark)

func registerBenchmark(b Benchmark) {
	if _, ok := benchmarkRegistry[b.Name]; ok {
		spiceerrors.MustPanicf("doubly registered benchmark: %s", b.Name)
	}
	benchmarkRegistry[b.Name] = b
}

// All returns a copy of every registered benchmark.
func All() []Benchmark {
	out := make([]Benchmark, 0, len(benchmarkRegistry))
	for _, b := range benchmarkRegistry {
		out = append(out, b)
	}
	return out
}

// Get returns a single registered benchmark by name.
func Get(name string) (Benchmark, bool) {
	b, ok := benchmarkRegistry[name]
	return b, ok
}

// WithTag returns all benchmarks that carry the given tag.
func WithTag(tag Tag) []Benchmark {
	var out []Benchmark
	for _, b := range benchmarkRegistry {
		for _, t := range b.Tags {
			if t == tag {
				out = append(out, b)
				break
			}
		}
	}
	return out
}
