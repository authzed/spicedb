package checkbaseline

import (
	"context"
	"fmt"
	bm "github.com/authzed/spicedb/pkg/benchmarks"
	"github.com/authzed/spicedb/pkg/datastore"
	"sort"
)

func RegistryDatasets() []Dataset {
	var out []Dataset
	for _, b := range bm.All() {
		out = append(out, Dataset{ID: "registry/" + b.Name, Family: "registry", Source: "pkg/benchmarks/" + b.Name, Setup: func(ctx context.Context, ds datastore.Datastore) ([]Case, error) {
			qs, err := b.Setup(ctx, ds)
			if err != nil {
				return nil, err
			}
			var cases []Case
			depth := qs.MaxRecursionDepth
			if depth == 0 {
				depth = 50
			}
			for i, q := range qs.Checks {
				cases = append(cases, Case{ID: fmt.Sprint(i), Query: q, Expected: Decision{Outcome: Allow}, ClassicDepth: uint32(depth), QPDepth: depth})
			}
			return cases, nil
		}})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
