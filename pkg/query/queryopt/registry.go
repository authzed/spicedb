package queryopt

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/authzed/spicedb/pkg/query"
)

var optimizationRegistry = make(map[string]Optimizer)

func MustRegisterOptimization(opt Optimizer) {
	if _, ok := optimizationRegistry[opt.Name]; ok {
		panic("queryopt: `" + opt.Name + "` registered twice at initialization")
	}
	optimizationRegistry[opt.Name] = opt
}

func GetOptimization(name string) (Optimizer, error) {
	v, ok := optimizationRegistry[name]
	if !ok {
		return Optimizer{}, fmt.Errorf("queryopt: no optimizer named `%s`", name)
	}
	return v, nil
}

// StandardOptimzations is the default set of optimization names applied when
// no custom selection is required.
var StandardOptimzations = []string{
	"simple-caveat-pushdown",
}

// Optimizer describes a single named outline optimization.
type Optimizer struct {
	Name        string
	Description string
	Mutation    query.OutlineMutation
	// Priority controls the order in which optimizations are applied.
	// Higher values run first.
	Priority int
}

// ApplyOptimizations looks up each optimizer by name, sorts them by descending
// Priority (higher priority runs first), and applies their mutations to the
// outline via query.MutateOutline. Nodes that survive mutation unchanged keep
// their existing IDs. Newly synthesized nodes (ID==0) receive fresh IDs via
// query.FillMissingNodeIDs. The returned CanonicalOutline is ready to compile.
func ApplyOptimizations(co query.CanonicalOutline, names []string) (query.CanonicalOutline, error) {
	// Look up each named optimizer.
	opts := make([]Optimizer, 0, len(names))
	for _, name := range names {
		opt, err := GetOptimization(name)
		if err != nil {
			return query.CanonicalOutline{}, err
		}
		opts = append(opts, opt)
	}

	// Sort by descending priority so higher-priority mutations run first.
	slices.SortFunc(opts, func(a, b Optimizer) int {
		return cmp.Compare(b.Priority, a.Priority)
	})

	// Collect mutations in priority order.
	mutations := make([]query.OutlineMutation, len(opts))
	for i, opt := range opts {
		mutations[i] = opt.Mutation
	}

	// Apply all mutations bottom-up.
	mutated := query.MutateOutline(co.Root, mutations)

	// Extend the CanonicalKeys map with entries for any newly created nodes.
	// We copy first so the original CanonicalOutline's map is not mutated.
	extendedKeys := make(map[query.OutlineNodeID]query.CanonicalKey, len(co.CanonicalKeys))
	for id, key := range co.CanonicalKeys {
		extendedKeys[id] = key
	}
	filled := query.FillMissingNodeIDs(mutated, extendedKeys)

	return query.CanonicalOutline{
		Root:          filled,
		CanonicalKeys: extendedKeys,
		Hints:         co.Hints,
	}, nil
}
