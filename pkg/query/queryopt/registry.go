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
	"reachability-pruning",
}

// RequestParams holds request-specific values available to optimizations.
// Static (schema-level) optimizations ignore these; request-parameterized
// optimizations (e.g. reachability pruning) use them to tailor their behavior.
type RequestParams struct {
	SubjectType     string
	SubjectRelation string
}

// OutlineTransform is a function that transforms an entire outline tree.
// Each optimizer produces one of these, and they are applied sequentially.
type OutlineTransform func(query.Outline) query.Outline

// Optimizer describes a single named outline optimization.
type Optimizer struct {
	Name        string
	Description string
	// NewTransform creates a whole-tree transformation for this optimizer,
	// optionally using request-specific parameters. Each transform typically
	// calls query.MutateOutline internally with its own mutations.
	NewTransform func(RequestParams) OutlineTransform
	// Priority controls the order in which optimizations are applied.
	// Higher values run first.
	Priority int
}

// ApplyOptimizations looks up each optimizer by name, sorts them by descending
// Priority (higher priority runs first), and applies their mutations to the
// outline via query.MutateOutline. Nodes that survive mutation unchanged keep
// their existing IDs. Newly synthesized nodes (ID==0) receive fresh IDs via
// query.FillMissingNodeIDs. The returned CanonicalOutline is ready to compile.
func ApplyOptimizations(co query.CanonicalOutline, names []string, requestParams RequestParams) (query.CanonicalOutline, error) {
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

	// Apply each optimizer's transform sequentially in priority order.
	mutated := co.Root
	for _, opt := range opts {
		transform := opt.NewTransform(requestParams)
		mutated = transform(mutated)
	}

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
