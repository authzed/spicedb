package queryopt

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/tuple"
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

// RequestParams holds request-specific values available to optimizations and
// optimizer selection. Static (schema-level) optimizations ignore these;
// request-parameterized optimizations (e.g. reachability pruning) use them
// to tailor their behavior. The selection function uses them to decide which
// optimizers to include.
type RequestParams struct {
	// Operation identifies which query operation is being planned.
	// Used by the selection function to determine which optimizers are safe.
	Operation query.Operation
	// SubjectType is the object type of the subject/filter.
	SubjectType string
	// SubjectRelation is the relation on the subject.
	// Set to "..." for ellipsis subjects (e.g. user:...), "" for bare subjects,
	// or a specific relation name (e.g. "viewer").
	SubjectRelation string
}

// OptimizersForRequest returns the list of optimizers to apply for the given
// request parameters. The selection logic lives here (in the queryopt package)
// rather than in the service layer, so that optimizer safety constraints are
// enforced centrally.
//
// alias-chain-collapse is included when safe:
//   - Always safe for Check (self-edge is a redundant fallback in alias chains)
//   - Safe for IterResources/IterSubjects when SubjectRelation is "..."
//     (ellipsis never matches any alias relation, so no self-edge is triggered)
//   - NOT safe for IterResources/IterSubjects when SubjectRelation is a
//     specific name, as it could match an inner alias but not the outer —
//     collapsing would change self-edge behavior.
func OptimizersForRequest(params RequestParams) []Optimizer {
	base := []Optimizer{
		optimizationRegistry["simple-caveat-pushdown"],
		optimizationRegistry["reachability-pruning"],
	}

	if params.Operation == query.OperationCheck ||
		params.SubjectRelation == tuple.Ellipsis {
		base = append(base, optimizationRegistry["alias-chain-collapse"])
	}

	return base
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

// ApplyOptimizations sorts the given optimizers by descending Priority
// (higher priority runs first), and applies their transforms to the outline.
// Nodes that survive mutation unchanged keep their existing IDs. Newly
// synthesized nodes (ID==0) receive fresh IDs via query.FillMissingNodeIDs.
// The returned CanonicalOutline is ready to compile.
func ApplyOptimizations(co query.CanonicalOutline, opts []Optimizer, params RequestParams) (query.CanonicalOutline, error) {
	// Sort by descending priority so higher-priority mutations run first.
	sorted := slices.SortedFunc(slices.Values(opts), func(a, b Optimizer) int {
		return cmp.Compare(b.Priority, a.Priority)
	})

	// Apply each optimizer's transform sequentially in priority order.
	mutated := co.Root
	for _, opt := range sorted {
		transform := opt.NewTransform(params)
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
