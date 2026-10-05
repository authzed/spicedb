package queryopt

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt/aliaschaincollapse"
	"github.com/authzed/spicedb/pkg/query/queryopt/applybranchorder"
	"github.com/authzed/spicedb/pkg/query/queryopt/arrowdirection"
	"github.com/authzed/spicedb/pkg/query/queryopt/caveatpushdown"
	"github.com/authzed/spicedb/pkg/query/queryopt/optimization"
	"github.com/authzed/spicedb/pkg/query/queryopt/reachabilitypruning"
	"github.com/authzed/spicedb/pkg/query/queryopt/setsimplification"
	"github.com/authzed/spicedb/pkg/tuple"
)

func init() {
	MustRegisterOptimization(caveatpushdown.New())
	MustRegisterOptimization(setsimplification.New())
	MustRegisterOptimization(reachabilitypruning.New())
	MustRegisterOptimization(aliaschaincollapse.New())
	MustRegisterOptimization(arrowdirection.New())
	MustRegisterOptimization(applybranchorder.New())
}

var optimizationRegistry = make(map[string]Optimizer)

// MustRegisterOptimization registers an optimization by name, panicking on duplicate registration.
func MustRegisterOptimization(opt Optimizer) {
	if _, ok := optimizationRegistry[opt.Name]; ok {
		panic("queryopt: `" + opt.Name + "` registered twice at initialization")
	}

	optimizationRegistry[opt.Name] = opt
}

// GetOptimization returns the registered optimization with the supplied name.
func GetOptimization(name string) (Optimizer, error) {
	v, ok := optimizationRegistry[name]
	if !ok {
		return Optimizer{}, fmt.Errorf("queryopt: no optimizer named `%s`", name)
	}

	return v, nil
}

// RequestParams contains request-specific optimization inputs.
type RequestParams = optimization.RequestParams

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
		optimizationRegistry["set-simplification"],
		optimizationRegistry["reachability-pruning"],
	}

	if params.Operation == query.OperationCheck ||
		params.SubjectRelation == tuple.Ellipsis {
		base = append(base, optimizationRegistry["alias-chain-collapse"])
	}

	return base
}

// OutlineTransform rewrites an entire outline.
type OutlineTransform = optimization.OutlineTransform

// Optimizer describes a named outline transformation.
type Optimizer = optimization.Optimizer

// ApplyOptimizations sorts the given optimizers by descending Priority
// (higher priority runs first), and applies their transforms to the outline.
// Nodes that survive mutation unchanged keep their existing IDs. Newly
// synthesized nodes (ID==0) receive fresh IDs via query.FillMissingNodeIDs.
// The returned CanonicalOutline is ready to compile.
func ApplyOptimizations(co query.CanonicalOutline, opts []Optimizer, params RequestParams) (query.CanonicalOutline, error) {
	p, err := NewPipeline(opts)
	if err != nil {
		return query.CanonicalOutline{}, err
	}

	return p.Run(co, optimization.NewContext(params, nil, nil))
}

// DecisionOptimizers returns the signal/hint phase after structural passes and dispatch wrapping.
func DecisionOptimizers() []Optimizer {
	return []Optimizer{optimizationRegistry["observed-arrow-direction"], optimizationRegistry["apply-branch-order"]}
}
