# Query outline optimizations

Optimizations rewrite a canonical outline before compilation. Each pass owns its
implementation, tests and documentation:

- [Caveat pushdown](caveatpushdown/README.md), priority 20.
- [Set simplification](setsimplification/README.md), priority 10.
- [Reachability pruning](reachabilitypruning/README.md), priority 0.
- [Alias chain collapse](aliaschaincollapse/README.md), priority 0.
- [Dispatch wrapping](../../../internal/dispatch/queryopt/dispatchwrap/README.md), applied separately by dispatch.

## Defining a pass

Put a new pass in its own subpackage with `New() optimization.Optimizer` (from
`queryopt/optimization`). Provide a stable Name, Description, Priority and
NewTransform factory. Keep mutations private; MutateOutline walks bottom-up.
Return a node unchanged when it is inapplicable. Preserve existing node IDs
where their identities remain valid; new identities use ID zero so the runner
can assign fresh IDs and keys.

Leaf packages import `query` and `queryopt/optimization`, not this parent
catalog. This package explicitly registers the structural and decision descriptors in
init. Add a new descriptor there and add its request selection rules to
OptimizersForRequest. Registration alone does not enable a pass. Duplicate
names panic; GetOptimization reports unknown names. Existing Optimizer,
RequestParams and OutlineTransform names are aliases for compatibility.

NewPipeline orders explicit After edges and hint producers before consumers.
Independent passes use descending Priority then name. Declare RequiredSignals /
OptionalSignals, RequiredHints / OptionalHints and ProducesHints. A Run callback
receives an optimization.Context; NewTransform remains available for structural
passes. Undeclared reads/emissions and malformed declarations are errors. Missing
required evidence skips that pass. Consumers must accept or justify declining
hints. Structural rewrites invalidate stale targets unless explicitly remapped.

## Documentation and testing contract

Each README explains the transformation, actual inputs/signals, applicability,
correctness restrictions, ordering, expected execution/planning costs, and tests.
Describe expected effects honestly; support speedup claims with measurements.

Keep deterministic rewrite tests local. Add an external-package
TestSemanticEquivalence using the [shared Rapid harness](../../../internal/queryopttest/README.md)
and exactly one pass. Broad schemas alone may never hit a rewrite: provide
named targeted generators with RequireChange and appropriate applicability.
Keep catalog selection/registration tests here. External test packages avoid
an import cycle when the harness imports the catalog to apply a descriptor.

## Signal and hint phase

RequestParams carries operation and subject type/relation. Standard structural
passes consume no runtime observations. After these passes and dispatch wrapping,
DecisionOptimizers uses exact result sizes with aged historical fallback to emit
typed arrow-direction recommendations. The branch-order consumer accepts validated
union/intersection permutations; no statistical branch predictor is installed yet.
See [signals](../signals/README.md) and [hints](../planhint/README.md).
Legacy CountAdvisor remains available to callers; production QP execution uses the
shared signal owner and completion-aware collector.

```sh
go test ./pkg/query/queryopt/...
```

`NewPipeline(passes, registry)` validates signal declarations against an explicit
provider catalog; omitting the registry uses the default catalog. Unknown signal
names and hint kinds fail during construction, even if a callback never reads them.
