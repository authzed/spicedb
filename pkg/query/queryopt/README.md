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
catalog. This package explicitly registers the four standard descriptors in
init. Add a new descriptor there and add its request selection rules to
OptimizersForRequest. Registration alone does not enable a pass. Duplicate
names panic; GetOptimization reports unknown names. Existing Optimizer,
RequestParams and OutlineTransform names are aliases for compatibility.

ApplyOptimizations orders descriptors by descending Priority and runs each
whole-tree transform once. Equal priorities carry no additional ordering
contract. It copies/extends canonical keys, fills missing IDs, and retains hints.

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

## Existing signals and advisor stage

RequestParams carries operation and subject type/relation. Each rewrite may
also inspect schema-derived outline structure. These standard passes consume
no runtime observations. After standard rewrites and dispatch wrapping, the
service separately applies CountAdvisor using accumulated call/result counts
to choose arrow direction, then compiles and executes the plan.

The common signal interface and any advisor-policy extraction are deferred.
Checkbaseline leaves optional rewrites/advising disabled by default; this
organization change does not change the benchmark's execution controls.

```sh
go test ./pkg/query/queryopt/...
```
