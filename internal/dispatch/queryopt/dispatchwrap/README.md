# Dispatch alias wrapping

Registered name: `dispatch-wrap-alias`. Factory:
`New(iteratorType query.IteratorType) optimization.Optimizer`.
The dispatch package supplies its real DispatchIteratorType and registers the pass.

## Transformation

`Alias(view)(child)` becomes `Dispatch(Alias(view)(child))` for eligible aliases.
The wrapper is a local passthrough iterator marking a potential RPC boundary.

## Inputs and signals

Alias shape and recursive iterator/sentinel identities drive this pass. It uses
neither request parameters nor observed execution statistics. The supplied
iterator type avoids a dependency on the parent dispatch package.

## Correctness and applicability

An alias with an unmatched recursive sentinel stays unwrapped: an RPC could
separate the sentinel from its collection context. A matching recursive
iterator inside the subtree makes the static boundary eligible. Runtime
in-progress dispatch-key cycle checks remain the executor's responsibility.
New wrappers receive IDs and canonical keys through ApplyOptimizations.

## Ordering and expected performance

Applied separately after standard queryopt passes and before advising. Marking
boundaries during planning avoids rediscovering them during execution. Walking
alias subtrees adds planning cost; RPC/caching benefits require dispatch-layer
measurements. Reapplying this pass is not an idempotent operation.

## Tests

Colocated unit tests cover eligible aliases, matching/unmatched recursion and
new node identities. A parent integration test exercises ApplyDispatchWrap and
compilation to the real DispatchIterator. Shared properties run with that real
iterator in local mode over arbitrary generated schemas, nested aliases and
recursive boundaries. RPC behavior remains covered by dispatch tests.

```sh
go test ./internal/dispatch ./internal/dispatch/queryopt/dispatchwrap
go test ./internal/dispatch/queryopt/dispatchwrap -run TestSemanticEquivalence -rapid.checks=1000
```

Use Rapid's emitted seed/failure-file command for replay. See the
[shared harness](../../../queryopttest/README.md).
