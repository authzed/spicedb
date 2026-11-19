# Reachability pruning

Registered name: `reachability-pruning`. Factory: `New() optimization.Optimizer`.

## Transformation

Replace datastore/self leaves that cannot produce the requested subject type
with null nodes, then propagate null through compound operations.
For example, querying `user` through `viewer: user | group` can remove a direct
`group` branch.

## Inputs and signals

Request SubjectType and SubjectRelation, datastore leaf types, self-node
definitions and arrow positions drive this pass. It uses no observed counts.
The type is a query target, not an intermediate arrow hop.

## Correctness and applicability

An empty SubjectType or a named SubjectRelation makes the pass a no-op.
Bare and ellipsis subjects may be pruned. All node IDs within arrow left
subtrees are protected because their subjects represent intermediate hops.
Null propagation follows each compound iterator's semantics.

## Ordering and expected performance

Priority 0 runs after caveat pushdown and set simplification. The pre-pass
collects protected arrow subtree IDs; the bottom-up pass prunes leaves and
propagates null. The expected benefit is avoiding work for impossible subject
types at the cost of two outline traversals.

## Tests

Unit tests cover pruning/propagation, empty and named inputs, and arrow immunity.
Shared properties cover generated schemas, multiple subject types, intermediate
hops and named-subject requests. Eligible targeted cases must change a plan;
named-subject fixtures verify preserved behavior without requiring a rewrite.

```sh
go test ./pkg/query/queryopt/reachabilitypruning
go test ./pkg/query/queryopt/reachabilitypruning -run TestSemanticEquivalence -rapid.checks=1000
```

Rapid reports a replay seed and minimized failure file. Use the emitted
`-run`, `-rapid.seed` or `-rapid.failfile` command to reproduce a failure.
The shared runner is documented in [queryopttest](../../../../internal/queryopttest/README.md).
