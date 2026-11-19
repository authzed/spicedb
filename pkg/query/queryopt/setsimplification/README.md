# Set simplification

Registered name: `set-simplification`. Factory: `New() optimization.Optimizer`.

## Transformation

Normalize nested unions/intersections and remove redundant branches using set
laws. Examples: `A + (A & B) = A`, `A + (A - B) = A`, `A - A = empty`.
The implementation also handles duplicates, intersection absorption/complement,
left-side exclusion pruning, null identity and null propagation.

## Inputs and signals

Structural equality and provable subset relationships in the outline drive this
pass. It uses no request-specific values or runtime observations.

## Correctness and applicability

Caveats and arrows are opaque for subset reasoning: only structurally identical
units can match. The bottom-up walk applies normalization before absorption
and propagation. It makes no claim about branches that cannot be proved
redundant structurally.

## Ordering and expected performance

Priority 10 runs after caveat pushdown (20) and before reachability pruning (0).
The expected benefit is fewer branches, datastore reads and iterator calls.
Pairwise subset comparisons add planning work; no speedup is claimed without
benchmark evidence.

## Tests

Unit tests cover the algebraic laws and opaque caveat/arrow boundaries.
Shared properties use arbitrary generated set expressions and targeted
absorption families, including conditional grants and wildcard exclusions.
Targeted eligible cases must actually change the outline.

```sh
go test ./pkg/query/queryopt/setsimplification
go test ./pkg/query/queryopt/setsimplification -run TestSemanticEquivalence -rapid.checks=1000
```

Rapid reports a replay seed and minimized failure file. Use the emitted
`-run`, `-rapid.seed` or `-rapid.failfile` command to reproduce a failure.
The shared runner is documented in [queryopttest](../../../../internal/queryopttest/README.md).
