# Caveat pushdown

Registered name: `simple-caveat-pushdown`. Factory: `New() optimization.Optimizer`.

## Transformation

Move a caveat wrapper toward the datastore branches that carry that caveat.
For example, `Caveat(Union[A, B])` becomes `Union[Caveat(A), B]` when only
A can emit relationships with the caveat. Relevant branches are wrapped and
pushdown continues recursively.

## Inputs and signals

Outline shape, caveat names and the allowed relation variants drive this pass.
Mixed caveated/unconditional variants are inspected through schema relation
traits. It consumes no observed execution statistics or request-specific inputs.

## Correctness and applicability

Pushdown stops at another caveat wrapper, leaves, and intersection arrows
(`all()`), whose caveats require post-intersection evaluation. It leaves a
wrapper in place if no child branch contains the caveat. Relocated wrappers
retain the original caveat node identity.

## Ordering and expected performance

Priority 20 runs before set simplification (10). Branch scanning and rebuilding
cost planning time; the expected benefit is evaluating caveats close to their
source rather than through unrelated branches.

## Tests

Unit tests pin traversal boundaries, recursive pushdown, node identities and
mixed-trait regressions. Shared properties compare all three operations on
random schemas and on mixed caveated/unconditional relations, multiple caveats
and intersection arrows, including complete and missing caveat contexts.

```sh
go test ./pkg/query/queryopt/caveatpushdown
go test ./pkg/query/queryopt/caveatpushdown -run TestSemanticEquivalence -rapid.checks=1000
```

Rapid reports a replay seed and minimized failure file. Use the emitted
`-run`, `-rapid.seed` or `-rapid.failfile` command to reproduce a failure.
The shared runner is documented in [queryopttest](../../../../internal/queryopttest/README.md).
