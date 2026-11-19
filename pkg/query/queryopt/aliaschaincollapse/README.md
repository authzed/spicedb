# Alias chain collapse

Registered name: `alias-chain-collapse`. Factory: `New() optimization.Optimizer`.

## Transformation

Merge nested aliases into one alias node. `Alias(view)(Alias(read)(Alias(viewer)(DS)))`
becomes an alias with underlying RelationName `viewer` and AliasedAs
`[read, view]`. The outermost name remains the emitted path relation.

## Inputs and signals

Nested alias structure drives rewriting. Operation and SubjectRelation govern
selection in the parent catalog. There is no execution-statistics input.

## Correctness and applicability

The catalog enables this pass for Check, and for lookups whose subject relation
is ellipsis (`...`). It excludes named or bare subject-relation lookups, where
collapsing could alter self-edge behavior. Calling New directly does not enforce
selection: callers must respect this domain. The merged node gets a new ID/key
and retains its innermost alias identity and outer naming chain.

## Ordering and expected performance

Priority 0 follows the higher-priority simplifications. The catalog retains its
existing equal-priority ordering behavior. The expected benefit is fewer alias
iterator calls and wrappers; building merged alias-name slices adds planning
allocations.

## Tests

Unit tests cover chain lengths, identity, naming and invalid node shapes.
Catalog tests cover request eligibility and registered application.
Shared properties cover generated schemas, forced permission chains, and
self-edge checks at every alias name. Unsupported lookup requests are excluded
from application, with their exclusion pinned by catalog tests.

```sh
go test ./pkg/query/queryopt/aliaschaincollapse
go test ./pkg/query/queryopt/aliaschaincollapse -run TestSemanticEquivalence -rapid.checks=1000
```

Rapid reports a replay seed and minimized failure file. Use the emitted
`-run`, `-rapid.seed` or `-rapid.failfile` command to reproduce a failure.
The shared runner is documented in [queryopttest](../../../../internal/queryopttest/README.md).
