# Per-optimization semantic properties

This test-only package runs Rapid properties over the repository's generated
schemas/relationships and pass-specific targeted cases. It applies exactly
one descriptor and compares independent unmodified/optimized plans against the
same memdb revision, with fresh iterators, contexts and caveat runners.

## Using the runner

Call Check from an external test package with Config{Optimizer: pass.New(),
Cases: ...}. Add Applicable when the pass is safe only for certain requests.
Optional Setup runs once before properties for iterator registration/setup;
the harness itself does not import dispatch. See each optimization's
property_test.go for runnable examples.

The broad suite uses schema/v2/testing.CheckWithSchema, at most 20 relationships,
a connected three-ID pool plus an absent query ID, and two sampled targets with
all three operations (six requests). Named subject relations are sampled too.
Schema generation is already bounded to three expression levels.

Cases maps names to Rapid generators returning Case. ParseCase compiles a DSL
schema and retains original caveat definitions: Schema.ToDefinitions currently
omits their parameter types, so caveated cases must carry CaveatDefinitions.
Relationships selects valid candidate tuples with randomized inclusion, always
keeping the first. Requests supplies nine requests covering all three operations
and positive/negative anchors. Generators may construct their own schemas,
relationships, requests and contexts for more varied shapes.

RequireChange rejects a targeted case unless an applicable request changes its
outline. No-applicable-request cases fail. Unsupported operations, compilation,
datastore and execution errors fail rather than counting as equivalence.

## What is compared

Check compares effective membership and partial-context state. Lookups compare
order-independent endpoint/relation membership after OR-merging duplicate grants.
Caveat expressions are evaluated with the same complete or partial context;
raw syntax and path provenance are not semantic equality requirements.

For IterSubjects, the runner seals operation/target state before invoking the
context, following existing wildcard tests and dispatch receivers. This observes
the iterator set before the public API wrapper strips wildcard paths. Public
LookupSubjects projection behavior is a separate API concern; a redundant
conditional concrete path can change that projection even when the underlying
wildcard set is equivalent. These properties assert iterator permission
semantics, not equality of that lossy projection.

Wildcards are expanded over every explicitly mentioned ID
(including exclusions) and a generic unmentioned-subject class. This checks
finite subjects and preserves the wildcard's meaning beyond the sampled object
pool. Conditional exclusions subtract their caveat from the wildcard grant.
Subject type/relation and resource/relation identities remain part of lookup keys.
This comparator targets permission semantics, not pagination, metadata,
expiration time transitions or relationship-read/performance equivalence.

Supply contexts that exercise all relevant caveat outcomes. Sampled equivalence
is not a proof for arbitrary caveat expressions or object populations. The broad
schema generator currently lacks caveats, wildcards, permission alias chains and
recursive permissions; local targeted families fill those gaps for each pass.
Native-vs-planner integration properties remain a separate independent oracle.

## Replay and extending coverage

```sh
go test ./pkg/query/queryopt/caveatpushdown -run TestSemanticEquivalence
go test ./pkg/query/queryopt/caveatpushdown -run TestSemanticEquivalence -rapid.checks=1000
```

Rapid prints a minimized failure-file command and a seed. Replay with the emitted
-run and -rapid.failfile, or -rapid.seed. Only pass Rapid flags to test packages
that import Rapid (the catalog's own test binary does not). Diagnostics include
the optimization, schema, relationships, request, context and before/after outlines.
Persist semantic regression cases as explicit unit tests or useful replay files;
setup-error artifacts do not belong in the shipped regression corpus.

Harness self-tests deliberately add/remove access and strip conditional access.
Comparator tests pin order/duplicate invariance, subject identities, missing
context and wildcard exclusion behavior.
