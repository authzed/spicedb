# Apply recommended branch order

This optimization consumes typed `BranchOrder` hints from earlier passes and
applies the recommended child permutation to unions and intersections. Trying a
likely union grant first, or a likely intersection denial first, can avoid later
work. The permission expression and canonical identities remain unchanged.

The pass reads no signals directly. A predictive producer may use signals and
attach a strong recommendation; this consumer reads the hint kind declared in its
optimizer descriptor. No predictive branch-order producer is enabled by default.

## Example

Summarized schema:

```zed
definition user {}
definition document {
  relation reader: user
  relation writer: user
  permission view = reader + writer
  permission edit = reader & writer
  permission limited = reader - writer
}
```

Summarized request: Check `document:report#view@user:alice`.

| Scenario | Recommendation and behavior |
| --- | --- |
| A producer expects `writer` to grant | Emit `BranchOrder{Order: []int{1, 0}}` for this union occurrence. The consumer applies that complete permutation and records acceptance. An incorrect prediction only changes work. |
| A producer expects `writer` to deny `edit` | The same permutation is valid for the intersection. Execution retains conjunction semantics and may stop at the denial. |
| A producer targets `limited` | Decline with `unsupported-target`: exclusion operand positions determine meaning and cannot be permuted. |
| Another rewrite changed this target's arity | Decline with `stale-target`. A malformed or duplicate-bearing permutation fails validation before it reaches this pass. |
| The same subtree appears under two permissions | Hints target occurrence IDs, so only the requested occurrence moves. Other copies retain their order. |
| No hint is present | Retain the current child order. |

Hints contain the recommendation and its lifecycle/disposition. Supporting
signal snapshot references are retained only when debug evidence collection is
active. The consumer records a structured reason when it declines a hint.

## Tests

Unit tests inspect the reordered outline and accepted/declined dispositions.
The common Rapid semantics harness generates schemas and relationships, injects
reversed orders, and compares Check, IterResources and IterSubjects behavior.
The composed-pipeline properties also combine misleading arrow-size estimates
with branch-order recommendations.

```sh
go test -race ./pkg/query/queryopt/applybranchorder
go test -race ./pkg/query/queryopt/applybranchorder \
  -run TestSemanticEquivalence -rapid.checks=1000
```
