# branchoutcome

Check outcomes among attempted subtrees at the exact revision. Denial, unconditional grant and conditional results have distinct counters. Errors and cancellation count as attempts but never as denial or completed outcome. This population depends on execution order; no unbiased winner probability is claimed.

Registered through the explicit signal catalog. Access with typed Ref/Input values.
Evidence remains in its original schema/revision view. Colocated unit tests and
Rapid properties compare the estimate with an exact independent counting model.
Run `go test -race ./pkg/query/signals/branchoutcome`.

Completed batches contribute per-element outcomes with their batch mode preserved.
Receiver loops observe each attempted element, including its own error. When an
executor returns a whole-batch error without per-element results, actual attempts
are unknown: `UnresolvedBatches` and `UnresolvedAttemptUpperBound` record that
uncertainty separately. Such events add no known attempts or completed outcomes.

## Revision policy

[`New`](signal.go) declares `Policy: signal.RevisionBound`. The shared store includes
`scope.Revision.String()` in the entry key for revision-bound providers, alongside
the schema hash, signal name, and execution population key. If checks select the
same datastore quantization revision, they share evidence. Selecting a different
revision starts a separate population; this provider does not quantize revisions
or carry its counters forward.

## Example

Summarized schema:

```zed
definition user {}
caveat enabled(flag bool) { flag }
definition document {
    relation direct: user | user with enabled
    relation fallback: user
    permission view = direct + fallback
}
```

Summarized request: `CheckPermission(document:report, view, user:alice)` at
revision `R1`, with optional context `{ "flag": true }`. Consider observations
of the `direct` subtree across requests with the same schema and population key.
The key distinguishes subtree identity, Check filters, invocation mode, execution
strategy, and sender/receiver role; it does not identify individual object IDs.

Each row below is a separate scenario, with counters starting empty:

| Scenario | What the signal stores |
| --- | --- |
| `direct` returns no path | `Attempted=1`, `Completed=1`, `Denied=1`, `DenyFraction=1` |
| `direct` returns an unconditional path | `Attempted=1`, `Completed=1`, `Granted=1`, `GrantFraction=1` |
| `direct` returns a caveated path with unresolved context | `Attempted=1`, `Completed=1`, `Conditional=1`, `ConditionalFraction=1` |
| `direct` errors or is canceled | `Attempted=1`, `Completed=0`; no outcome counter or fraction increases |
| Another branch short-circuits the union before `direct` runs | No observation of `direct`; its counters stay unchanged |
| A many-resources batch fails with only a whole-batch error, for 8 inputs | `UnresolvedBatches=1`, `UnresolvedAttemptUpperBound=8`; known attempt/outcome counters stay zero |

Combining one denial, one unconditional grant, one conditional result, and one
single-invocation error yields `Attempted=4`, `Completed=3`, and each completed
outcome fraction equal to `1/3`. Batch modes have separate keys from single checks.
At `R2`, none of these `R1` counters are visible. An eventual union-ordering pass
could use these frequencies to recommend ordering, while accounting for the bias
from branches that were never attempted. No such producer is enabled yet.
