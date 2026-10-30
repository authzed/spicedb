# Branch outcomes

## Summary

Tracks grants, denials, and conditional outcomes for attempted checks.

- **Revision scope:** Exact datastore revision.
- **Aging:** No time-based aging; storage limits apply.

## Description

When a permission has several ways to grant access, some paths may succeed much
more often than others. This signal records what happens when each part of a check
actually runs: it grants access, denies access, or returns a result that still
needs caveat context.

That history can help an optimization choose what to try first. It also records
unfinished work separately: an error or cancellation is not a denial, and a branch
that was never tried tells us nothing about whether it would have succeeded.

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

A request checks whether Alice can view `document:report`, optionally supplying
`{ "flag": true }`. The signal can record what happens when the `direct` part of
that check runs. Requests for different documents or users can contribute to the
same history when they execute the same part of the plan in the same way.

Each row starts with an empty history at revision `R1`:

| What happens to `direct` | What the signal records |
| --- | --- |
| It finds no match | One attempted check and one denial |
| It grants access without a caveat | One attempted check and one grant |
| It needs more caveat context | One attempted check and one conditional result |
| It fails or is canceled | One attempt, with no completed outcome |
| `fallback` finishes the request before `direct` runs | No observation for `direct` |
| A batch of eight inputs fails without individual results | One unresolved batch, with at most eight attempts; no assumed outcomes |

One denial, one grant, one conditional result and one error produce four attempts
but only three completed outcomes. Each completed outcome accounts for one-third
of those results. A request at a different datastore revision starts a separate
history.

The order in which branches run affects what we get to observe. A branch tried
only after another branch fails has a different history from one always tried
first. Consumers need to account for that when using these frequencies.

## Details

`New` declares `Policy: signal.RevisionBound`. The store keys evidence by schema,
exact `scope.Revision.String()`, signal name, and execution population. Requests
selecting the same datastore quantization revision can share evidence. This
provider neither chooses the revision nor carries counters to another revision.

The execution key distinguishes subtree identity, Check filters, invocation mode,
strategy, and sender/receiver role. It does not identify individual object IDs.
Batch modes have separate populations from single checks. Only attempted Check
subtrees contribute; this is not an unbiased probability that a branch would win.

The estimate tracks `Attempted`, `Completed`, `Denied`, `Granted`, and
`Conditional`. Fractions divide their outcome counters by `Completed`. The combined
example above gives `Attempted=4`, `Completed=3`, and each outcome fraction `1/3`.
Errors and cancellation increase known attempts but never completed-outcome counts.

Completed batches supply per-element outcomes. When only a whole-batch error is
available, `UnresolvedBatches` and `UnresolvedAttemptUpperBound` record uncertainty
without adding known per-element attempts or outcomes. For eight inputs, those
counters increase by 1 and 8 respectively.

Unit tests and Rapid properties compare the estimate with an independent counting
model. Run `go test -race ./pkg/query/signals/branchoutcome`.
