# Historical result size

Recent list sizes can still be useful when a request moves to a newer datastore
revision. This signal carries that history forward while gradually reducing the
influence of older measurements. It gives an optimization something to work with
before enough observations exist at the new revision.

Old measurements become less convincing, not numerically smaller: a list that
contained 100 paths remains a measurement of 100 paths. With enough time and no
new observations, the signal stops offering an estimate.

## Example

Summarized schema:

```zed
definition user {}
definition group {
    relation member: user
}
definition document {
    relation group: group
    permission view = group->member
}
```

A request checks whether Alice can view `document:report` at revision `R2`. If the
planner has no size observations for the relevant search at `R2`, recent completed
searches at `R1` can provide a starting estimate. The permission check's yes/no
answer does not contribute a size observation.

Suppose two earlier searches found 2 and 6 paths at the same time:

| What happens next | What the signal offers |
| --- | --- |
| The measurements first arrive | An average size of 4, based on two samples |
| A request uses `R2` one minute later | The same average of 4, with half the original weight |
| Another minute passes without new samples | Too little weight remains; the estimate is unavailable |
| A measurement is delivered twice | It is counted once |
| A measurement arrives after its retention period | It is ignored; late delivery does not make old work fresh |
| An unrelated schema definition changes | Compatible history can carry over with its original age |
| `group#member` or a caveat it depends on changes | The affected history is discarded |

This is a prediction about likely work, not a claim that the data is unchanged at
`R2`. The original exact-revision observations remain separate in
[result size](../resultsize/README.md).

## Details

This cross-revision provider consumes accepted `result-size-at-revision`
increments, never cumulative snapshots. Source epochs and sequence numbers prevent
re-applying an increment. The execution key identifies the same enumeration
population across revisions; no ordering or comparability between those revisions
is assumed. `Policy: signal.CrossRevision` makes this sharing explicit.

Defaults are 10-second buckets, at most five minutes of retention, and a 60-second
exponential half-life based on bucket midpoint age. Original invocation start
times determine age. Retention is measured from bucket start; late publication
does not renew it. Storage limits can evict evidence earlier.

Exact sums and zero counters use the same fractional weights as the mixture of
compression-100 t-digest distributions. Quantiles use at most 64 bisection steps.
Decay reduces evidence weight, not the observed numeric values. Effective weight
below one returns `Expired`.

For two samples of 2 and 6 at one bucket midpoint, the initial estimate is
`Samples=2`, `Weight=2`, `Mean=4`, `Min=2`, and `Max=6`. After 60 seconds it has
`Weight=1` and `Mean=4`; after 120 seconds its weight is 0.5 and it is unavailable.

`NewWithOptions` configures bucket width, retention and half-life. Zero values use
the defaults above. Invalid durations or configurations requiring more than 1024
buckets are rejected. Each bucket retains a digest, exact statistics, and its
weighting factor; source watermarks track accepted increments.

Schema transfer clones compatible history, retaining observation timestamps.
Unchanged transitive dependencies and comment-only changes can preserve evidence;
affected structural or caveat changes reset it. Each schema copy learns
independently after transfer.

Unit tests and Rapid properties cover aging, deduplication, schema transfer, and
weighted mixtures. Run `go test -race ./pkg/query/signals/historicalresultsize`.
