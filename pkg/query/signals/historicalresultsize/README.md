# Historical result size

A cross-revision derivative of accepted `result-size-at-revision` increments.
Source epochs/sequences prevent re-applying an increment. Cumulative snapshots
are never replayed as observations. Original start time determines observation age.

By default, 10-second buckets retain at most five minutes of evidence. Bucket midpoint
age gives a 60-second exponential half-life. Exact sums and zero counters use the
same weights as the mixture of compression-100 t-digest CDFs. Quantiles use at
most 64 bisection steps. Effective weight below one is unavailable. Decay reduces
weight, not the numeric value estimated; no integer rounding of decayed weights.

Schema transfer retains only unchanged transitive dependencies/comment changes,
with immutable cloning and original timestamps; structural/caveat changes reset
its affected entries. This evidence may describe different or incomparable revisions
and is a heuristic, never a snapshot fact. Package properties compare constant streams
and weighted mixture fixtures against independent expectations.

`NewWithOptions` configures bucket width, retention and half-life; zero values use
10 seconds, five minutes and one minute respectively. Invalid durations or more
than 1024 buckets are rejected. Decay changes evidence weight, never observed values.
Weighted CDF mixtures preserve fractional bucket weights.

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

Summarized check: `CheckPermission(document:report, view, user:alice)` at revision
`R2`. The check's outcome is not a result-size observation. The arrow planner can
use historical size estimates for its matching enumeration subtrees when `R2`
has no exact-revision size evidence. For example, completed `LookupSubjects`
enumerations at `R1` can have trained those populations earlier.

For a fixed schema and execution key, assume two accepted `R1` enumerations had
raw counts 2 and 6. Their original start times are at the same 10-second bucket
midpoint, `t0`. With the default aging options:

| Scenario | What the signal stores or returns |
| --- | --- |
| Both source increments arrive at `t0` | One bucket containing a digest, exact count/sum/zero statistics, plus source epoch/sequence watermarks; `Samples=2`, `Weight=2`, `Mean=4`, `Min=2`, `Max=6` |
| The same execution key is read at `R2`, 60 seconds after `t0` | The `CrossRevision` entry is shared: `Samples=2`, `Weight=1`, `Mean=4`; effective weight halves while the estimated values stay the same |
| That population is read 120 seconds after `t0`, with no new samples | Effective weight is 0.5, below the threshold of 1; availability is `Expired` |
| A previously accepted source epoch/sequence is delivered again | No additional sample; accepted increments are deduplicated |
| A source observation arrives after its retention window | No historical sample is added; arrival does not renew its original age |
| An unrelated schema definition changes | The provider can clone compatible evidence into the new schema scope while preserving original timestamps |
| `group#member` changes, or a dependent caveat changes | Affected evidence is reset; an old size estimate is not transferred |

The source samples remain revision-bound in `resultsize`; only this derivative
has `Policy: signal.CrossRevision`. It stores aged bucket distributions rather
than treating `R1` observations as facts about `R2`. No ordering or comparability
between those datastore revisions is required. Retention is measured from each
bucket's start, while decay uses its midpoint. Colocated tests and Rapid properties
verify aging, deduplication, schema transfer, and weighted mixtures; run
`go test -race ./pkg/query/signals/historicalresultsize`.
