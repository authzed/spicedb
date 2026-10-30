# resultsize

Completed iterator path-count distribution at an exact revision, keyed by subtree, operation, filter, mode and strategy. Only normal exhaustion supplies samples; Check, cancellation, error and early stop supply no cardinality sample. Zero is a real sample. Paths include duplicates, wildcard and conditional paths: this is not distinct entity cardinality. Use a compression-100 t-digest plus exact sums and zero counters.

Registered through the explicit signal catalog. Access with typed Ref/Input values.
Evidence remains in its original schema/revision view. Colocated unit tests and
Rapid properties compare the estimate with an exact independent counting model.
Run `go test -race ./pkg/query/signals/resultsize`.

## Example

Summarized schema:

```zed
definition user {}
definition group {
    relation member: user
}
definition document {
    relation reader: user
    relation group: group
    permission view = reader + group->member
}
```

Summarized check: `CheckPermission(document:report, view, user:alice)` at revision
`R1`. A point Check outcome does not train this signal. However, iterator populations
enumerated while evaluating an arrow can supply evidence when they exhaust normally.
An explicit `LookupSubjects(document:report, view, user)` also trains the subject
enumeration populations it actually executes and exhausts.

For one fixed subtree, operation, type/relation filter, mode, strategy, and role
at `R1`, consider these scenarios:

| Scenario | What the signal stores |
| --- | --- |
| Three enumerations exhaust with raw path counts 0, 2, and 4 | `Samples=3`, `Weight=3`, `Mean=2`, `ZeroFraction=1/3`, `Min=0`, `Max=4`; a compression-100 t-digest represents the distribution |
| The consumer stops after one path, before exhaustion | No additional sample, even if more paths may exist |
| Enumeration fails or is canceled | No additional sample; neither outcome is treated as zero |
| Two paths refer to the same user | A completed enumeration contributes count 2, even if the API later deduplicates to one user |
| A completed enumeration includes a wildcard or caveated path | Each produced path contributes to its raw count |
| A request selects `R2` | It reads a separate revision-bound population; the three `R1` samples do not appear there |

The store retains digest centroids, sums, counts, and provenance rather than a list
of users or individual raw counts. Quantiles are approximate. If API deduplication
eagerly exhausts an underlying iterator, that completed raw population is a valid
sample even if the API consumer later stops. The arrow-direction optimization
uses the mean from matching enumeration keys, falling back to historical evidence
when the exact-revision estimate is unavailable.

The memory estimate includes the digest's preallocated buffers and slice-growth
headroom. Even a sparse compression-100 digest reserves about 16 KiB before entry
and key overhead, so a smaller per-entry budget declines it. Historical buckets
account for their digests independently and can be evicted before their configured
retention period when the shared storage budget is exhausted.
