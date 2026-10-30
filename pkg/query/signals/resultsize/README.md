# Result size

Some permission operations build a list of possible matches before checking them.
This signal records how large those completed lists tend to be. An optimization
can use that history to compare ways of doing the work, such as which side of an
arrow to explore first.

It learns only from work that finishes. A search that stops early, fails, or is
canceled tells us too little about the full list, so it is not counted as an empty
result.

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

A request checks whether Alice can view `document:report`. Gathering the possible
matches while following `group->member` can teach this signal about list sizes.
A request to list the document's viewers can also contribute. A simple yes/no
answer to the permission check is not itself a size measurement.

For the same part of the plan at one datastore revision:

| What happens | What the signal learns |
| --- | --- |
| Three searches finish with 0, 2, and 4 paths | The average is 2; one-third of searches are empty; sizes range from 0 to 4 |
| A search stops after its first path | Nothing about its full size; no sample is added |
| A search fails or is canceled | No sample is added |
| Two paths lead to the same user | Both paths count, even if the final API response lists the user once |
| A path includes a wildcard or a caveat | It still counts as a produced path |
| A request uses a different datastore revision | It starts with a separate size history |

The signal keeps a summary of the sizes, not the users or a list of every past
measurement. Older measurements from other revisions are handled separately by
[historical result size](../historicalresultsize/README.md).

## Details

The provider uses `RevisionBound`. Its execution key distinguishes subtree,
operation, type/relation filter, invocation mode, strategy and role within the
schema and exact selected revision.

Only normally exhausted enumerations contribute. Point `Check` operations,
cancellation, errors and consumer early stops supply no cardinality sample. Zero
is a valid completed sample. Counts are raw iterator paths before API projection
or deduplication, not distinct-entity cardinalities. If API deduplication eagerly
exhausts an underlying iterator, that completed population is valid even if the
API consumer later stops.

A compression-100 t-digest represents the distribution; exact sums, sample counts
and zero counters supply the other statistics. The example counts 0, 2 and 4 give
`Samples=3`, `Weight=3`, `Mean=2`, `ZeroFraction=1/3`, `Min=0`, and `Max=4`.
Quantiles are approximate. Consumers must match the enumeration key rather than
substituting the enclosing Check's outcome population.

Digest clones use a concurrency-safe random source so independently retained
schema histories can continue learning concurrently. Memory accounting includes
preallocated buffers and slice-growth headroom: even a sparse digest reserves
about 16 KiB before entry and key overhead. A smaller per-entry budget declines
it. Historical buckets account for their digests independently and may be evicted
before their configured retention period when storage is exhausted.

Unit tests and Rapid properties compare estimates with independent expectations
and cover snapshot ownership and memory bounds. Run
`go test -race ./pkg/query/signals/resultsize`.
