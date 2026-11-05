# Planner signal bridge

`Bind` reuses an already-loaded schema and selected revision without datastore reads.
`Plan` runs post-dispatch decisions using memoized estimates. Collector buffers
terminal events and flushes once on request exit. Oversize buffers and invalid
observations are dropped without affecting permission results. Context fields are
sampled once at ingress after validation.

Counts measure raw paths before API deduplication/projection. A consumer stopping
after an eager deduplicator exhausted its underlying iterator does not invalidate
that completed sample. Directly stopped, canceled or errored populations never train
result-size evidence. Unknown descriptors are dropped, not pooled with known ones.

Use the [shared signal test harness](../querysignalstest/README.md) for deterministic
streams. Tests cover revision binding, context typing/truncation, idempotent flush
and incomplete observations. Run under the race detector. Benchmarks measure batch
and planning overhead, not overall QP speedup.

## Overhead measurements

A local 100 ms microbenchmark run on Apple M1 Max measured approximately 0.18 µs
for memoized lookup (112 B, one allocation), 0.61 µs for a fresh result-size snapshot,
0.10 µs for warm schema binding, and 5.4 µs for cold empty-schema transfer. A 32-event
completed batch took 170 µs. The two-leaf decision pipeline took about 14 µs and about
16 KiB per call with disabled, empty or warm signals. The previous empty legacy
advisor fast path took 0.04 µs. These measure different amounts of work: the new
phase validates the pipeline, computes identities and emits/accountably consumes
hints. They expose added planning overhead, not a whole-QP performance improvement.
Re-run the benchmarks for representative large plans before performance tuning.
