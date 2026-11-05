# Signal test framework

Use `testing/synctest` for deterministic aging, then observe typed streams into
isolated registries/owners. Provider properties should compare counts, means, availability
and revision/schema isolation against exact independent models. Quantile sketches
need rank tolerances rather than exact centroid assertions. Run concurrency tests
under the race detector. No live datastore is needed to produce a signal.

`Owner`, `View`, `Stored` and `Schema` create isolated stores and schema snapshots.
Store state-machine properties cover observations, stable snapshots, revision/schema
changes, eviction and revisits against exact counts. Provider properties cover
replay and decay separately. Never use a second invocation of the implementation
as the property oracle.

## Recording observations

`Record` publishes any typed signal input through the real writer. For completed
result-size samples, `ResultSizes` derives the execution key from a canonical
subtree and accepts one raw path count per invocation:

```go
left, right := co.Root.SubOutlines[0], co.Root.SubOutlines[1]
querysignalstest.ResultSizes(t, view, left, query.OperationIterSubjects, time.Now(), 8, 8, 8, 8)
querysignalstest.ResultSizes(t, view, right, query.OperationIterResources, time.Now(), 2, 2, 2, 2)
ctx.Key = querysignalstest.ExecutionKey
```

The default key describes unfiltered, single-operation body execution. Tests of
filters, dispatch roles or batched execution must supply explicit keys to `Record`
and use matching keys when planning. The view supplies schema/revision scope;
provider validation, caching and derivative publication remain active. Use
`Record` directly for incomplete enumerations, varied timestamps, or other signals.
