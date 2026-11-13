# Fixed Check concurrency

This optimizer recommends bounded parallel execution for each union and
intersection or exclusion reached by a Check. It preserves the permission tree
and schema order, changes no canonical/cache identities, and reads no signals. The deployment
supplies a positive fixed limit; adaptive scheduling remains future work.

The producer emits typed `CheckConcurrency` hints. The compiler attaches the
recommendation and originating hint ID to the iterator. The request context
supplies the ledger and execution ceiling; the iterator never stores a ledger.
The runtime uses the smaller of the recommendation and ceiling. Each reducer
has its own worker limit, so nested sets cannot deadlock waiting for slots held
by their parents. Child tasks are admitted in schema order; completion order
and the decisive winner may vary.

## Expensive union

Summarized schema:

```zed
definition user {}
definition document {
  relation reader: user
  relation writer: user
  relation owner: user
  permission view = reader + writer + owner
}
```

Request: Check `document:report#view@user:alice`.

With limit two, reader and writer start first. A denial lets another child start.
An unconditional grant ends the foreground Check and cancels remaining work.
A conditional grant stays in the result while other candidates run: those paths
are OR-combined until an unconditional grant or exhaustion. Slow independent
reads can overlap, while a cheap first-branch grant may cause unnecessary reads.

## All-required intersection

Summarized schema: `permission edit = reader & writer & owner` with the same
relations. Request: Check `document:report#edit@user:alice`.

Successful paths are AND-combined. Any denial ends the foreground Check and
cancels remaining work. Missing caveat fields remain conditional rather than
becoming a denial. When all branches must run, independent reads can overlap;
a cheap early denial can make serial execution cheaper.

## Base-first exclusion

Summarized schema: `permission view = reader - banned`, with `reader` and
`banned` relations accepting users, optionally with caveats. Request: Check
`document:report#view@user:alice`.

At width two or greater both operands start, but the reducer first awaits the
reader result. An empty base denies and cancels banned work, ignoring an error
from that unused operand. A nonempty base waits for banned: unconditional banned
membership removes the base, while conditional membership adds its negation to
the base caveat. A base error takes precedence over a speculative banned error.
Width one checks the base first without speculation. The request scope tracks canceled excluded work separately
from foreground latency.

## Ownership and fallback

Legacy Observer callbacks and TraceLogger state require serial execution. Active
BFS recursion frontier collectors also retain serial execution, because children
must contribute to the same collector. The runtime records declines with
`unsupported-observer` or `active-frontier` and runs the original serial path.
Completion observers are supported through request-wide synchronized callbacks;
targets, recursion maps and caveat runners belong to individual branches.

Width one does not speculate. The compiler accepts attaching the policy as
`iterator-compiler`; each runtime invocation responds as `query-executor`.
Debug dispositions include requested, effective and ceiling widths. Repeated
invocations are traced independently, while ledger inspection retains the latest
response for each consumer. Evidence/details are collected only for tracing.

Serialized plans do not carry scheduler policy. A co-configured local receiver
reapplies its own profile and uses its own limit; remote mixed-profile deployments
are outside the first native comparison contract.

Scalar relation forms retain direct-before-indirect order and decline speculation
with `relation-child-order`. A slow direct read cannot admit an indirect userset
read before its decisive grant arrives. Independent permission operands can
overlap. The schema builder marks these permission boundaries explicitly, and
compilation, copies and serialization preserve the marker without changing the
logical canonical identity.

## Recursive inherited access

Summarized schema:

```zed
definition user {}
definition document {
  relation viewer: user
  relation parent: document
  permission view = viewer + parent->view
}
```

Request: Check `document:report#view@user:alice`, where `report` has a parent,
and the viewer grant may live on an ancestor. Independent scalar permission
operands can overlap within each reducer's limit. Each child inherits its current
recursion depth and controller bindings in an independent context. A direct grant
cancels outstanding parent work; a denial waits for both branches. Width one
remains serial, and active recursive frontier collectors force serial execution.

## Tests

Package unit tests cover recommendation targets and logical identity. The shared
Rapid harness generates schemas, relationships and requests, comparing optimized
scalar execution with a serial reference across composed sets, aliases, arrows,
exclusions, recursion, wildcard subjects and caveats. Query runtime tests use
barriers for bounded admission, nested progress, conditional reduction,
cancellation and errors. Race tests cover branch and observer ownership.
Identity tests compare marked and unmarked outlines, and serialization tests
preserve permission ownership on the wire.
