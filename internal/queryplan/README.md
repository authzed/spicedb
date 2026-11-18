# Shared query planning

This package owns planning policies for public v1 requests and dispatch receivers.
The API validates requests and evaluates the final caveat expression; it does not
recognize profile names or choose a different planning implementation.

`Planner.FromSchema` builds an outline, applies the profile's structural passes,
establishes dispatch boundaries, runs eligible signal-driven decisions, and
compiles it. `Planner.FromWire` decodes an already-optimized dispatch body and
applies the receiver's local scheduling recommendations while preserving wire
identities and execution order. Both return a `Prepared` plan with fresh execution
options and hint accounting. Call `Flush` after execution to publish buffered
observations, including on errors and partially consumed streams.

`Planner.Check` also owns dispatch caveat-context policy and the early root result
cache probe. Symbolic Check policies keep request values at the public evaluation
boundary; request-independent policies can return a validated cache hit before
schema conversion or compilation. Learned policies still prepare per request so
context observations and recommendations remain current.

Profiles configure these capabilities in `pkg/query/queryprofile`. Native parity
currently applies only to Check; schema-built lookup plans retain the normal
optimizer and signal pipeline. Receiver scheduling uses the receiving deployment's
policy, independently of the sender. Reducing the supported profile set therefore
does not require another API or receiver planning path.

## Compiled Check plan reuse

Request-independent policies remove repeated schema-to-outline construction,
optimization, compilation, and receiver deserialization. Currently this retains
completed, read-only iterator trees for `native-parity-v1` Checks. The cache belongs
to the exact immutable single-store schema that owns the iterator's relation
pointers. Legacy readers and learned policies build per request. No learned
signals select this reuse policy.

Service keys contain the full profile and limits, resource type, permission,
normalized subject type/relation, and operation. Receiver keys contain the
exact serialized plan, receiver profile/limits, and normalized operation.
Single Check and both CheckMany operations use the logical Check policy.
Object IDs, caveat context, datastore revision, depth budget and result mode
remain request execution inputs; they are never retained in the plan.

Compilation and hint application finish before publication. Every execution
gets an independent copy of the never-executed compiled hint ledger, retaining
hint IDs and compiler decisions. It creates fresh context, recursion bindings,
depth, caveat runner, reader and executor. Runtime reducers still account for
accepted or declined hints separately in each request. Trees must not be mutated
after publication, and returned paths remain independently owned.

## Example: different objects and revisions

Summarized schema:

```zed
 definition user {}
 definition folder {
   relation reader: user
   relation writer: user
   permission view = reader + writer
 }
```

Summarized requests and scenarios:

- Check `folder:a#view@user:alice` at revision R1. Build and compile the service
  plan keyed by `folder`, `view`, `user#...`, Check, and the resolved profile.
  Keep its immutable iterator and compiled ledger template.
- Check `folder:b#view@user:bob` at R1. Reuse that plan and copy its ledger.
  Query relationships for `b` and `bob` with a fresh reader and execution state.
  The answer for `a` and `alice` is not stored here.
- Check the same permission at R2 after relationship changes, with the same
  stored schema. Reuse the plan but read relationships at R2; revision-sensitive
  result caching remains a separate layer.
- Change `view` to `reader & writer` in a new stored schema. Its cache starts
  empty, so neither service nor receiver can retain the obsolete union plan.
- Send identical plan bytes to receivers configured with widths 2 and 8. Each
  profile has a separate entry; the sender's wire policy cannot override local
  receiver recommendations. Different bytes remain different entries even if
  their logical dispatch keys match.

## Budget and lifecycle

A byte-costed LRU bounds each schema's retained plans. The schema cache reserves
`max(512 KiB, 16 × stored schema bytes)`, capped at 4 MiB, before admission.
The plan LRU accounts for retained keys, iterator nodes, arguments, canonical
keys and compiled hint templates using conservative estimates. Oversized plans
execute without retention; admitting another plan evicts least-recently-used
entries until its cost fits. The minimum covers the service plan and its nested
receiver working set; smaller allowances can evict the warm root during its own
execution, turning every result miss into a planning miss. Eviction does not
invalidate trees already used by active requests. Losing the stored schema discards its plan cache too.

Concurrent first requests share construction through scoped singleflight.
Cancellation of one caller does not cancel live peers. Build failures are not
retained. Caches never retain request contexts, paths, runners or executors.

Package tests cover early root hits, symbolic context isolation, operation-scoped
policy, revision-scoped evidence, reuse, schema/policy/wire identity, request hint
and path ownership, concurrent first loads, errors, cancellation, byte bounds and
LRU behavior. Public and generated-schema tests verify execution semantics through
the service and receiver integration.
