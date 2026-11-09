# Compiled Check plan reuse

This package removes repeated schema-to-outline construction, optimization,
compilation, and receiver deserialization from checks that need the same plan.
It caches completed, read-only iterator trees for `native-parity-v1` Checks.
The cache belongs to the exact immutable single-store schema that owns the
iterator's relation pointers. Legacy readers and other profiles build normally.
No learned signals select this reuse policy.

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

Package tests cover reuse, schema/policy/wire identity, request hint and path
ownership, concurrent first loads, errors, cancellation, byte bounds and LRU
behavior. The working-set test builds service and receiver plans for a mixed
userset/exclusion schema and verifies that the bounded cache retains the root.
