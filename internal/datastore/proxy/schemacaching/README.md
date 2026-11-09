# Schema caching

The production schema cache retains read-only namespace and caveat definitions
under its configured byte budget. Named lookups and complete legacy listings use
exact datastore revision keys. The watch-enabled reader uses the revision cache
for complete listings because an index of individually watched definitions does
not establish which names existed at an arbitrary historical revision.

For example, a schema has `user` and `document`, relations `r1` through `r16`, and
`permission view = r1 & ... & r16`. A Check request asks whether
`user:alice` can `view` `document:doc`. QP dispatches relation aliases and each
receiver needs schema definitions to deserialize its subtree. Before listing
caching, every receiver issued namespace and caveat listing queries even with a
warm schema cache. After the first successful listing at that exact revision,
subsequent receivers use the same retained definitions without backend listings.

Concurrent misses share one load per listing kind and revision. Successful empty
listings are cached; backend failures are retried. Each caller receives its own
slice container, while definition protos retain the existing read-only contract.
Definitions, retained slice elements, and entry overhead count against the same
cache budget as named lookups. Oversized entries may be rejected or evicted.

A different revision loads its own listing, including additions and deletions.
Read-write transactions use their existing readers rather than snapshot listing
entries, so writes within a transaction remain visible. Unified schema storage
continues to use the data layer's stored-schema cache. This change does not cache
query outlines or compiled iterators, change execution profiles, or rewrite plans.

All schema-cache flights register with an optional execution scope before launch,
so cancellation cannot let unstarted work escape a benchmark cleanup barrier.
Tests cover concurrent misses, revisions, slice ownership, emptiness, error retry,
capacity rejection, canceled flights, and both reader modes. The public Check
benchmark regression verifies that warmed dispatched aliases make no schema
backend calls while result-cache misses continue to execute relationships.
