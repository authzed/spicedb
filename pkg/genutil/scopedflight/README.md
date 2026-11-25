# Scoped singleflight

This package copies `resenje.org/singleflight` v0.4.3, under the adjacent BSD
license. It preserves duplicate suppression, shared-context cancellation, panic
propagation and forgetting keys. The copied upstream tests exercise those contracts.

The production change registers each newly launched flight with `execscope`
**before** starting its goroutine, then releases the registration on exit. A
canceled caller can return before its callback starts, so registering inside the
callback cannot guarantee that a benchmark phase has finished. Followers do not
launch work and do not register another worker. Contexts without a measurement
scope keep their existing foreground completion and cancellation behavior.

Dispatch Check and Plan-Check use this group. Expand retains the upstream group.
Review future upstream updates against this launch hook and its canceled-caller
regression test; this is a narrowly scoped local copy, not a new dispatch policy.
