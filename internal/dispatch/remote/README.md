# Remote dispatch and secondary strategies

`clusterDispatcher` sends dispatches to peer SpiceDB nodes. A request can also go to
**secondary dispatchers**: named upstreams, chosen per request by a CEL expression
(`--experimental-dispatch-secondary-upstream-exprs`). The expression also chooses the
**strategy** used to race the secondaries against the primary.

## Expressions

An expression evaluates to a list of secondary names, or to a strategy function call:

```cel
['tiger']                                      // a plain list: hedged
hedged(['tiger'])                              // the same, explicitly
hedged(['tiger'], duration('5ms'))             // hedged, with this query's delay capped at 5ms
prefer_secondary(['tiger'], duration('50ms'))  // the primary waits for failure or the deadline
hedged([])                                     // primary only

// CEL requires both branches of a ternary to have the same type: once one branch calls a
// strategy function, the other must too.
request.resource_relation.namespace == 'document'
  ? prefer_secondary(['tiger'], duration('50ms'))
  : hedged(['tiger'], duration('2ms'))
```

Unknown functions and wrong argument types fail when the expression is parsed, at startup.
Durations of zero or less fail when the expression is evaluated: the request then logs a
warning and goes to the primary only. A cursor produced by a secondary locks its continuations
to that secondary, and the expression is not evaluated for them.

## Strategies

| | `hedged(list[, maxDelay])` | `prefer_secondary(list, deadline)` |
|---|---|---|
| Secondaries start | immediately | immediately |
| Primary starts after | `min(p90 of secondary first-result latency, cap)` | `deadline` |
| Primary released early when | **any** secondary fails | **all** secondaries have failed |
| Winner | first result | first result |
| Optimizes for | latency | primary offload |

For `hedged`, `cap` is `maxDelay` if given, otherwise the largest
`--experimental-dispatch-secondary-maximum-primary-hedging-delays` value among the selected
secondaries. Until 10 samples exist, the primary waits the starting delay (1ms by default). For
relations a secondary reported as unsupported, it doesn't wait at all.

The duration means different things in each function. In `hedged` it is an upper **cap** on an
adaptive delay, so the primary usually starts much earlier. In `prefer_secondary` it is a fixed
**deadline**. When the deadline passes, the secondaries keep running and race the primary.

## Design

A race is split into mechanism and policy:

- **Engine** (`runRace`, [race.go](race.go)): goroutines, per-participant contexts, streams,
  buffering, choosing a single winner, publishing, cursor tagging, cancellation and error
  selection. Every request type uses it; Check goes through a one-message adapter
  (`unaryReceiver`).
- **Policy** (`racePolicy`, [strategy.go](strategy.go)): every decision the engine takes. It is
  created per request by a `secondaryStrategy`, the value a CEL strategy function returns.

The engine asks the policy at each decision point of a participant's lifecycle:

| Method | Asked |
|---|---|
| `start(ctx, p) bool` | before the participant runs; blocks until it may, `false` means never |
| `readyToClaim(p, received, complete) bool` | after every result (and at the end of the stream): may it try to win now? Results before that are buffered, up to `maxBufferedResults` |
| `firstResult(p, latency)` | when the first result arrives; latency excludes time spent in `start` |
| `done(p, err)` | exactly once per participant, on every path |
| `noWinnerError(errs) error` | when nobody won |

`basePolicy` holds the defaults: start immediately, claim at the first result, return the
primary's error. Strategies embed it and override only what they change.

What races learn lives in `raceStats` ([stats.go](stats.go)): the secondaries' first-result
latency digest and the relations they reported as unsupported. The engine records into it for
every strategy, so all traffic trains the statistics `hedged` reads. Successes are recorded in
a goroutine of their own, off the dispatch's critical path; like the digest's `TryLock`, this is
best-effort.

A race ends in one of two ways:

- **Streaming** (`runRace`, for LookupResources and LookupSubjects): the winner keeps
  streaming after it wins. Each participant has its own context, the losers are canceled as
  soon as there is a winner, and the race returns once every participant has exited, so none
  outlives the stream.
- **Unary** (`runUnaryRace`, for Check): the winner holds its whole result when it wins. The
  participants share the caller's context, the race returns as soon as the winner has exited,
  and the losers stop when the caller cancels that context, exiting in the background.

Check with secondaries is on the hot path, and the engine keeps it at the cost of the code it
replaced (see `BenchmarkSecondaryDispatching`, compared at a fixed `-benchtime`: the digest
grows with the iteration count). Three details matter, and are commented in the code:

- the participant goroutines' stacks stay shallow (deep calls such as metrics are made from
  `runAndReport` or kept out of line), since a stack that outgrows its initial size is copied
  on every dispatch;
- the winner signals the caller as its very last action, so the caller runs next on the same
  thread instead of waking another one;
- per-request-kind metric children are resolved once, not looked up by label on every
  dispatch.

## Adding a strategy

1. Create `strategy_<name>.go` with a `secondaryStrategy` value type, a policy embedding
   `basePolicy`, and a `strategyDescriptor` declaring its CEL function.
2. Add the descriptor to `registeredStrategies` in [expr.go](expr.go).
3. Test the policy by calling its methods directly, with `testing/synctest` for anything
   time-based, and add a case per request kind to
   [strategy_integration_test.go](strategy_integration_test.go).

Nothing else changes. Some strategies need engine work first: shadow mode (let losers finish
without publishing), comparing participants' results, primary-latency feedback, or composing a
start rule from one strategy with a claim rule from another.

## Metrics

- `spicedb_dispatch_remote_dispatch_handler_total{request_kind, handler_name}`: which
  participant won.
- `spicedb_dispatch_remote_dispatch_secondary_strategy_total{rpc, strategy}`: dispatches that
  had secondaries, by strategy.
- `spicedb_dispatch_remote_dispatch_prefer_secondary_primary_release_total{rpc, reason}`: why a
  `prefer_secondary` primary started (`deadline` or `secondaries_failed`). Use it to tune the
  deadline.
- `spicedb_dispatch_remote_dispatch_primary_total{cancelled, rpc}`: whether a scheduled primary
  ran.
- `spicedb_dispatch_remote_dispatch_hedge_wait_duration_seconds` and
  `..._hedge_actual_wait_duration_seconds`: computed and actual primary wait, `hedged` only.

## Tests

- [race_test.go](race_test.go): the engine with scripted receivers, no gRPC. Covers `done` on
  every exit path, buffered claims, the buffer cap, error selection, and latency measurement.
- [strategy_test.go](strategy_test.go): each policy in a `synctest` bubble.
- [strategy_integration_test.go](strategy_integration_test.go): strategies end to end for
  Check, LookupSubjects and LookupResources over bufconn.
- [expr_test.go](expr_test.go): parsing, evaluation, and the expression cost limit.
- The `cluster_*_test.go` files: hedging, cancellation and cursor locking.
