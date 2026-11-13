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
prefer_primary()                               // primary only: the same as hedged([])

// CEL requires both branches of a conditional to have the same type: once one branch calls a
// strategy function, the other must too.
request.resource_relation.namespace == 'document'
  ? prefer_secondary(['tiger'], duration('50ms'))
  : prefer_primary()
```

Unknown functions, wrong argument types, and literal durations of zero or less (or malformed)
are rejected when the expression is parsed, at startup. A duration computed from the request is
checked when the expression is evaluated: if it is invalid, the request goes to the primary
only, a warning is logged, and `remote_dispatch_expr_eval_errors_total` is incremented. A cursor
produced by a secondary locks its continuations to that secondary, and the expression is not
evaluated for them.

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
secondaries. Until 10 samples exist, the primary waits a fixed starting delay of 1ms, whatever
the cap. For relations a secondary reported as unsupported, it doesn't wait at all.

`prefer_primary()` takes no arguments and sends the request to the primary only, exactly like
`hedged([])`. There is no race, so it has no options and is not counted in
`remote_dispatch_secondary_strategy_total`.

The duration means different things in each function. In `hedged` it is an upper **cap** on an
adaptive delay, so the primary usually starts much earlier. In `prefer_secondary` it is a fixed
**deadline**. When the deadline passes, the secondaries keep running and race the primary.

With either strategy, a request fails only once every participant has failed: a failed primary
waits for the secondaries, which may answer. One exception, for Check only: when the primary
fails because of the request itself (invalid argument, failed precondition such as an unknown
definition, out of range, or the maximum dispatch depth exceeded), the secondaries would fail
the same way, so the error is returned at once (`isRequestError` in [race.go](race.go)).

## Design

A race is split into mechanism and policy:

- **Engine** (`runRace` and `runUnaryRace`, [race.go](race.go)): goroutines, contexts, streams,
  buffering, choosing a single winner, publishing, cursor tagging and cancellation. Every
  request type uses it; Check goes through a one-message adapter (`unaryReceiver`).
- **Policy** (`racePolicy`, [strategy.go](strategy.go)): the decisions at each point of a
  participant's lifecycle: when it may start, when it may claim the win, and which error to
  return when nobody won. It is created per request by a `secondaryStrategy`, the value a CEL
  strategy function returns. `basePolicy` holds the defaults; strategies embed it and override
  only what they change.

What races learn lives in `raceStats` ([stats.go](stats.go)): the secondaries' first-result
latency digest and the relations they reported as unsupported. The engine records into it for
every strategy, so all traffic trains the statistics `hedged` reads. Successes are recorded in a
goroutine of their own, off the dispatch's critical path.

A race ends in one of two ways:

- **Streaming** (`runRace`, for LookupResources and LookupSubjects): the winner keeps
  streaming after it wins. Each participant has its own context, the losers are canceled as
  soon as there is a winner, and the race returns once every participant has exited, so none
  outlives the stream.
- **Unary** (`runUnaryRace`, for Check): the winner holds its whole result when it wins. The
  participants share the caller's context, the race returns as soon as the winner has exited,
  and the losers stop when the caller cancels that context, exiting in the background.

Check with secondaries is on the hot path; `BenchmarkSecondaryDispatching` guards it. Compare
runs at a fixed `-benchtime`, because the latency digest's cost grows with the iteration count.
Three details matter, each commented where it is implemented:

- participant goroutines keep a shallow stack (the stack rule above `traceDispatcher`);
- the winner signals the caller as its very last action (`runAndReport`);
- per-request-kind metric children are resolved once ([metrics.go](metrics.go)).

## Adding a strategy

1. Create `strategy_<name>.go` with a `secondaryStrategy` value type, a policy embedding
   `basePolicy`, and a `strategyDescriptor` declaring its CEL function.
2. Add the descriptor to `registeredStrategies` in [expr.go](expr.go).
3. Test the policy by calling its methods directly, with `testing/synctest` for anything
   time-based, and add a case per request kind to
   [strategy_integration_test.go](strategy_integration_test.go).

## Metrics

All are prefixed with `spicedb_dispatch_`:

- `remote_dispatch_handler_total{request_kind, handler_name}`: which participant won.
- `remote_dispatch_secondary_strategy_total{rpc, strategy}`: dispatches that had secondaries, by
  strategy (`hedged`, `prefer_secondary`, or `cursor_locked` for continuations pinned to a
  secondary).
- `remote_dispatch_prefer_secondary_primary_release_total{rpc, reason}`: why a
  `prefer_secondary` primary started (`deadline` or `secondaries_failed`).
- `remote_dispatch_primary_total{cancelled, rpc}`: whether a scheduled primary ran.
- `remote_dispatch_expr_eval_errors_total{rpc}`: dispatches whose expression failed to evaluate
  and went to the primary only.
- `remote_dispatch_hedge_wait_duration_seconds` and `..._hedge_actual_wait_duration_seconds`:
  computed and actual primary wait, `hedged` only.

For example:

- share of dispatches answered by each participant:
  `sum by (handler_name) (rate(spicedb_dispatch_remote_dispatch_handler_total[5m]))`
- how often `prefer_secondary`'s deadline fires:
  `rate(spicedb_dispatch_remote_dispatch_prefer_secondary_primary_release_total{reason="deadline"}[5m])
  / rate(spicedb_dispatch_remote_dispatch_secondary_strategy_total{strategy="prefer_secondary"}[5m])`
