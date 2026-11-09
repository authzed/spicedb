# Execution scopes

A Check can finish after a decisive grant or denial while canceled sibling work
is still unwinding. An optional execution scope lets a caller wait for that work
before measuring a new phase or tearing down shared fixtures. It also records
configured concurrency and serial fallback decisions.

Attach a scope with `scope.Context(ctx)`. Register each task with `Begin` before
launching it, and call the returned function when it finishes. Keep a root
registration until the caller has finished launching tasks, including nested
ones. Call `Wait` outside the foreground request-latency measurement.

Contexts without a scope retain their usual scheduling and cancellation behavior.
A scope tracks completion; it does not impose a worker limit or cancel tasks.
Call `ResetStatistics` only after all registered work has quiesced. Statistics
measure policy and cleanup barriers, rather than simultaneous datastore queries.

Unit tests cover nested work, cancellation cleanup and policy accounting. Run
`go test -race ./pkg/genutil/execscope ./internal/taskrunner`.
