package remote

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics used on every dispatch with secondaries are resolved per request kind once, at
// startup, into requestKindMetrics: looking a child up by label values hashes the values and
// locks the vector, and doing so from a participant goroutine also deepens its stack (see the
// note above traceDispatcher in race.go). Keep new per-dispatch metrics on this scheme, and
// check BenchmarkSecondaryDispatching, at a fixed -benchtime, when changing them.
//
// The one exception is dispatchCounter, whose handler_name label is the secondary's name.

var dispatchCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "remote_dispatch_handler_total",
	Help:      "which dispatcher handled a request",
}, []string{"request_kind", "handler_name"})

var hedgeWaitHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "remote_dispatch_hedge_wait_duration_seconds",
	Help:      "distribution in seconds of calculated wait time for hedging requests to the primary dispatcher when a secondary is active.",
	Buckets:   []float64{0.001, 0.002, 0.003, 0.005, 0.01, 0.02, 0.05, 0.1, 0.3, 0.5, 1, 10},
}, []string{"rpc"})

var hedgeActualWaitHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "remote_dispatch_hedge_actual_wait_duration_seconds",
	Help:      "distribution in seconds of actual wait time for hedging requests to the primary dispatcher when a secondary is active.",
	Buckets:   []float64{0.001, 0.002, 0.003, 0.005, 0.01, 0.02, 0.05, 0.1, 0.3, 0.5, 1, 10},
}, []string{"rpc"})

var primaryDispatch = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "remote_dispatch_primary_total",
	Help:      "indicates what was the outcome of a scheduled primary dispatch: dispatched, or cancelled",
}, []string{"cancelled", "rpc"})

var secondaryStrategyCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "remote_dispatch_secondary_strategy_total",
	Help:      "number of dispatches with secondaries, by the strategy used to race them (cursor_locked for continuations pinned to a secondary)",
}, []string{"rpc", "strategy"})

var primaryReleaseCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "remote_dispatch_prefer_secondary_primary_release_total",
	Help:      "number of prefer_secondary dispatches whose primary was started, by reason: deadline or secondaries_failed",
}, []string{"rpc", "reason"})

var exprEvalErrorsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "remote_dispatch_expr_eval_errors_total",
	Help:      "number of dispatches whose secondary dispatch expression failed to evaluate; they are sent to the primary only",
}, []string{"rpc"})

func init() {
	prometheus.MustRegister(dispatchCounter)
	prometheus.MustRegister(hedgeWaitHistogram)
	prometheus.MustRegister(hedgeActualWaitHistogram)
	prometheus.MustRegister(primaryDispatch)
	prometheus.MustRegister(secondaryStrategyCounter)
	prometheus.MustRegister(primaryReleaseCounter)
	prometheus.MustRegister(exprEvalErrorsCounter)
}

// requestKindMetrics holds the metric children of one request kind.
type requestKindMetrics struct {
	reqKey string

	hedgeWait       prometheus.Observer
	hedgeActualWait prometheus.Observer

	primaryDispatched prometheus.Counter
	primaryCanceled   prometheus.Counter

	strategies map[string]prometheus.Counter // by strategy name

	releasedAtDeadline     prometheus.Counter
	releasedOnAllFailures  prometheus.Counter
	expressionEvalFailures prometheus.Counter
}

// cursorLockedStrategyName labels races of continuations pinned to the secondary that produced
// their cursor, in which the request's strategy plays no part.
const cursorLockedStrategyName = "cursor_locked"

var metricsByRequestKind = func() map[string]*requestKindMetrics {
	byKind := make(map[string]*requestKindMetrics, len(supportsSecondaries))
	for _, reqKey := range supportsSecondaries {
		byKind[reqKey] = newRequestKindMetrics(reqKey)
	}
	return byKind
}()

func newRequestKindMetrics(reqKey string) *requestKindMetrics {
	strategies := make(map[string]prometheus.Counter, len(registeredStrategies)+1)
	for _, strategy := range registeredStrategies {
		strategies[strategy.function] = secondaryStrategyCounter.WithLabelValues(reqKey, strategy.function)
	}
	strategies[cursorLockedStrategyName] = secondaryStrategyCounter.WithLabelValues(reqKey, cursorLockedStrategyName)

	return &requestKindMetrics{
		reqKey:                 reqKey,
		hedgeWait:              hedgeWaitHistogram.WithLabelValues(reqKey),
		hedgeActualWait:        hedgeActualWaitHistogram.WithLabelValues(reqKey),
		primaryDispatched:      primaryDispatch.WithLabelValues("false", reqKey),
		primaryCanceled:        primaryDispatch.WithLabelValues("true", reqKey),
		strategies:             strategies,
		releasedAtDeadline:     primaryReleaseCounter.WithLabelValues(reqKey, "deadline"),
		releasedOnAllFailures:  primaryReleaseCounter.WithLabelValues(reqKey, "secondaries_failed"),
		expressionEvalFailures: exprEvalErrorsCounter.WithLabelValues(reqKey),
	}
}

// metricsFor returns the metric children of a request kind. Only kinds that support secondaries
// are resolved in advance; others are resolved on each call.
func metricsFor(reqKey string) *requestKindMetrics {
	if m, ok := metricsByRequestKind[reqKey]; ok {
		return m
	}
	return newRequestKindMetrics(reqKey)
}

func (m *requestKindMetrics) strategy(name string) prometheus.Counter {
	if counter, ok := m.strategies[name]; ok {
		return counter
	}
	return secondaryStrategyCounter.WithLabelValues(m.reqKey, name)
}

//go:noinline
func countPrimaryDispatch(reqKey string, canceled bool) {
	if canceled {
		metricsFor(reqKey).primaryCanceled.Inc()
	} else {
		metricsFor(reqKey).primaryDispatched.Inc()
	}
}

//go:noinline
func countWinner(reqKey string, dispatcher string) {
	dispatchCounter.WithLabelValues(reqKey, dispatcher).Add(1)
}
