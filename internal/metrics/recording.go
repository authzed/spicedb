package metrics

import (
	"strings"
	"sync"
	"sync/atomic"
)

// RecordingFactory is a test-only Factory implementation that records all
// counter increments and histogram observations in memory.  No Prometheus
// dependency is required.
//
// Retrieve recorded values via CounterValue / CounterVecValue after the code
// under test has run.
type RecordingFactory struct {
	mu       sync.Mutex
	counters map[string]*recordingCounter    // GUARDED_BY(mu)
	vecs     map[string]*recordingCounterVec // GUARDED_BY(mu)
}

// NewRecordingFactory returns an empty RecordingFactory ready for use.
func NewRecordingFactory() *RecordingFactory {
	return &RecordingFactory{
		counters: make(map[string]*recordingCounter),
		vecs:     make(map[string]*recordingCounterVec),
	}
}

// Counter creates an in-memory counter identified by the fully-qualified name
// derived from opts (Namespace_Subsystem_Name).
func (f *RecordingFactory) Counter(opts Opts) Counter {
	name := metricName(opts)
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.counters[name]; ok {
		return c
	}
	c := &recordingCounter{}
	f.counters[name] = c
	return c
}

// CounterVec creates an in-memory labelled counter.
func (f *RecordingFactory) CounterVec(opts Opts, _ []string) CounterVec {
	name := metricName(opts)
	f.mu.Lock()
	defer f.mu.Unlock()
	if cv, ok := f.vecs[name]; ok {
		return cv
	}
	cv := &recordingCounterVec{children: make(map[string]*recordingCounter)}
	f.vecs[name] = cv
	return cv
}

// Close is a no-op; recording factories do not hold external resources.
func (f *RecordingFactory) Close() error { return nil }

// CounterValue returns the current value of the counter registered under
// namespace/subsystem/name.  Returns 0 if the counter was never created.
func (f *RecordingFactory) CounterValue(namespace, subsystem, name string) float64 {
	key := metricName(Opts{Namespace: namespace, Subsystem: subsystem, Name: name})
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.counters[key]; ok {
		return c.value()
	}
	return 0
}

// CounterVecValue returns the current value of a labelled counter.  Labels
// are supplied as alternating key/value pairs (e.g. "operation", "check").
// Returns 0 if the metric or label combination was never observed.
func (f *RecordingFactory) CounterVecValue(namespace, subsystem, name string, labelValues ...string) float64 {
	key := metricName(Opts{Namespace: namespace, Subsystem: subsystem, Name: name})
	f.mu.Lock()
	defer f.mu.Unlock()
	cv, ok := f.vecs[key]
	if !ok {
		return 0
	}
	return cv.value(labelValues...)
}

// ---- internal types ----

type recordingCounter struct {
	v atomic.Int64 // stored as fixed-point × 1e9 to avoid float races
}

func (c *recordingCounter) Inc() {
	c.v.Add(1e9)
}

func (c *recordingCounter) value() float64 {
	return float64(c.v.Load()) / 1e9
}

type recordingCounterVec struct {
	mu       sync.Mutex
	children map[string]*recordingCounter // GUARDED_BY(mu)
}

func (cv *recordingCounterVec) WithLabelValues(lvs ...string) Counter {
	key := strings.Join(lvs, "\x00")
	cv.mu.Lock()
	defer cv.mu.Unlock()
	if c, ok := cv.children[key]; ok {
		return c
	}
	c := &recordingCounter{}
	cv.children[key] = c
	return c
}

func (cv *recordingCounterVec) value(lvs ...string) float64 {
	key := strings.Join(lvs, "\x00")
	cv.mu.Lock()
	defer cv.mu.Unlock()
	if c, ok := cv.children[key]; ok {
		return c.value()
	}
	return 0
}

// metricName builds a canonical "namespace_subsystem_name" key, omitting
// empty parts so the result is stable regardless of which fields are set.
func metricName(opts Opts) string {
	parts := make([]string, 0, 3)
	for _, p := range []string{opts.Namespace, opts.Subsystem, opts.Name} {
		if p != "" {
			parts = append(parts, p)
		}
	}
	return strings.Join(parts, "_")
}

// NoopFactory discards all metrics.  Use when metrics are disabled.
type NoopFactory struct{}

func (NoopFactory) Counter(_ Opts) Counter                   { return noopCounter{} }
func (NoopFactory) CounterVec(_ Opts, _ []string) CounterVec { return noopCounterVec{} }
func (NoopFactory) Close() error                             { return nil }

type (
	noopCounter    struct{}
	noopCounterVec struct{}
)

func (noopCounter) Inc()                                   {}
func (noopCounterVec) WithLabelValues(_ ...string) Counter { return noopCounter{} }
