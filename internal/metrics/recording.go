package metrics

import (
	"strings"
	"sync"
)

// RecordingFactory is a test-only Factory implementation that records all
// counter increments and histogram observations in memory.  No Prometheus
// dependency is required.
//
// Retrieve recorded values via CounterValue / CounterVecValue after the code
// under test has run.
type RecordingFactory struct {
	mu            sync.Mutex
	counters      map[string]*recordingCounter      // GUARDED_BY(mu)
	counterVecs   map[string]*recordingCounterVec   // GUARDED_BY(mu)
	gauges        map[string]*recordingGauge        // GUARDED_BY(mu)
	gaugeVecs     map[string]*recordingGaugeVec     // GUARDED_BY(mu)
	histograms    map[string]*recordingHistogram    // GUARDED_BY(mu)
	histogramVecs map[string]*recordingHistogramVec // GUARDED_BY(mu)
}

// NewRecordingFactory returns an empty RecordingFactory ready for use.
func NewRecordingFactory() *RecordingFactory {
	return &RecordingFactory{
		counters:      make(map[string]*recordingCounter),
		counterVecs:   make(map[string]*recordingCounterVec),
		gauges:        make(map[string]*recordingGauge),
		gaugeVecs:     make(map[string]*recordingGaugeVec),
		histograms:    make(map[string]*recordingHistogram),
		histogramVecs: make(map[string]*recordingHistogramVec),
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
	if cv, ok := f.counterVecs[name]; ok {
		return cv
	}
	cv := &recordingCounterVec{children: make(map[string]*recordingCounter)}
	f.counterVecs[name] = cv
	return cv
}

// Gauge creates an in-memory gauge identified by the fully-qualified name
// derived from opts (Namespace_Subsystem_Name).
func (f *RecordingFactory) Gauge(opts Opts) Gauge {
	name := metricName(opts)
	f.mu.Lock()
	defer f.mu.Unlock()
	if g, ok := f.gauges[name]; ok {
		return g
	}
	g := &recordingGauge{}
	f.gauges[name] = g
	return g
}

// GaugeVec creates an in-memory labelled gauge.
func (f *RecordingFactory) GaugeVec(opts Opts, _ []string) GaugeVec {
	name := metricName(opts)
	f.mu.Lock()
	defer f.mu.Unlock()
	if gv, ok := f.gaugeVecs[name]; ok {
		return gv
	}
	gv := &recordingGaugeVec{children: make(map[string]*recordingGauge)}
	f.gaugeVecs[name] = gv
	return gv
}

// Histogram creates an in-memory histogram identified by the fully-qualified
// name derived from opts (Namespace_Subsystem_Name).
func (f *RecordingFactory) Histogram(opts Opts) Histogram {
	name := metricName(opts)
	f.mu.Lock()
	defer f.mu.Unlock()
	if h, ok := f.histograms[name]; ok {
		return h
	}
	h := &recordingHistogram{}
	f.histograms[name] = h
	return h
}

// HistogramVec creates an in-memory labelled histogram.
func (f *RecordingFactory) HistogramVec(opts Opts, _ []string) HistogramVec {
	name := metricName(opts)
	f.mu.Lock()
	defer f.mu.Unlock()
	if hv, ok := f.histogramVecs[name]; ok {
		return hv
	}
	hv := &recordingHistogramVec{children: make(map[string]*recordingHistogram)}
	f.histogramVecs[name] = hv
	return hv
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
	cv, ok := f.counterVecs[key]
	if !ok {
		return 0
	}
	return cv.value(labelValues...)
}

// GaugeValue returns the current value of the gauge registered under
// namespace/subsystem/name. Returns 0 if the gauge was never created.
func (f *RecordingFactory) GaugeValue(namespace, subsystem, name string) float64 {
	key := metricName(Opts{Namespace: namespace, Subsystem: subsystem, Name: name})
	f.mu.Lock()
	defer f.mu.Unlock()
	if g, ok := f.gauges[key]; ok {
		return g.value()
	}
	return 0
}

// GaugeVecValue returns the current value of a labelled gauge. Returns 0 if the
// metric or label combination was never observed.
func (f *RecordingFactory) GaugeVecValue(namespace, subsystem, name string, labelValues ...string) float64 {
	key := metricName(Opts{Namespace: namespace, Subsystem: subsystem, Name: name})
	f.mu.Lock()
	defer f.mu.Unlock()
	gv, ok := f.gaugeVecs[key]
	if !ok {
		return 0
	}
	return gv.value(labelValues...)
}

// HistogramCount returns the number of observations recorded for the histogram.
func (f *RecordingFactory) HistogramCount(namespace, subsystem, name string) uint64 {
	key := metricName(Opts{Namespace: namespace, Subsystem: subsystem, Name: name})
	f.mu.Lock()
	defer f.mu.Unlock()
	if h, ok := f.histograms[key]; ok {
		return h.count()
	}
	return 0
}

// HistogramSum returns the sum of observations recorded for the histogram.
func (f *RecordingFactory) HistogramSum(namespace, subsystem, name string) float64 {
	key := metricName(Opts{Namespace: namespace, Subsystem: subsystem, Name: name})
	f.mu.Lock()
	defer f.mu.Unlock()
	if h, ok := f.histograms[key]; ok {
		return h.sumValue()
	}
	return 0
}

// HistogramVecCount returns the number of observations recorded for a labelled histogram.
func (f *RecordingFactory) HistogramVecCount(namespace, subsystem, name string, labelValues ...string) uint64 {
	key := metricName(Opts{Namespace: namespace, Subsystem: subsystem, Name: name})
	f.mu.Lock()
	defer f.mu.Unlock()
	hv, ok := f.histogramVecs[key]
	if !ok {
		return 0
	}
	return hv.count(labelValues...)
}

// HistogramVecSum returns the sum of observations recorded for a labelled histogram.
func (f *RecordingFactory) HistogramVecSum(namespace, subsystem, name string, labelValues ...string) float64 {
	key := metricName(Opts{Namespace: namespace, Subsystem: subsystem, Name: name})
	f.mu.Lock()
	defer f.mu.Unlock()
	hv, ok := f.histogramVecs[key]
	if !ok {
		return 0
	}
	return hv.sumValue(labelValues...)
}

// ---- internal types ----

type recordingCounter struct {
	mu sync.Mutex
	v  float64 // GUARDED_BY(mu)
}

func (c *recordingCounter) Inc() {
	c.Add(1)
}

func (c *recordingCounter) Add(delta float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.v += delta
}

func (c *recordingCounter) value() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.v
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

type recordingGauge struct {
	mu sync.Mutex
	v  float64 // GUARDED_BY(mu)
}

func (g *recordingGauge) Inc() {
	g.Add(1)
}

func (g *recordingGauge) Add(delta float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.v += delta
}

func (g *recordingGauge) Set(value float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.v = value
}

func (g *recordingGauge) value() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.v
}

type recordingGaugeVec struct {
	mu       sync.Mutex
	children map[string]*recordingGauge // GUARDED_BY(mu)
}

func (gv *recordingGaugeVec) WithLabelValues(lvs ...string) Gauge {
	key := strings.Join(lvs, "\x00")
	gv.mu.Lock()
	defer gv.mu.Unlock()
	if g, ok := gv.children[key]; ok {
		return g
	}
	g := &recordingGauge{}
	gv.children[key] = g
	return g
}

func (gv *recordingGaugeVec) value(lvs ...string) float64 {
	key := strings.Join(lvs, "\x00")
	gv.mu.Lock()
	defer gv.mu.Unlock()
	if g, ok := gv.children[key]; ok {
		return g.value()
	}
	return 0
}

type recordingHistogram struct {
	mu               sync.Mutex
	observationCount uint64  // GUARDED_BY(mu)
	sum              float64 // GUARDED_BY(mu)
}

func (h *recordingHistogram) Observe(value float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.observationCount++
	h.sum += value
}

func (h *recordingHistogram) count() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.observationCount
}

func (h *recordingHistogram) sumValue() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sum
}

type recordingHistogramVec struct {
	mu       sync.Mutex
	children map[string]*recordingHistogram // GUARDED_BY(mu)
}

func (hv *recordingHistogramVec) WithLabelValues(lvs ...string) Histogram {
	key := strings.Join(lvs, "\x00")
	hv.mu.Lock()
	defer hv.mu.Unlock()
	if h, ok := hv.children[key]; ok {
		return h
	}
	h := &recordingHistogram{}
	hv.children[key] = h
	return h
}

func (hv *recordingHistogramVec) count(lvs ...string) uint64 {
	key := strings.Join(lvs, "\x00")
	hv.mu.Lock()
	defer hv.mu.Unlock()
	if h, ok := hv.children[key]; ok {
		return h.count()
	}
	return 0
}

func (hv *recordingHistogramVec) sumValue(lvs ...string) float64 {
	key := strings.Join(lvs, "\x00")
	hv.mu.Lock()
	defer hv.mu.Unlock()
	if h, ok := hv.children[key]; ok {
		return h.sumValue()
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

func (NoopFactory) Counter(_ Opts) Counter                       { return noopCounter{} }
func (NoopFactory) CounterVec(_ Opts, _ []string) CounterVec     { return noopCounterVec{} }
func (NoopFactory) Gauge(_ Opts) Gauge                           { return noopGauge{} }
func (NoopFactory) GaugeVec(_ Opts, _ []string) GaugeVec         { return noopGaugeVec{} }
func (NoopFactory) Histogram(_ Opts) Histogram                   { return noopHistogram{} }
func (NoopFactory) HistogramVec(_ Opts, _ []string) HistogramVec { return noopHistogramVec{} }
func (NoopFactory) Close() error                                 { return nil }

type (
	noopCounter      struct{}
	noopCounterVec   struct{}
	noopGauge        struct{}
	noopGaugeVec     struct{}
	noopHistogram    struct{}
	noopHistogramVec struct{}
)

func (noopCounter) Inc()                                       {}
func (noopCounter) Add(_ float64)                              {}
func (noopCounterVec) WithLabelValues(_ ...string) Counter     { return noopCounter{} }
func (noopGauge) Inc()                                         {}
func (noopGauge) Add(_ float64)                                {}
func (noopGauge) Set(_ float64)                                {}
func (noopGaugeVec) WithLabelValues(_ ...string) Gauge         { return noopGauge{} }
func (noopHistogram) Observe(_ float64)                        {}
func (noopHistogramVec) WithLabelValues(_ ...string) Histogram { return noopHistogram{} }
