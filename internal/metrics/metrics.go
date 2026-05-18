// Package metrics defines the dependency-injection interfaces for metrics
// collection in SpiceDB.  Business components receive a Factory at
// construction time; they never import prometheus directly.
//
// Two implementations are provided:
//   - PrometheusFactory  – registers real Prometheus collectors with a
//     supplied prometheus.Registerer.  Created once at the composition root
//     (main / server.Config.Complete) and threaded through constructors.
//   - RecordingFactory   – accumulates counter and histogram observations
//     in memory with no external dependencies.  Intended for unit tests that
//     need deterministic assertions on what was emitted.
//
// A NoopFactory is also available for components that are constructed with
// metrics disabled.
package metrics

import "io"

// Counter is a monotonically increasing scalar value.
type Counter interface {
	// Inc increments the counter by 1.
	Inc()
	// Add increments the counter by delta.
	Add(delta float64)
}

// Gauge is a scalar value that may increase or decrease over time.
type Gauge interface {
	// Inc increments the gauge by 1.
	Inc()
	// Add increments the gauge by delta.
	Add(delta float64)
	// Set updates the gauge to the provided value.
	Set(value float64)
}

// CounterVec is a Counter partitioned by one or more label dimensions.
type CounterVec interface {
	// WithLabelValues returns the Counter for the given label values.
	// The number of values must match the number of label names declared
	// when the CounterVec was created.
	WithLabelValues(lvs ...string) Counter
}

// GaugeVec is a Gauge partitioned by one or more label dimensions.
type GaugeVec interface {
	// WithLabelValues returns the Gauge for the given label values.
	// The number of values must match the number of label names declared
	// when the GaugeVec was created.
	WithLabelValues(lvs ...string) Gauge
}

// Histogram records observed values into a distribution.
type Histogram interface {
	// Observe records a new value.
	Observe(value float64)
}

// HistogramVec is a Histogram partitioned by one or more label dimensions.
type HistogramVec interface {
	// WithLabelValues returns the Histogram for the given label values.
	// The number of values must match the number of label names declared
	// when the HistogramVec was created.
	WithLabelValues(lvs ...string) Histogram
}

// Opts carries the metadata used to describe a metric.  The fields mirror
// prometheus.Opts / prometheus.HistogramOpts so callers can be written
// without a direct Prometheus import.
type Opts struct {
	// Namespace is prepended to the metric name, e.g. "spicedb".
	Namespace string
	// Subsystem is inserted between Namespace and Name, e.g. "dispatch_client".
	Subsystem string
	// Name is the metric base name, e.g. "check_total".
	Name string
	// Help is a human-readable description.
	Help string
	// Buckets is used only when creating histograms.
	Buckets []float64
	// NativeHistogramBucketFactor configures Prometheus native histograms.
	NativeHistogramBucketFactor float64
}

// Factory creates and registers named metrics.
// Calling Close deregisters every metric created by this factory, making
// it safe to use one factory per server lifecycle (or per test).
type Factory interface {
	// Counter creates and registers a counter metric.
	Counter(opts Opts) Counter
	// CounterVec creates and registers a labelled counter metric.
	CounterVec(opts Opts, labelNames []string) CounterVec
	// Gauge creates and registers a gauge metric.
	Gauge(opts Opts) Gauge
	// GaugeVec creates and registers a labelled gauge metric.
	GaugeVec(opts Opts, labelNames []string) GaugeVec
	// Histogram creates and registers a histogram metric.
	Histogram(opts Opts) Histogram
	// HistogramVec creates and registers a labelled histogram metric.
	HistogramVec(opts Opts, labelNames []string) HistogramVec
	// Close deregisters all metrics created by this factory.
	io.Closer
}
