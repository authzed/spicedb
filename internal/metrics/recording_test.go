package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecordingFactoryRecordsCounterGaugeAndHistogramValues(t *testing.T) {
	rf := NewRecordingFactory()

	counter := rf.Counter(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "recording_total", Help: "test counter"})
	counter.Inc()
	counter.Add(2.5)
	require.InEpsilon(t, 3.5, rf.CounterValue("spicedb", "metrics", "recording_total"), 1e-9)

	gaugeVec := rf.GaugeVec(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "recording_current", Help: "test gauge"}, []string{"kind"})
	gauge := gaugeVec.WithLabelValues("check")
	gauge.Set(4)
	gauge.Add(1.5)
	gauge.Inc()
	require.InEpsilon(t, 6.5, rf.GaugeVecValue("spicedb", "metrics", "recording_current", "check"), 1e-9)

	histogramVec := rf.HistogramVec(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "recording_seconds", Help: "test histogram", Buckets: []float64{0.1, 1, 10}}, []string{"kind"})
	histogram := histogramVec.WithLabelValues("check")
	histogram.Observe(0.5)
	histogram.Observe(1.5)
	require.Equal(t, uint64(2), rf.HistogramVecCount("spicedb", "metrics", "recording_seconds", "check"))
	require.InEpsilon(t, 2.0, rf.HistogramVecSum("spicedb", "metrics", "recording_seconds", "check"), 1e-9)
}

func TestRecordingFactoryGaugeAndHistogramAndCounterVec(t *testing.T) {
	rf := NewRecordingFactory()

	// Gauge (non-vec)
	gauge := rf.Gauge(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "gauge_val", Help: "test gauge"})
	gauge.Inc()
	gauge.Add(2)
	gauge.Set(10)
	require.InEpsilon(t, 10.0, rf.GaugeValue("spicedb", "metrics", "gauge_val"), 1e-9)

	// Histogram (non-vec)
	hist := rf.Histogram(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "hist_seconds", Help: "test histogram"})
	hist.Observe(1.0)
	hist.Observe(2.0)
	require.Equal(t, uint64(2), rf.HistogramCount("spicedb", "metrics", "hist_seconds"))
	require.InEpsilon(t, 3.0, rf.HistogramSum("spicedb", "metrics", "hist_seconds"), 1e-9)

	// CounterVec
	cv := rf.CounterVec(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "cv_total", Help: "test counter vec"}, []string{"op"})
	cv.WithLabelValues("read").Inc()
	cv.WithLabelValues("read").Add(4)
	cv.WithLabelValues("write").Add(7)
	require.InEpsilon(t, 5.0, rf.CounterVecValue("spicedb", "metrics", "cv_total", "read"), 1e-9)
	require.InEpsilon(t, 7.0, rf.CounterVecValue("spicedb", "metrics", "cv_total", "write"), 1e-9)
}

func TestRecordingFactoryReturnsZeroForNeverCreatedMetrics(t *testing.T) {
	rf := NewRecordingFactory()

	require.Equal(t, 0.0, rf.CounterValue("a", "b", "c"))
	require.Equal(t, 0.0, rf.CounterVecValue("a", "b", "c", "x"))
	require.Equal(t, 0.0, rf.GaugeValue("a", "b", "c"))
	require.Equal(t, 0.0, rf.GaugeVecValue("a", "b", "c", "x"))
	require.Equal(t, uint64(0), rf.HistogramCount("a", "b", "c"))
	require.Equal(t, 0.0, rf.HistogramSum("a", "b", "c"))
	require.Equal(t, uint64(0), rf.HistogramVecCount("a", "b", "c", "x"))
	require.Equal(t, 0.0, rf.HistogramVecSum("a", "b", "c", "x"))
}

func TestRecordingFactoryIdempotentLookup(t *testing.T) {
	rf := NewRecordingFactory()

	opts := Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "idem_total", Help: "test"}

	// Creating the same metric twice returns the same underlying instance.
	c1 := rf.Counter(opts)
	c1.Add(1)
	c2 := rf.Counter(opts)
	c2.Add(2)
	require.InEpsilon(t, 3.0, rf.CounterValue("spicedb", "metrics", "idem_total"), 1e-9)

	// Same for GaugeVec.
	gv1 := rf.GaugeVec(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "idem_gauge", Help: "test"}, []string{"k"})
	gv1.WithLabelValues("a").Set(5)
	gv2 := rf.GaugeVec(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "idem_gauge", Help: "test"}, []string{"k"})
	gv2.WithLabelValues("a").Add(3)
	require.InEpsilon(t, 8.0, rf.GaugeVecValue("spicedb", "metrics", "idem_gauge", "a"), 1e-9)
}

func TestRecordingFactoryExistingAndMissingVecLabelPaths(t *testing.T) {
	rf := NewRecordingFactory()

	// Re-acquire gauge and histogram metrics to exercise the existing-instance branches.
	gaugeOpts := Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "existing_gauge", Help: "test"}
	g1 := rf.Gauge(gaugeOpts)
	g2 := rf.Gauge(gaugeOpts)
	require.Same(t, g1, g2)

	histOpts := Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "existing_hist", Help: "test"}
	h1 := rf.Histogram(histOpts)
	h2 := rf.Histogram(histOpts)
	require.Same(t, h1, h2)

	histVecOpts := Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "existing_hist_vec", Help: "test"}
	hv1 := rf.HistogramVec(histVecOpts, []string{"kind"})
	hv2 := rf.HistogramVec(histVecOpts, []string{"kind"})
	require.Same(t, hv1, hv2)

	// Reuse an existing child and ensure missing labels on existing vecs return zero.
	hv1.WithLabelValues("present").Observe(3)
	hv2.WithLabelValues("present").Observe(2)
	require.Equal(t, uint64(2), rf.HistogramVecCount("spicedb", "metrics", "existing_hist_vec", "present"))
	require.InEpsilon(t, 5.0, rf.HistogramVecSum("spicedb", "metrics", "existing_hist_vec", "present"), 1e-9)

	// These calls exercise the "vec exists but label does not" zero paths.
	require.Equal(t, 0.0, rf.CounterVecValue("spicedb", "metrics", "cv_total", "missing"))
	require.Equal(t, 0.0, rf.GaugeVecValue("spicedb", "metrics", "idem_gauge", "missing"))
	require.Equal(t, uint64(0), rf.HistogramVecCount("spicedb", "metrics", "existing_hist_vec", "missing"))
	require.Equal(t, 0.0, rf.HistogramVecSum("spicedb", "metrics", "existing_hist_vec", "missing"))
}

func TestRecordingFactoryClose(t *testing.T) {
	rf := NewRecordingFactory()
	require.NoError(t, rf.Close())
}

func TestNoopFactoryAllMethods(t *testing.T) {
	nf := NoopFactory{}

	// All methods must be callable and not panic.
	c := nf.Counter(Opts{Name: "noop_c"})
	c.Inc()
	c.Add(1)

	cv := nf.CounterVec(Opts{Name: "noop_cv"}, []string{"k"})
	cv.WithLabelValues("v").Inc()

	g := nf.Gauge(Opts{Name: "noop_g"})
	g.Inc()
	g.Add(1)
	g.Set(0)

	gv := nf.GaugeVec(Opts{Name: "noop_gv"}, []string{"k"})
	gv.WithLabelValues("v").Set(1)

	h := nf.Histogram(Opts{Name: "noop_h"})
	h.Observe(1)

	hv := nf.HistogramVec(Opts{Name: "noop_hv"}, []string{"k"})
	hv.WithLabelValues("v").Observe(1)

	require.NoError(t, nf.Close())
}
