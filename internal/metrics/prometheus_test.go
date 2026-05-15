package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestPrometheusFactoryReusesCounterGaugeAndHistogramVectors(t *testing.T) {
	registry := prometheus.NewRegistry()
	pf := NewPrometheusFactory(registry)

	firstCounter := pf.Counter(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "factory_total", Help: "test counter"})
	firstCounter.Inc()
	secondCounter := pf.Counter(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "factory_total", Help: "test counter"})
	secondCounter.Add(2)

	firstGaugeVec := pf.GaugeVec(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "factory_current", Help: "test gauge"}, []string{"kind"})
	firstGaugeVec.WithLabelValues("check").Set(1)
	secondGaugeVec := pf.GaugeVec(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "factory_current", Help: "test gauge"}, []string{"kind"})
	secondGaugeVec.WithLabelValues("check").Add(2)

	firstHistogramVec := pf.HistogramVec(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "factory_seconds", Help: "test histogram", Buckets: []float64{0.1, 1, 10}}, []string{"kind"})
	firstHistogramVec.WithLabelValues("check").Observe(0.5)
	secondHistogramVec := pf.HistogramVec(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "factory_seconds", Help: "test histogram", Buckets: []float64{0.1, 1, 10}}, []string{"kind"})
	secondHistogramVec.WithLabelValues("check").Observe(1.5)

	families, err := registry.Gather()
	require.NoError(t, err)

	counterMetric := metricFamilyByName(t, families, "spicedb_metrics_factory_total")
	require.Len(t, counterMetric.GetMetric(), 1)
	require.InEpsilon(t, 3.0, counterMetric.GetMetric()[0].GetCounter().GetValue(), 1e-9)

	gaugeMetric := metricFamilyByName(t, families, "spicedb_metrics_factory_current")
	require.Len(t, gaugeMetric.GetMetric(), 1)
	require.InEpsilon(t, 3.0, gaugeMetric.GetMetric()[0].GetGauge().GetValue(), 1e-9)

	histogramMetric := metricFamilyByName(t, families, "spicedb_metrics_factory_seconds")
	require.Len(t, histogramMetric.GetMetric(), 1)
	require.Equal(t, uint64(2), histogramMetric.GetMetric()[0].GetHistogram().GetSampleCount())
	require.InEpsilon(t, 2.0, histogramMetric.GetMetric()[0].GetHistogram().GetSampleSum(), 1e-9)
}

func TestPrometheusFactoryGaugeAndHistogramAndCounterVec(t *testing.T) {
	registry := prometheus.NewRegistry()
	pf := NewPrometheusFactory(registry)

	// Gauge (non-vec)
	g := pf.Gauge(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "pf_gauge", Help: "test gauge"})
	g.Set(5)
	g.Add(2)
	g.Inc()
	// Reuse returns same gauge.
	g2 := pf.Gauge(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "pf_gauge", Help: "test gauge"})
	g2.Add(1)

	// Histogram (non-vec)
	h := pf.Histogram(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "pf_hist", Help: "test histogram", Buckets: []float64{1, 5, 10}})
	h.Observe(2)
	h.Observe(4)
	// Reuse returns same histogram.
	h2 := pf.Histogram(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "pf_hist", Help: "test histogram", Buckets: []float64{1, 5, 10}})
	h2.Observe(6)

	// CounterVec
	cv := pf.CounterVec(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "pf_cv_total", Help: "test counter vec"}, []string{"op"})
	cv.WithLabelValues("read").Inc()
	cv.WithLabelValues("write").Add(3)
	// Reuse returns same vec.
	cv2 := pf.CounterVec(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "pf_cv_total", Help: "test counter vec"}, []string{"op"})
	cv2.WithLabelValues("read").Add(2)

	families, err := registry.Gather()
	require.NoError(t, err)

	gaugeMetric := metricFamilyByName(t, families, "spicedb_metrics_pf_gauge")
	require.InEpsilon(t, 9.0, gaugeMetric.GetMetric()[0].GetGauge().GetValue(), 1e-9)

	histMetric := metricFamilyByName(t, families, "spicedb_metrics_pf_hist")
	require.Equal(t, uint64(3), histMetric.GetMetric()[0].GetHistogram().GetSampleCount())

	cvMetric := metricFamilyByName(t, families, "spicedb_metrics_pf_cv_total")
	readVal, writeVal := 0.0, 0.0
	for _, m := range cvMetric.GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "op" {
				switch lp.GetValue() {
				case "read":
					readVal = m.GetCounter().GetValue()
				case "write":
					writeVal = m.GetCounter().GetValue()
				}
			}
		}
	}
	require.InEpsilon(t, 3.0, readVal, 1e-9)
	require.InEpsilon(t, 3.0, writeVal, 1e-9)
}

func TestPrometheusFactoryNilRegistererUsesDefault(t *testing.T) {
	// NewPrometheusFactory(nil) should not panic and should create a usable factory.
	pf := NewPrometheusFactory(nil)
	require.NotNil(t, pf)
	// Ensure we get a non-nil metric back without panicking.
	c := pf.Counter(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "nil_reg_total", Help: "test"})
	require.NotNil(t, c)
}

func TestPrometheusFactoryRegistrationFailureFallsBackToOriginal(t *testing.T) {
	// When the registerer returns a non-AlreadyRegisteredError, register()
	// logs a warning and returns the originally-created metric as a fallback.
	pf := NewPrometheusFactory(failingRegisterer{})
	c := pf.Counter(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "fail_reg_total", Help: "test"})
	require.NotNil(t, c)
	require.NotPanics(t, func() { c.Inc() })
}

func TestPrometheusFactoryClose(t *testing.T) {
	registry := prometheus.NewRegistry()
	pf := NewPrometheusFactory(registry)

	pf.Counter(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "close_total", Help: "test"})
	pf.Gauge(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "close_gauge", Help: "test"})
	pf.Histogram(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "close_hist", Help: "test"})

	// Before close, metrics are registered.
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 3)

	require.NoError(t, pf.Close())

	// After close, the registry is empty.
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Empty(t, families)
}

func TestAsPrometheusHistogramVec(t *testing.T) {
	registry := prometheus.NewRegistry()
	pf := NewPrometheusFactory(registry)

	hv := pf.HistogramVec(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "as_hv", Help: "test"}, []string{"op"})

	promHV, ok := AsPrometheusHistogramVec(hv)
	require.True(t, ok)
	require.NotNil(t, promHV)

	// A RecordingFactory vec should return false.
	rf := NewRecordingFactory()
	rhv := rf.HistogramVec(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "as_hv", Help: "test"}, []string{"op"})
	_, ok = AsPrometheusHistogramVec(rhv)
	require.False(t, ok)
}

func TestAsPrometheusGaugeVec(t *testing.T) {
	registry := prometheus.NewRegistry()
	pf := NewPrometheusFactory(registry)

	gv := pf.GaugeVec(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "as_gv", Help: "test"}, []string{"op"})

	promGV, ok := AsPrometheusGaugeVec(gv)
	require.True(t, ok)
	require.NotNil(t, promGV)

	// A RecordingFactory vec should return false.
	rf := NewRecordingFactory()
	rgv := rf.GaugeVec(Opts{Namespace: "spicedb", Subsystem: "metrics", Name: "as_gv", Help: "test"}, []string{"op"})
	_, ok = AsPrometheusGaugeVec(rgv)
	require.False(t, ok)
}

func metricFamilyByName(t *testing.T, families []*dto.MetricFamily, name string) *dto.MetricFamily {
	t.Helper()
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	t.Fatalf("metric family %s not found", name)
	return nil
}
