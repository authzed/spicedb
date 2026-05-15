package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
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
