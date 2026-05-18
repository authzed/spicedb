package pool

import (
	"testing"

	"github.com/stretchr/testify/require"

	internalmetrics "github.com/authzed/spicedb/internal/metrics"
)

func TestSetMetricsFactoryAppliesToAllPoolMetrics(t *testing.T) {
	recording := internalmetrics.NewRecordingFactory()
	SetMetricsFactory(recording)
	t.Cleanup(func() {
		SetMetricsFactory(internalmetrics.NewPrometheusFactory(nil))
	})

	resetHistogram.Observe(1)
	healthyCRDBNodeCountGauge.Set(2)
	connectionsPerCRDBNodeCountGauge.WithLabelValues("read", "1").Set(3)
	pruningTimeHistogram.WithLabelValues("read").Observe(4)

	require.Equal(t, uint64(1), recording.HistogramCount("", "", "crdb_client_resets"))
	require.InEpsilon(t, 2.0, recording.GaugeValue("", "", "crdb_healthy_nodes"), 1e-9)
	require.InEpsilon(t, 3.0, recording.GaugeVecValue("", "", "crdb_connections_per_node", "read", "1"), 1e-9)
	require.Equal(t, uint64(1), recording.HistogramVecCount("", "", "crdb_pruning_duration", "read"))
}

func TestSetMetricsFactoryWithNilAndNoopPool(t *testing.T) {
	SetMetricsFactory(internalmetrics.NoopFactory{})
	require.NotPanics(t, func() {
		resetHistogram.Observe(1)
		healthyCRDBNodeCountGauge.Set(1)
		connectionsPerCRDBNodeCountGauge.WithLabelValues("read", "1").Set(1)
		pruningTimeHistogram.WithLabelValues("read").Observe(1)
	})

	SetMetricsFactory(nil)
	require.NotPanics(t, func() {
		resetHistogram.Observe(1)
		healthyCRDBNodeCountGauge.Set(1)
		connectionsPerCRDBNodeCountGauge.WithLabelValues("read", "1").Set(1)
		pruningTimeHistogram.WithLabelValues("read").Observe(1)
	})

	SetMetricsFactory(internalmetrics.NewPrometheusFactory(nil))
}
