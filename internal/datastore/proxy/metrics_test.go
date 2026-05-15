package proxy

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/metrics"
)

func TestSetMetricsFactoryAppliesToAllProxyMetrics(t *testing.T) {
	recording := metrics.NewRecordingFactory()
	SetMetricsFactory(recording)
	t.Cleanup(func() {
		SetMetricsFactory(metrics.NewPrometheusFactory(nil))
	})

	loadedRelationshipCount.Observe(3)
	queryLatency.WithLabelValues("op", "shape").Observe(0.1)

	checkingReplicatedTotalReaderCount.Add(2)
	checkingReplicatedReplicaReaderCount.WithLabelValues("replica-a").Inc()
	readReplicatedSelectedReplicaCount.WithLabelValues("replica-a").Add(3)

	strictReadReplicatedTotalQueryCount.Add(4)
	strictReadReplicatedFallbackQueryCount.WithLabelValues("replica-a").Add(5)

	require.Equal(t, uint64(1), recording.HistogramCount("spicedb", "datastore", "loaded_relationships_count"))
	require.Equal(t, uint64(1), recording.HistogramVecCount("spicedb", "datastore", "query_latency", "op", "shape"))
	require.InEpsilon(t, 2.0, recording.CounterValue("spicedb", "datastore_replica", "checking_replicated_reader_total"), 1e-9)
	require.InEpsilon(t, 1.0, recording.CounterVecValue("spicedb", "datastore_replica", "checking_replicated_replica_reader_total", "replica-a"), 1e-9)
	require.InEpsilon(t, 3.0, recording.CounterVecValue("spicedb", "datastore_replica", "selected_replica_total", "replica-a"), 1e-9)
	require.InEpsilon(t, 4.0, recording.CounterValue("spicedb", "datastore_replica", "strict_replicated_query_total"), 1e-9)
	require.InEpsilon(t, 5.0, recording.CounterVecValue("spicedb", "datastore_replica", "strict_replicated_fallback_query_total", "replica-a"), 1e-9)
}

func TestSetMetricsFactoryWithNilAndNoopProxy(t *testing.T) {
	SetMetricsFactory(metrics.NoopFactory{})
	require.NotPanics(t, func() {
		loadedRelationshipCount.Observe(1)
		queryLatency.WithLabelValues("op", "shape").Observe(0.1)
		checkingReplicatedTotalReaderCount.Inc()
		checkingReplicatedReplicaReaderCount.WithLabelValues("replica-a").Inc()
		readReplicatedSelectedReplicaCount.WithLabelValues("replica-a").Inc()
		strictReadReplicatedTotalQueryCount.Inc()
		strictReadReplicatedFallbackQueryCount.WithLabelValues("replica-a").Inc()
	})

	SetMetricsFactory(nil)
	require.NotPanics(t, func() {
		loadedRelationshipCount.Observe(1)
		queryLatency.WithLabelValues("op", "shape").Observe(0.1)
		checkingReplicatedTotalReaderCount.Inc()
		checkingReplicatedReplicaReaderCount.WithLabelValues("replica-a").Inc()
		readReplicatedSelectedReplicaCount.WithLabelValues("replica-a").Inc()
		strictReadReplicatedTotalQueryCount.Inc()
		strictReadReplicatedFallbackQueryCount.WithLabelValues("replica-a").Inc()
	})

	SetMetricsFactory(metrics.NewPrometheusFactory(nil))
}
