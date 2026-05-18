package telemetry

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	proxymock "github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	internalmetrics "github.com/authzed/spicedb/internal/metrics"
	"github.com/authzed/spicedb/pkg/datastore"
)

func TestCollect(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 100, 10*time.Hour, 10*time.Hour)
	require.NoError(t, err)

	_, c, err := registerTelemetryCollector("memdb", ds)
	require.NoError(t, err)

	ch := make(chan prometheus.Metric, 100)
	c.Collect(ch)

	// Check that the metrics have been collected.
	metricCount := 0
outer:
	for {
		select {
		case metric := <-ch:
			require.NotNil(t, metric)
			metricCount++

		default:
			break outer
		}
	}

	require.GreaterOrEqual(t, metricCount, 3, "Expected at least three metrics to be collected")
}

func TestSpiceDBClusterInfoCollector(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 100, 10*time.Hour, 10*time.Hour)
	require.NoError(t, err)

	ctx := t.Context()
	collector, err := SpiceDBClusterInfoCollector(ctx, "test", "memdb", ds)
	require.NoError(t, err)

	ch := make(chan prometheus.Metric, 10)
	collector(ch)

	// Should have exactly one info metric
	metricCount := 0
outer:
	for {
		select {
		case metric := <-ch:
			require.NotNil(t, metric)
			metricCount++
		default:
			break outer
		}
	}

	require.Equal(t, 1, metricCount, "Expected exactly one info metric")
}

func TestRegisterTelemetryCollector(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 100, 10*time.Hour, 10*time.Hour)
	require.NoError(t, err)

	registry, err := RegisterTelemetryCollector("memdb", ds)
	require.NoError(t, err)
	require.NotNil(t, registry)

	// Verify we can gather metrics from the registry
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	require.NotEmpty(t, metricFamilies)
}

func TestCollectorDescribe(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 100, 10*time.Hour, 10*time.Hour)
	require.NoError(t, err)

	_, collector, err := registerTelemetryCollector("memdb", ds)
	require.NoError(t, err)

	ch := make(chan *prometheus.Desc, 10)
	collector.Describe(ch)

	// Should describe all four metrics
	descCount := 0
outer:
	for {
		select {
		case desc := <-ch:
			require.NotNil(t, desc)
			descCount++
		default:
			break outer
		}
	}

	require.Equal(t, 4, descCount, "Expected four metric descriptions")
}

func TestRegisterTelemetryCollectorRepeatedOnSameRegistry(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 100, 10*time.Hour, 10*time.Hour)
	require.NoError(t, err)

	registry := prometheus.NewRegistry()

	firstCollector, err := registerTelemetryCollectorWithRegistry(registry, "memdb", ds)
	require.NoError(t, err)
	require.NotNil(t, firstCollector)

	secondCollector, err := registerTelemetryCollectorWithRegistry(registry, "memdb", ds)
	require.NoError(t, err)
	require.NotNil(t, secondCollector)

	// Re-registration should reuse the existing collector instance.
	require.Same(t, firstCollector, secondCollector)

	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	require.NotEmpty(t, metricFamilies)
}

func TestCollectWithRecordingFactory(t *testing.T) {
	SetMetricsFactory(internalmetrics.NewRecordingFactory())
	t.Cleanup(func() {
		SetMetricsFactory(internalmetrics.NewPrometheusFactory(nil))
	})

	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 100, 10*time.Hour, 10*time.Hour)
	require.NoError(t, err)

	_, c, err := registerTelemetryCollector("memdb", ds)
	require.NoError(t, err)

	ch := make(chan prometheus.Metric, 100)
	require.NotPanics(t, func() {
		c.Collect(ch)
	})
}

func TestSetMetricsFactoryWithNoopFactory(t *testing.T) {
	original := internalmetrics.NewPrometheusFactory(nil)
	t.Cleanup(func() {
		SetMetricsFactory(original)
	})

	SetMetricsFactory(internalmetrics.NoopFactory{})

	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 100, 10*time.Hour, 10*time.Hour)
	require.NoError(t, err)

	_, c, err := registerTelemetryCollector("memdb", ds)
	require.NoError(t, err)

	ch := make(chan prometheus.Metric, 100)
	require.NotPanics(t, func() {
		c.Collect(ch)
	})
}

func TestSetMetricsFactoryWithNil(t *testing.T) {
	original := internalmetrics.NewPrometheusFactory(nil)
	t.Cleanup(func() {
		SetMetricsFactory(original)
	})

	// Passing nil should default to NoopFactory
	SetMetricsFactory(nil)

	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 100, 10*time.Hour, 10*time.Hour)
	require.NoError(t, err)

	_, c, err := registerTelemetryCollector("memdb", ds)
	require.NoError(t, err)

	ch := make(chan prometheus.Metric, 100)
	require.NotPanics(t, func() {
		c.Collect(ch)
	})
}

func TestRegisterTelemetryCollectorWithRegistryStatisticsError(t *testing.T) {
	ds := &proxymock.MockDatastore{}
	ds.On("Statistics").Return(datastore.Stats{}, errors.New("stats unavailable")).Once()

	registry := prometheus.NewRegistry()
	collector, err := registerTelemetryCollectorWithRegistry(registry, "mock", ds)
	require.Error(t, err)
	require.Nil(t, collector)
	require.Contains(t, err.Error(), "unable create info collector")

	ds.AssertExpectations(t)
}

func TestCollectWithDatastoreStatisticsErrorUsesZeroStats(t *testing.T) {
	ds := &proxymock.MockDatastore{}
	ds.On("Statistics").Return(datastore.Stats{UniqueID: "cluster-1"}, nil).Twice()

	registry := prometheus.NewRegistry()
	collector, err := registerTelemetryCollectorWithRegistry(registry, "mock", ds)
	require.NoError(t, err)
	require.NotNil(t, collector)

	collector.ds = ds
	ds.On("Statistics").Return(datastore.Stats{}, errors.New("stats unavailable during collect")).Once()

	ch := make(chan prometheus.Metric, 100)
	require.NotPanics(t, func() {
		collector.Collect(ch)
	})

	// Collect should still emit metrics even if datastore statistics fail.
	count := 0
	for {
		select {
		case metric := <-ch:
			require.NotNil(t, metric)
			count++
		default:
			require.GreaterOrEqual(t, count, 3)
			ds.AssertExpectations(t)
			return
		}
	}
}

func TestCollectUsesNilDatastoreWithoutPanicking(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 100, 10*time.Hour, 10*time.Hour)
	require.NoError(t, err)

	_, collector, err := registerTelemetryCollector("memdb", ds)
	require.NoError(t, err)

	collector.ds = nil

	ch := make(chan prometheus.Metric, 100)
	require.NotPanics(t, func() {
		collector.Collect(ch)
	})

	// Ensure something was emitted on the nil-datastore path.
	select {
	case metric := <-ch:
		require.NotNil(t, metric)
	default:
		t.Fatal("expected at least one collected metric")
	}
}
