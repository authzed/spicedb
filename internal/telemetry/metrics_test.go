package telemetry

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
)

func TestCollect(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(100, 10*time.Hour, 10*time.Hour)
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
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(100, 10*time.Hour, 10*time.Hour)
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
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(100, 10*time.Hour, 10*time.Hour)
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
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(100, 10*time.Hour, 10*time.Hour)
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
