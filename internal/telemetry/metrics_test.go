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
