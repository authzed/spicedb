package promutil

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestMustCounterValue(t *testing.T) {
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_total",
		Help: "A test counter for unit testing",
	})

	result := MustCounterValue(counter)
	require.Equal(t, 0.0, result, "Initial counter value should be 0.0")

	counter.Add(15)
	result = MustCounterValue(counter)
	require.Equal(t, 15.0, result, "Counter value should be 15 after Add(15)")

	counter.Inc()
	result = MustCounterValue(counter)
	require.Equal(t, 16.0, result, "Counter value should be 16 after Inc()")
}

func TestMustCounterValue_Panics(t *testing.T) {
	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "test",
		Help: "A test histogram for unit testing",
	})
	require.Panics(t, func() {
		MustCounterValue(histogram)
	})
}
