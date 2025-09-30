package promutil

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMustCounterValue(t *testing.T) {
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_total",
		Help: "A test counter for unit testing",
	})

	result := MustCounterValue(counter)
	if result != 0.0 {
		t.Errorf("Initial counter value should be 0.0, got %v", result)
	}

	counter.Add(15)
	result = MustCounterValue(counter)
	if result != 15 {
		t.Errorf("Counter value should be 15.5 after Add(15.5), got %v", result)
	}

	counter.Inc()
	result = MustCounterValue(counter)
	if result != 16 {
		t.Errorf("Counter value should be 16.5 after Inc(), got %v", result)
	}
}

func TestMustCounterValue_Panics(t *testing.T) {
	defer func() { _ = recover() }()
	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "test",
		Help: "A test histogram for unit testing",
	})
	MustCounterValue(histogram)
	t.Fatal("did not panic when provided a non-counter")
}
