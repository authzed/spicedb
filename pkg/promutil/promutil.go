package promutil

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

var _ prometheus.Collector = (CollectorFunc)(nil)

// CollectorFunc is a convenient way to implement a Prometheus Collector
// without interface boilerplate.
//
// This implementation relies on prometheus.DescribeByCollect; familiarize
// yourself with that documentation before using.
type CollectorFunc func(ch chan<- prometheus.Metric)

func (c CollectorFunc) Collect(ch chan<- prometheus.Metric) { c(ch) }
func (c CollectorFunc) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(c, ch) }

// MustCounterValue returns the current value of a counter metric.
// If any error occurs, this function panics.
func MustCounterValue(m prometheus.Metric) float64 {
	var written dto.Metric
	if err := m.Write(&written); err != nil {
		panic("failed to read Prometheus counter: " + err.Error())
	}
	return *written.Counter.Value
}
