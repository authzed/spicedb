package promutil

import "github.com/prometheus/client_golang/prometheus"

var _ prometheus.Collector = (CollectorFunc)(nil)

// CollectorFunc is a convenient way to implement a Prometheus Collector
// without interface boilerplate.
//
// This implementation relies on prometheus.DescribeByCollect; familiarize
// yourself with that documentation before using.
type CollectorFunc func(ch chan<- prometheus.Metric)

func (c CollectorFunc) Collect(ch chan<- prometheus.Metric) { c(ch) }
func (c CollectorFunc) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(c, ch) }
