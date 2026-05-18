package runtime

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	internalmetrics "github.com/authzed/spicedb/internal/metrics"
)

// prometheus client_golang by default registers a collector that collects all metrics, except scheduler metrics
// this package unregisters the default collector and adds one that includes scheduler metrics
//
// in order to register this, the package must be imported anonymously
func init() {
	prometheus.DefaultRegisterer.Unregister(collectors.NewGoCollector())
	internalmetrics.MustRegisterOrReuse(prometheus.DefaultRegisterer, collectors.NewGoCollector(
		collectors.WithGoCollectorRuntimeMetrics(collectors.MetricsAll),
	))
}
