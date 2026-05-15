package proxy

import "github.com/authzed/spicedb/internal/metrics"

// SetMetricsFactory configures which metrics factory is used by this package.
func SetMetricsFactory(factory metrics.Factory) {
	setObservableMetricsFactory(factory)
	setCheckingReplicatedMetricsFactory(factory)
	setStrictReplicatedMetricsFactory(factory)
}
