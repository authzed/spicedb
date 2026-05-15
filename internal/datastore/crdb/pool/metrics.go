package pool

import internalmetrics "github.com/authzed/spicedb/internal/metrics"

// SetMetricsFactory configures which metrics factory is used by this package.
func SetMetricsFactory(factory internalmetrics.Factory) {
	setBalancerMetricsFactory(factory)
	setHealthMetricsFactory(factory)
	setPoolMetricsFactory(factory)
}
