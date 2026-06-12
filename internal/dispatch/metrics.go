package dispatch

import "github.com/prometheus/client_golang/prometheus"

// MetricsOptions configures how a dispatcher reports Prometheus metrics.
//
// Metrics are only emitted when both fields are set.
// The zero value disables metrics entirely.
type MetricsOptions struct {
	// PrometheusSubsystem is the subsystem name prepended to every metric
	// emitted by the dispatcher (e.g. "dispatch" or "dispatch_client"). It
	// namespaces the dispatcher's metrics so they don't collide with those of
	// other components registered to the same registry.
	PrometheusSubsystem string

	// PrometheusRegistry is the registry the dispatcher's metrics are registered with.
	PrometheusRegistry prometheus.Registerer
}

// Enabled reports whether metrics should be emitted.
func (m *MetricsOptions) Enabled() bool {
	return m.PrometheusSubsystem != "" && m.PrometheusRegistry != nil
}
