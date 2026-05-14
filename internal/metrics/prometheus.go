package metrics

import (
	"errors"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	log "github.com/authzed/spicedb/internal/logging"
)

// PrometheusFactory creates Prometheus metrics and registers them with the
// supplied prometheus.Registerer.  All metrics it creates are unregistered
// when Close is called.
//
// If a duplicate-registration error occurs (AlreadyRegisteredError) the
// existing collector is reused silently – this matches the behaviour of
// the previous RegisterPrometheusCollectors helper.
type PrometheusFactory struct {
	registerer prometheus.Registerer
	mu         sync.Mutex
	collectors []prometheus.Collector // GUARDED_BY(mu)
}

// NewPrometheusFactory returns a factory that registers metrics with r.
// If r is nil, prometheus.DefaultRegisterer is used.
func NewPrometheusFactory(r prometheus.Registerer) *PrometheusFactory {
	if r == nil {
		r = prometheus.DefaultRegisterer
	}
	return &PrometheusFactory{registerer: r}
}

// Counter creates a Prometheus counter and registers it.  Returns the
// existing counter if this metric was already registered.
func (f *PrometheusFactory) Counter(opts Opts) Counter {
	c := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: opts.Namespace,
		Subsystem: opts.Subsystem,
		Name:      opts.Name,
		Help:      opts.Help,
	})
	if existing, ok := f.register(c); ok {
		if ec, ok2 := existing.(prometheus.Counter); ok2 {
			return ec
		}
	}
	return c
}

// CounterVec creates a Prometheus CounterVec and registers it.  Returns the
// existing vec if already registered.
func (f *PrometheusFactory) CounterVec(opts Opts, labelNames []string) CounterVec {
	cv := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: opts.Namespace,
		Subsystem: opts.Subsystem,
		Name:      opts.Name,
		Help:      opts.Help,
	}, labelNames)
	if existing, ok := f.register(cv); ok {
		if ecv, ok2 := existing.(*prometheus.CounterVec); ok2 {
			return &prometheusCounterVec{ecv}
		}
	}
	return &prometheusCounterVec{cv}
}

// prometheusCounterVec adapts *prometheus.CounterVec to the CounterVec interface.
// The adaptation is needed because *prometheus.CounterVec.WithLabelValues returns
// prometheus.Counter, not metrics.Counter (different named types despite same shape).
type prometheusCounterVec struct {
	cv *prometheus.CounterVec
}

func (p *prometheusCounterVec) WithLabelValues(lvs ...string) Counter {
	return p.cv.WithLabelValues(lvs...)
}

// Close unregisters every metric this factory created.
func (f *PrometheusFactory) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.collectors {
		f.registerer.Unregister(c)
	}
	f.collectors = nil
	return nil
}

// register attempts to register c with the Prometheus registerer.  On
// AlreadyRegisteredError it returns (existingCollector, true).  On any other
// error it logs a warning and returns (nil, false) – the original c should be
// used as a no-op.
func (f *PrometheusFactory) register(c prometheus.Collector) (prometheus.Collector, bool) {
	if err := f.registerer.Register(c); err != nil {
		if are, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); ok {
			return are.ExistingCollector, true
		}
		log.Warn().Err(err).Msg("failed to register prometheus metric")
		return nil, false
	}
	f.mu.Lock()
	f.collectors = append(f.collectors, c)
	f.mu.Unlock()
	return nil, false
}
