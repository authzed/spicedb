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

// Gauge creates a Prometheus gauge and registers it. Returns the
// existing gauge if this metric was already registered.
func (f *PrometheusFactory) Gauge(opts Opts) Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: opts.Namespace,
		Subsystem: opts.Subsystem,
		Name:      opts.Name,
		Help:      opts.Help,
	})
	if existing, ok := f.register(g); ok {
		if eg, ok2 := existing.(prometheus.Gauge); ok2 {
			return eg
		}
	}
	return g
}

// Histogram creates a Prometheus histogram and registers it. Returns the
// existing histogram if this metric was already registered.
func (f *PrometheusFactory) Histogram(opts Opts) Histogram {
	h := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace:                   opts.Namespace,
		Subsystem:                   opts.Subsystem,
		Name:                        opts.Name,
		Help:                        opts.Help,
		Buckets:                     opts.Buckets,
		NativeHistogramBucketFactor: opts.NativeHistogramBucketFactor,
	})
	if existing, ok := f.register(h); ok {
		if eh, ok2 := existing.(prometheus.Histogram); ok2 {
			return eh
		}
	}
	return h
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

// GaugeVec creates a Prometheus GaugeVec and registers it. Returns the
// existing vec if already registered.
func (f *PrometheusFactory) GaugeVec(opts Opts, labelNames []string) GaugeVec {
	gv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: opts.Namespace,
		Subsystem: opts.Subsystem,
		Name:      opts.Name,
		Help:      opts.Help,
	}, labelNames)
	if existing, ok := f.register(gv); ok {
		if egv, ok2 := existing.(*prometheus.GaugeVec); ok2 {
			return &prometheusGaugeVec{egv}
		}
	}
	return &prometheusGaugeVec{gv}
}

// HistogramVec creates a Prometheus HistogramVec and registers it. Returns the
// existing vec if already registered.
func (f *PrometheusFactory) HistogramVec(opts Opts, labelNames []string) HistogramVec {
	hv := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:                   opts.Namespace,
		Subsystem:                   opts.Subsystem,
		Name:                        opts.Name,
		Help:                        opts.Help,
		Buckets:                     opts.Buckets,
		NativeHistogramBucketFactor: opts.NativeHistogramBucketFactor,
	}, labelNames)
	if existing, ok := f.register(hv); ok {
		if ehv, ok2 := existing.(*prometheus.HistogramVec); ok2 {
			return &prometheusHistogramVec{ehv}
		}
	}
	return &prometheusHistogramVec{hv}
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

type prometheusGaugeVec struct {
	gv *prometheus.GaugeVec
}

func (p *prometheusGaugeVec) WithLabelValues(lvs ...string) Gauge {
	return p.gv.WithLabelValues(lvs...)
}

type prometheusHistogramVec struct {
	hv *prometheus.HistogramVec
}

func (p *prometheusHistogramVec) WithLabelValues(lvs ...string) Histogram {
	return p.hv.WithLabelValues(lvs...)
}

// AsPrometheusHistogramVec returns the underlying Prometheus histogram vec when
// hv was created by PrometheusFactory.
func AsPrometheusHistogramVec(hv HistogramVec) (*prometheus.HistogramVec, bool) {
	promHV, ok := hv.(*prometheusHistogramVec)
	if !ok {
		return nil, false
	}
	return promHV.hv, true
}

// AsPrometheusGaugeVec returns the underlying Prometheus gauge vec when
// gv was created by PrometheusFactory. Useful for callers that need
// Prometheus-specific operations (e.g. DeletePartialMatch) beyond the
// GaugeVec interface.
func AsPrometheusGaugeVec(gv GaugeVec) (*prometheus.GaugeVec, bool) {
	promGV, ok := gv.(*prometheusGaugeVec)
	if !ok {
		return nil, false
	}
	return promGV.gv, true
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
