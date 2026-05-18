package metrics

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// RegisterOrReuse registers c with r. If c has already been registered,
// the existing collector is returned.
func RegisterOrReuse(r prometheus.Registerer, c prometheus.Collector) (prometheus.Collector, error) {
	if err := r.Register(c); err != nil {
		if are, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); ok {
			return are.ExistingCollector, nil
		}
		return nil, err
	}
	return c, nil
}

// MustRegisterOrReuse registers c with r, panicking only on non-duplicate
// registration errors.
func MustRegisterOrReuse(r prometheus.Registerer, c prometheus.Collector) {
	if _, err := RegisterOrReuse(r, c); err != nil {
		panic(err)
	}
}
