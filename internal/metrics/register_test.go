package metrics

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterOrReuseReturnsExistingCollectorOnDuplicate(t *testing.T) {
	registry := prometheus.NewRegistry()

	first := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "metrics",
		Name:      "register_or_reuse_total",
		Help:      "test counter",
	})

	registeredFirst, err := RegisterOrReuse(registry, first)
	require.NoError(t, err)
	require.Same(t, first, registeredFirst)

	duplicate := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "metrics",
		Name:      "register_or_reuse_total",
		Help:      "test counter",
	})

	registeredDuplicate, err := RegisterOrReuse(registry, duplicate)
	require.NoError(t, err)
	require.Same(t, registeredFirst, registeredDuplicate)
}

func TestMustRegisterOrReuse(t *testing.T) {
	t.Run("does not panic on duplicate registration", func(t *testing.T) {
		registry := prometheus.NewRegistry()

		first := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "spicedb",
			Subsystem: "metrics",
			Name:      "must_register_or_reuse_total",
			Help:      "test counter",
		})
		MustRegisterOrReuse(registry, first)

		duplicate := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "spicedb",
			Subsystem: "metrics",
			Name:      "must_register_or_reuse_total",
			Help:      "test counter",
		})

		assert.NotPanics(t, func() {
			MustRegisterOrReuse(registry, duplicate)
		})
	})

	t.Run("panics on non-duplicate registration error", func(t *testing.T) {
		assert.Panics(t, func() {
			MustRegisterOrReuse(failingRegisterer{}, prometheus.NewCounter(prometheus.CounterOpts{
				Name: "failing_registerer_total",
				Help: "test counter",
			}))
		})
	})
}

type failingRegisterer struct{}

func (failingRegisterer) Register(prometheus.Collector) error  { return errors.New("register failed") }
func (failingRegisterer) MustRegister(...prometheus.Collector) {}
func (failingRegisterer) Unregister(prometheus.Collector) bool { return false }
